import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ChangeEvent, MouseEvent, ReactElement } from "react";
import { useSearchParams } from "react-router-dom";
import "./productConfigCompare.css";
import { LoadingSurface } from "../components/LoadingSurface";
import type { BusinessSummaryMode } from "../components/BusinessSummaryPanel";
import type {
  ConfigComparisonCellSavePayload,
  ConfigComparisonCellSaveResult,
  ConfigComparisonTableExportActions,
  ConfigComparisonTableExportStatus,
} from "../components/ConfigComparisonTable";
import { SearchDropdownFilter, type SearchDropdownOption } from "../components/SearchDropdownFilter";
import type { SourceEvidenceSelection } from "../components/SourceEvidenceDrawer";
import {
  DeckControlTabs,
  DeckFloatingDrawer,
  type DeckControlTabItem,
} from "../components/deckControls";
import { api } from "../api/client";
import { useOptionalAuth } from "../contexts/AuthContext";
import {
  rowMatchesConfigScope,
  rowMatchesConfigSearch,
  type ConfigComparisonDeltaFilter,
} from "../utils/configComparisonFilters";
import { rowDeltasForBase } from "../utils/configDelta";
import { formatEngineeringConfigDigestDraftFeedback } from "../utils/engineeringConfigDigestDraft";
import {
  engineeringConfigOcrComparisonText,
  isOcrSemanticStrategy,
} from "../utils/engineeringConfigOcr";
import type {
  CompareGroup,
  CompareResponse,
  CompareRow,
  CompareSummary,
  CompareTrimItem,
  ConfigValueState,
  AvailabilityState,
  EngineeringConfigBusinessSummaryItem,
  EngineeringConfigBusinessSummaryReadiness,
  EngineeringConfigBusinessSummaryUsage,
  EngineeringConfigCompareFactRequest,
  EngineeringConfigSourceContext,
  EngineeringConfigCompetitorRecommendation,
  EngineeringConfigCompetitorRecommendationResponse,
  EngineeringConfigDigestTrimIdentityOverride,
  EngineeringConfigDigestDraftResult,
  EngineeringConfigVersionScope,
  EngineeringConfigSourceDigest,
  EngineeringConfigSourceDigestGroup,
  EngineeringConfigSourceSnapshot,
  VehicleTrimItem,
} from "../types/engineeringConfig";

const LazyBusinessSummaryPanel = lazy(() => import("../components/BusinessSummaryPanel").then((module) => ({
  default: module.BusinessSummaryPanel,
})));
const LazyConfigComparisonTable = lazy(() => import("../components/ConfigComparisonTable").then((module) => ({
  default: module.ConfigComparisonTable,
})));
const LazyEngineeringConfigSourceUploadPanel = lazy(() => import("../components/EngineeringConfigSourceUploadPanel").then((module) => ({
  default: module.EngineeringConfigSourceUploadPanel,
})));
const LazyEngineeringConfigAiSummaryReadinessCard = lazy(() => import("../components/EngineeringConfigAiSummaryReadinessCard").then((module) => ({
  default: module.EngineeringConfigAiSummaryReadinessCard,
})));
const LazySourceEvidenceDrawer = lazy(() => import("../components/SourceEvidenceDrawer").then((module) => ({
  default: module.SourceEvidenceDrawer,
})));

const SOURCE_DIGEST_BROWSE_PREVIEW_LIMIT = 6;

type ProductConfigPanel = "filters" | "selected" | "source" | "display";
interface FeatureCatalogMappingUploadSummary {
  totalFeatures: number;
  createdFeatureCount: number;
  updatedFeatureCount: number;
  unchangedFeatureCount: number;
  warningCount: number;
  warnings?: string[];
  categories?: string[];
}

interface FeatureCatalogMappingUploadAudit {
  uploadId: string;
  fileName: string;
  status: string;
  importedBy?: string;
  importedRole?: string;
  importedAtUtc?: string;
  artifactRef?: string;
  persistedIn?: string;
  summary?: FeatureCatalogMappingUploadSummary;
}

function featureCatalogMappingAuditText(audit: FeatureCatalogMappingUploadAudit): string {
  const summary = audit.summary;
  const lines = [
    "Feature Catalog Mapping Import Audit",
    `Upload ID: ${audit.uploadId || "unknown"}`,
    `File: ${audit.fileName || "unknown"}`,
    `Status: ${audit.status || "unknown"}`,
    `Imported by: ${audit.importedBy || "unknown"} (${audit.importedRole || "role unknown"})`,
    `Imported at UTC: ${audit.importedAtUtc || "unknown"}`,
    `Audit artifact: ${audit.artifactRef || "upload session meta"}`,
    `Persisted in: ${audit.persistedIn || "upload_session_meta"}`,
  ];
  if (summary) {
    lines.push(
      `Total fields: ${summary.totalFeatures}`,
      `Updated: ${summary.updatedFeatureCount}`,
      `Created: ${summary.createdFeatureCount}`,
      `Unchanged: ${summary.unchangedFeatureCount}`,
      `Warnings: ${summary.warningCount}`,
    );
    if (summary.categories && summary.categories.length > 0) {
      lines.push(`Categories: ${summary.categories.join(" / ")}`);
    }
    if (summary.warnings && summary.warnings.length > 0) {
      lines.push("Warning details:", ...summary.warnings.map((warning) => `- ${warning}`));
    }
  }
  return lines.join("\n");
}
type TrimFilters = {
  brand: string;
  model: string;
  market: string;
  modelYear: string;
  trim: string;
  powertrain: string;
  segment: string;
  source: string;
  keyword: string;
};

function initialTrimFiltersFromSearchParams(searchParams: URLSearchParams): TrimFilters {
  return {
    brand: searchParams.get("brand") ?? "",
    model: searchParams.get("model") ?? searchParams.get("modelName") ?? "",
    market: searchParams.get("market") ?? searchParams.get("country") ?? "",
    modelYear: searchParams.get("modelYear") ?? searchParams.get("year") ?? "",
    trim: searchParams.get("trim") ?? "",
    powertrain: searchParams.get("powertrain") ?? searchParams.get("energyType") ?? "",
    segment: searchParams.get("segment") ?? "",
    source: searchParams.get("source") ?? "",
    keyword: searchParams.get("keyword") ?? searchParams.get("q") ?? "",
  };
}

function hasTrimFilterSearchParams(searchParams: URLSearchParams): boolean {
  const filterKeys = ["brand", "model", "modelName", "market", "country", "modelYear", "year", "trim", "powertrain", "energyType", "segment", "source", "keyword", "q"];
  return filterKeys.some((key) => Boolean(searchParams.get(key)?.trim()));
}

function trimPrefilterLabel(filters: TrimFilters): string | null {
  const labels = [
    filters.model,
    filters.market,
    filters.powertrain,
    filters.segment,
    filters.modelYear,
    filters.trim,
    filters.source,
    filters.keyword,
  ].map((value) => value.trim()).filter(Boolean);
  if (labels.length === 0) return null;
  const visibleLabels = labels.slice(0, 3).join(" · ");
  return labels.length > 3 ? `${visibleLabels} · +${labels.length - 3}` : visibleLabels;
}

function directTrimSearchScopeLabel(filters: TrimFilters): string | null {
  const labels = [
    filters.brand,
    filters.model,
    filters.market,
    filters.modelYear,
    filters.trim,
    filters.powertrain,
    filters.source,
  ].map((value) => value.trim()).filter(Boolean);
  if (labels.length === 0) return null;
  const visibleLabels = labels.slice(0, 3).join(" · ");
  return labels.length > 3 ? `${visibleLabels} · +${labels.length - 3}` : visibleLabels;
}

function sourceDigestLibraryLookupQuery(filters: TrimFilters, explicitQuery: string): string {
  return [
    explicitQuery,
    filters.model,
    filters.source,
    filters.keyword,
    filters.trim,
    filters.modelYear,
  ].map((value) => value.trim()).find(Boolean) ?? "";
}

function sourceDigestLibraryScopeHint(filters: TrimFilters, lookupQuery: string, searchActive: boolean): string {
  const scopeParts = [
    filters.market ? `国家 ${filters.market}` : null,
    filters.powertrain ? `动力 ${filters.powertrain}` : null,
    filters.segment ? `Segment ${filters.segment}` : null,
  ].filter((part): part is string => Boolean(part));
  if (!searchActive) {
    return scopeParts.length > 0
      ? `当前来源库范围：${scopeParts.join(" · ")}；输入车型 / 来源 / 上传人 / 物料号后开始搜索。`
      : "来源库搜索不区分本品 / 竞品；输入车型 / 来源 / 上传人 / 物料号或 sales version 后查询共享来源。";
  }
  const query = lookupQuery.trim();
  const scopeLabel = scopeParts.length > 0 ? `；范围 ${scopeParts.join(" · ")}` : "；未按国家、动力或 Segment 收窄";
  return `当前来源库查询：${query || "未指定关键词"}${scopeLabel}`;
}

function sourceDigestFiltersForContext(filters: TrimFilters, context: EngineeringConfigSourceContext | null): TrimFilters {
  if (!context) return filters;
  return {
    ...filters,
    brand: context.brand?.trim() || filters.brand,
    model: context.model?.trim() || filters.model,
    market: (context.country || context.market)?.trim() || filters.market,
    modelYear: context.modelYear?.trim() || filters.modelYear,
    powertrain: context.powertrain?.trim() || filters.powertrain,
    segment: context.segment?.trim() || filters.segment,
  };
}

type ComparableTrim = VehicleTrimItem | CompareTrimItem;
type TrimIdentityDraft = {
  brand: string;
  modelName: string;
  trimName: string;
  fullTrimName: string;
  market: string;
  modelYear: string;
  energyType: string;
  drivetrain: string;
  engine: string;
  materialNo: string;
  vehicleCode: string;
  identityKey: string;
};
type TrimIdentityFieldKey = keyof TrimIdentityDraft;
type TrimIdentityPatch = Partial<Pick<CompareTrimItem,
  "brand" | "modelName" | "trimName" | "fullTrimName" | "market" | "country" | "modelYear" | "energyType" | "drivetrain" | "engine" | "materialNo" | "vehicleCode" | "identityKey" | "salesVersion" | "hasMaterialNo" | "dataOrigin"
>>;
type SameModelTrimGroup = { key: string; label: string; meta: string; items: VehicleTrimItem[] };
type LibraryBrandTrimGroup = {
  key: string;
  brandLabel: string;
  marketLabel: string;
  modelYearLabel: string;
  modelCount: number;
  trimCount: number;
  sourceCount: number;
  groups: SameModelTrimGroup[];
};
type SourceDigestGroupCandidate = {
  group: EngineeringConfigSourceDigestGroup;
  createdBy?: string | null;
  ocrEngine?: EngineeringConfigSourceDigest["ocrEngine"];
  ocrEngineCandidates?: EngineeringConfigSourceDigest["ocrEngineCandidates"];
  ocrEvaluation?: EngineeringConfigSourceDigest["ocrEvaluation"];
  sourceContext?: EngineeringConfigSourceContext | null;
  sourceDigestType?: EngineeringConfigSourceDigest["digestType"];
  sourceId?: string;
  sourceFileName: string;
  sourceFormat?: EngineeringConfigSourceDigest["sourceFormat"];
  sourceGroupIndex?: number;
  sourceGroupCount?: number;
  sourceKind: "local" | "library";
  sourceSearchMatches?: string[];
};
type LocalDigestGroupEntry = {
  group: EngineeringConfigSourceDigestGroup;
  index: number;
  key: string;
};
type SourceDigestTrimSelectionMap = Record<string, string[]>;
type SourceDigestPendingCandidateMap = Record<string, SourceDigestGroupCandidate>;
type SourceDigestTrimIdentityDraftMap = Record<string, Record<string, EngineeringConfigDigestTrimIdentityOverride>>;
type SourceDigestTrimIdentityFieldKey = Exclude<keyof EngineeringConfigDigestTrimIdentityOverride, "trimId">;
type SourceDigestReviewFocusMap = Record<string, string>;
type SourceDigestDraftReviewFocus = {
  category: string;
  featureCode: string;
  featureName: string;
};
type DirectSourceDigestPendingItem = {
  candidate: SourceDigestGroupCandidate;
  key: string;
  selectedTrimIds: string[];
  selectedTrims: EngineeringConfigSourceDigestGroup["trims"];
};
type CategoryNavItem = { category: string; count: number };
type ComparisonIdentityNote = { key: string; label: string; detail: string };
type TargetAnchorTone = "neutral" | "ready" | "warning";
type TargetAnchorItem = { key: string; label: string; value: string; tone: TargetAnchorTone };
type SourceDigestCandidateCoverage = {
  differenceCount: number;
  modelCount: number;
  rowCount: number;
  sourceCount: number;
  trimCount: number;
};
type SourceDigestQualityFilterKey = "all" | "library" | "local" | "excel" | "tabular" | "pdf" | "ocr" | "ocr_temporary" | "review" | "price_list";
type SourceDigestQualityFilterItem = {
  count: number;
  description: string;
  key: SourceDigestQualityFilterKey;
  label: string;
};
type SourceDigestDirectOptionCoverage = {
  libraryGroupCount: number;
  libraryOptionCount: number;
  libraryModelCount: number;
  librarySourceCount: number;
  localGroupCount: number;
  localModelCount: number;
  localOptionCount: number;
  localSourceCount: number;
};

function hasSourceDigestTrimSelections(selectionMap: SourceDigestTrimSelectionMap): boolean {
  return Object.values(selectionMap).some((selectedTrimIds) => selectedTrimIds.length > 0);
}

type SourceDigestDirectCoverageItem = {
  key: string;
  label: string;
  groupCount: number;
  modelCount: number;
  optionCount: number;
  sourceCount: number;
  status: string;
};
type DirectConfigSearchSummaryItem = {
  key: string;
  label: string;
  value: string;
  description: string;
  tone: "ready" | "pending" | "muted";
};
type SelectedConfigPathGroup = {
  key: string;
  sourceLabel: string;
  brandLabel: string;
  marketLabel: string;
  modelYearLabel: string;
  modelLabel: string;
  originLabel: string;
  anchorLabel: string;
  ownerLabel: string | null;
  sourceCreatedAtLabel: string | null;
  trimLabels: string[];
  trimCount: number;
};
type SourceDigestDirectAmbiguityItem = {
  key: string;
  label: string;
  candidateCount: number;
  sourceCount: number;
  sheetCount: number;
  ownerCount: number;
  searchQuery: string;
};
type DirectModelAmbiguityItem = SourceDigestDirectAmbiguityItem & {
  origin: "formal-library" | "source-digest";
  itemUnitLabel: string;
};
type SourceDigestSearchAnchor = {
  key: string;
  label: string;
  value: string;
  query: string;
  sourceId?: string;
};
type SourceDigestLibraryPathItem = SourceDigestSearchAnchor;
type SourceDigestPathStageItem = {
  key: "source" | "model" | "config-columns";
  label: string;
  value: string;
  meta: string;
};
type SourceDigestDraftComparePlacement = {
  ids: string[];
  appendedToCurrentCompare: boolean;
  omittedCreatedTrimIds: string[];
  createdCompareTrimCount: number;
  visibleCreatedCompareTrimCount: number;
  addedToCurrentCompareCount: number;
  omittedFromCurrentCompareCount: number;
  currentCompareTrimCount: number;
};
type SourceDigestDraftCompareTrim = {
  trimId: string;
  label: string;
};
type SourceDigestDraftSuccessSummary = {
  feedback: string;
  currentCompare: { headline: string; label: string; meta: string };
  createdCompareTrimCount: number;
  omittedCompareTrims: SourceDigestDraftCompareTrim[];
  featureCatalogMatch?: {
    meta: string;
    samples: string[];
  } | null;
  ocrTransparency?: {
    meta: string;
    comparison: string | null;
    reviewNote: string;
  } | null;
  pathStages: SourceDigestPathStageItem[];
  metrics: Array<{ key: string; label: string; value: string }>;
};
type SourceDigestActiveScopeItem = {
  key: string;
  label: string;
  value: string;
  tone: "search" | "scope" | "focus" | "quality";
  clearLabel?: string;
  onClear?: () => void;
};
type SourceDigestBrowseGroup = {
  brandLabel: string;
  candidates: SourceDigestGroupCandidate[];
  coverage: SourceDigestCandidateCoverage;
  key: string;
  marketLabel: string;
  modelLabel: string;
  ownerLabel: string | null;
  pathAnchors: SourceDigestSearchAnchor[];
  sourceFileName: string;
  sourceId?: string;
  sourceScopeLabel: string;
};
type ScopedConfigSummaryMetrics = {
  confirmedDifferenceCount: number;
  inferredDifferenceCount: number;
  availabilityDifferenceCount: number;
  commonSameCount: number;
  valueChangedCount: number;
  missingUnknownCount: number;
};
type SourceEvidenceSummaryMetrics = {
  inferredCellCount: number;
  mergedCellCount: number;
  missingSourceValueCount: number;
  missingValueCellCount: number;
  sourceIssueCellCount: number;
  totalCellCount: number;
};
type PrimaryScopeMetric = {
  label: string;
  value: number;
  hint: string;
};
type DisplayScopeOption = {
  key: ConfigComparisonDeltaFilter;
  label: string;
  description: string;
};
type SummaryModeOption = {
  key: BusinessSummaryMode;
  label: string;
  description: string;
};
type AnalysisScopeItem = {
  key: string;
  label: string;
  value: string;
};
type CompetitorRecommendationSource = NonNullable<EngineeringConfigCompetitorRecommendationResponse["source"]>;
type CompetitorSourceDigestMatch = NonNullable<EngineeringConfigCompetitorRecommendation["sourceDigestMatches"]>[number];

function uniqueComparableTrims(trims: Array<ComparableTrim | null | undefined>): ComparableTrim[] {
  const byId = new Map<string, ComparableTrim>();
  trims.forEach((trim) => {
    if (trim) byId.set(trim.trimId, trim);
  });
  return Array.from(byId.values());
}

function uniqueVehicleTrims(trims: Array<VehicleTrimItem | null | undefined>): VehicleTrimItem[] {
  const byId = new Map<string, VehicleTrimItem>();
  trims.forEach((trim) => {
    if (trim) byId.set(trim.trimId, trim);
  });
  return Array.from(byId.values());
}

const LOCAL_CONFIG_WORKBOOK_FILE = "欧盟在售车型可控资源表20260226.xlsx";
const DIRECT_CONFIG_COLUMN_PICKER_LABEL = "搜索并添加配置列";
const SOURCE_UPLOAD_CTA_LABEL = "上传配置表 / 价格单";
const ALL_TARGET_TRIMS_VALUE = "";
const CONFIG_LIBRARY_DIRECT_SOURCE_OPTION_PREFIX = "config-library-source:";
const CONFIG_LIBRARY_DIRECT_BRAND_OPTION_PREFIX = "config-library-brand:";
const CONFIG_LIBRARY_DIRECT_MODEL_OPTION_PREFIX = "config-library-model:";
const CONFIG_LIBRARY_DIRECT_MODEL_ADD_OPTION_PREFIX = "config-library-add-model:";
const SOURCE_DIGEST_DIRECT_OPTION_PREFIX = "source-digest:";
const SOURCE_DIGEST_DIRECT_TRIM_OPTION_PREFIX = "source-digest-trim:";
const SOURCE_DIGEST_DIRECT_SOURCE_OPTION_PREFIX = "source-digest-source:";
const SOURCE_DIGEST_DIRECT_MODEL_OPTION_PREFIX = "source-digest-model:";
const SOURCE_DIGEST_DIRECT_CROSS_MODEL_OPTION_PREFIX = "source-digest-cross-model:";
const LOCAL_DIGEST_SAMPLE_DEFER_MS = 650;
const LOCAL_DIGEST_SAMPLE_IDLE_TIMEOUT_MS = 2200;
const BUSINESS_SUMMARY_PANEL_DEFER_MS = 240;
const BUSINESS_SUMMARY_PANEL_IDLE_TIMEOUT_MS = 1600;
const COMPETITOR_RECOMMENDATION_LIMIT = 10;
const FEATURE_CATALOG_MAPPING_ACCEPT = ".xlsx,.xlsm,.xls";
const FEATURE_CATALOG_MAPPING_CHUNK_SIZE = 5 * 1024 * 1024;
const SOURCE_DIGEST_SNAPSHOT_MATCH_LIMIT = 8;
const SOURCE_DIGEST_DIRECT_DETAIL_LIMIT = 4;
const SOURCE_DIGEST_SOURCE_PANEL_DETAIL_LIMIT = 8;
const SOURCE_DIGEST_GROUP_CANDIDATE_LIMIT = 40;
const SOURCE_DIGEST_PATH_PREVIEW_LIMIT = 4;

type LocalDigestIdleWindow = Window & {
  requestIdleCallback?: (callback: () => void, options?: { timeout: number }) => number;
  cancelIdleCallback?: (handle: number) => void;
};

function scheduleLocalDigestSampleLoad(callback: () => void): () => void {
  const idleWindow = window as LocalDigestIdleWindow;
  if (typeof idleWindow.requestIdleCallback === "function") {
    const handle = idleWindow.requestIdleCallback(callback, { timeout: LOCAL_DIGEST_SAMPLE_IDLE_TIMEOUT_MS });
    return () => idleWindow.cancelIdleCallback?.(handle);
  }
  const handle = window.setTimeout(callback, LOCAL_DIGEST_SAMPLE_DEFER_MS);
  return () => window.clearTimeout(handle);
}

function scheduleBusinessSummaryPanelLoad(callback: () => void): () => void {
  const idleWindow = window as LocalDigestIdleWindow;
  if (typeof idleWindow.requestIdleCallback === "function") {
    const handle = idleWindow.requestIdleCallback(callback, { timeout: BUSINESS_SUMMARY_PANEL_IDLE_TIMEOUT_MS });
    return () => idleWindow.cancelIdleCallback?.(handle);
  }
  const handle = window.setTimeout(callback, BUSINESS_SUMMARY_PANEL_DEFER_MS);
  return () => window.clearTimeout(handle);
}

const CONTROL_TABS: Array<DeckControlTabItem<ProductConfigPanel>> = [
  { key: "filters", label: "配置列", caption: "Model / Market / BOM" },
  { key: "selected", label: "已选对象", caption: "2-4 个配置列" },
  { key: "source", label: "Source Digest", caption: "上传 / 入库" },
  { key: "display", label: "显示 / 编辑", caption: "范围 / 导出" },
];

const SIMPLE_CONTROL_TABS: Array<DeckControlTabItem<ProductConfigPanel>> = [
  { key: "filters", label: "配置列", caption: "车型 / 物料" },
  { key: "selected", label: "已选对象", caption: "2-4 个配置列" },
  { key: "source", label: "来源 / 上传", caption: "配置表 / 价格单" },
  { key: "display", label: "显示 / 编辑", caption: "范围 / 导出" },
];

const DISPLAY_SCOPE_OPTIONS: DisplayScopeOption[] = [
  { key: "ALL", label: "全部配置", description: "默认展示完整配置矩阵，适合先巡检所有参数。" },
  { key: "DIFFERENCE", label: "差异项", description: "只看新增、减少、值变化、选装变化和待确认。" },
  { key: "INFERRED", label: "规则推断", description: "只看规则推断的差异，例如不配备*，需要回看来源证据。" },
  { key: "MISSING_SOURCE", label: "来源问题", description: "只看缺值或没有来源证据的配置行，适合补证据或重新消化来源。" },
  { key: "MERGED_SOURCE", label: "合并格", description: "只看来自合并单元格展开的配置项，适合核对共通参数。" },
  { key: "UNKNOWN", label: "待确认", description: "只看缺失或空值无法判断的配置项，不直接等于无配置。" },
  { key: "COMMON", label: "共同配置", description: "只看已选配置列保持一致的配置项，用于确认共性基线。" },
];

const SIMPLE_DISPLAY_SCOPE_KEYS: ReadonlySet<ConfigComparisonDeltaFilter> = new Set([
  "ALL",
  "DIFFERENCE",
]);

const SOURCE_DIGEST_QUALITY_FILTERS: Array<Omit<SourceDigestQualityFilterItem, "count">> = [
  { key: "all", label: "全部", description: "当前搜索范围内的全部可比组" },
  { key: "library", label: "来源库", description: "已上传入库、可生成在线配置列" },
  { key: "local", label: "本地样例", description: "页面内置 xlsx 样例，仅用于预览" },
  { key: "excel", label: "Excel", description: "xlsx / xls / xlsm 或 workbook digest" },
  { key: "tabular", label: "表格文本", description: "CSV / TSV / HTML / 网页表格 digest" },
  { key: "pdf", label: "PDF", description: "文本 PDF 或扫描 PDF 来源" },
  { key: "ocr", label: "OCR", description: "扫描 PDF / 图片 OCR digest" },
  { key: "ocr_temporary", label: "临时 OCR 列", description: "OCR 未识别表头，需补真实配置列身份" },
  { key: "review", label: "需核对", description: "OCR 行级对齐或长值等风险，需要人工复核" },
  { key: "price_list", label: "价格单", description: "价格单字段转成配置列" },
];

const SUMMARY_MODE_OPTIONS: SummaryModeOption[] = [
  { key: "simple", label: "简易模式", description: "AI 结论 + 完整配置表；高级诊断默认收起" },
  { key: "expert", label: "专家模式", description: "查看高级诊断、推断边界、升级路径和来源样本" },
];
const PRODUCT_CONFIG_SUMMARY_MODE_STORAGE_KEY = "jato_product_config_summary_mode_v3";

function isBusinessSummaryMode(value: string | null): value is BusinessSummaryMode {
  return value === "simple" || value === "expert";
}

function initialProductConfigSummaryMode(): BusinessSummaryMode {
  try {
    const storedMode = localStorage.getItem(PRODUCT_CONFIG_SUMMARY_MODE_STORAGE_KEY);
    return isBusinessSummaryMode(storedMode) ? storedMode : "simple";
  } catch {
    return "simple";
  }
}

function rememberProductConfigSummaryMode(mode: BusinessSummaryMode): void {
  try {
    localStorage.setItem(PRODUCT_CONFIG_SUMMARY_MODE_STORAGE_KEY, mode);
  } catch {
    // Ignore storage failures; the in-page toggle still works.
  }
}

function canEditEngineeringConfigValues(roleFromAuth: string | null | undefined): boolean {
  const role = roleFromAuth || localStorage.getItem("jato_user_role") || (import.meta.env.VITE_AUTH_TOKEN ? "admin" : "viewer");
  return role === "editor" || role === "admin" || role === "developer";
}

function parseTrimIdsParam(value: string | null): string[] {
  if (!value) return [];
  return value.split(",").map((item) => item.trim()).filter(Boolean).slice(0, 4);
}

function uniquePresent(values: Array<string | null | undefined>): string[] {
  return [...new Set(values.map((value) => value?.trim()).filter((value): value is string => Boolean(value)))];
}

function sortOptionLabels(values: string[]): string[] {
  return [...values].sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));
}

const CONFIG_LIBRARY_DROPDOWN_GROUP_RANK = 10;
const CONFIG_LIBRARY_SOURCE_FOCUS_DROPDOWN_GROUP_RANK = 5;
const CONFIG_LIBRARY_BRAND_FOCUS_DROPDOWN_GROUP_RANK = 6;
const CONFIG_LIBRARY_MODEL_FOCUS_DROPDOWN_GROUP_RANK = 7;
const SOURCE_DIGEST_LIBRARY_SOURCE_FOCUS_DROPDOWN_GROUP_RANK = 16;
const SOURCE_DIGEST_LIBRARY_MODEL_FOCUS_DROPDOWN_GROUP_RANK = 17;
const SOURCE_DIGEST_MODEL_PATH_DROPDOWN_GROUP_RANK = 18;
const SOURCE_DIGEST_LIBRARY_DROPDOWN_GROUP_RANK = 20;
const LOCAL_DIGEST_SOURCE_FOCUS_DROPDOWN_GROUP_RANK = 26;
const LOCAL_DIGEST_MODEL_FOCUS_DROPDOWN_GROUP_RANK = 27;
const LOCAL_DIGEST_DROPDOWN_GROUP_RANK = 30;

function compareDropdownOption(a: SearchDropdownOption, b: SearchDropdownOption): number {
  const rankCompare = (a.groupRank ?? 1000) - (b.groupRank ?? 1000);
  if (rankCompare !== 0) return rankCompare;
  const groupCompare = (a.group ?? "").localeCompare(b.group ?? "", undefined, { numeric: true, sensitivity: "base" });
  if (groupCompare !== 0) return groupCompare;
  return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
}

function sortDropdownOptions(options: SearchDropdownOption[]): SearchDropdownOption[] {
  return [...options].sort(compareDropdownOption);
}

function buildSimpleDropdownOptions(values: Array<string | null | undefined>): SearchDropdownOption[] {
  return sortOptionLabels(uniquePresent(values)).map((value) => ({ value, label: value }));
}

function recommendationProfileText(
  recommendation: EngineeringConfigCompetitorRecommendation,
  key: string,
): string | null {
  const value = recommendation.profile[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function competitorSharedDimensionLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (normalized === "segment") return "同 Segment";
  if (normalized === "powertrain" || normalized === "energytype" || normalized === "energy_type") return "同动力";
  if (normalized === "country" || normalized === "market") return "同国家/市场";
  return `同 ${value.trim()}`;
}

function competitorRecommendationEvidenceLabels(
  recommendation: EngineeringConfigCompetitorRecommendation,
): string[] {
  const evidenceLabels = recommendation.matchEvidence
    .map((evidence) => {
      const detail = evidence.detail?.trim();
      if (detail) return detail;
      const fieldLabel = evidence.label?.trim() || evidence.field.trim();
      const candidate = evidence.candidate ?? evidence.target;
      const candidateLabel = candidate === null || typeof candidate === "undefined" ? null : String(candidate).trim();
      const scoreLabel = typeof evidence.score === "number" ? `score ${Math.round(evidence.score)}` : null;
      return [fieldLabel, candidateLabel, scoreLabel].filter((part): part is string => Boolean(part)).join(" · ");
    })
    .filter((label) => label.length > 0);
  if (evidenceLabels.length > 0) return uniquePresent(evidenceLabels).slice(0, 3);
  const sharedLabels = recommendation.sharedDimensions
    .map((value) => value.trim())
    .filter((value) => value.length > 0)
    .map(competitorSharedDimensionLabel);
  if (sharedLabels.length > 0) return uniquePresent(sharedLabels).slice(0, 3);
  return [`AA 原始排序 #${recommendation.sourceRank || recommendation.rank}`];
}

function competitorSourceDigestMatchLabel(match: CompetitorSourceDigestMatch): string {
  return match.sourceFileName?.trim() || match.sourceId;
}

function competitorSourceDigestMatchMeta(match: CompetitorSourceDigestMatch): string {
  return `${match.groupCount} 组 · ${match.trimCount} 配置列`;
}

function sourceDigestDraftAppendsToCurrentCompare(context: EngineeringConfigSourceContext | null): boolean {
  return context?.contextType === "competitor_recommendation_source_digest"
    || context?.contextType === "competitor_recommendation_upload";
}

function uniqueCompareTrimIds(ids: string[]): string[] {
  const next: string[] = [];
  const selected = new Set<string>();
  ids.forEach((id) => {
    if (selected.has(id)) return;
    next.push(id);
    selected.add(id);
  });
  return next;
}

function mergeCreatedCompareTrimIds(currentIds: string[], createdIds: string[]): string[] {
  const next = [...currentIds];
  const selected = new Set(next);
  createdIds.forEach((id) => {
    if (next.length >= 4 || selected.has(id)) return;
    next.push(id);
    selected.add(id);
  });
  return next.length > 0 ? next : createdIds.slice(0, 4);
}

function sourceDigestDraftComparePlacement(
  currentIds: string[],
  createdIds: string[],
  appendToCurrentCompare: boolean,
): SourceDigestDraftComparePlacement {
  const normalizedCreatedIds = uniqueCompareTrimIds(createdIds).slice(0, 4);
  const ids = appendToCurrentCompare
    ? mergeCreatedCompareTrimIds(currentIds, normalizedCreatedIds)
    : normalizedCreatedIds;
  const currentIdSet = new Set(currentIds);
  const visibleIdSet = new Set(ids);
  const visibleCreatedCompareTrimCount = normalizedCreatedIds.filter((id) => visibleIdSet.has(id)).length;
  const addedToCurrentCompareCount = normalizedCreatedIds.filter((id) => visibleIdSet.has(id) && !currentIdSet.has(id)).length;
  const omittedCreatedTrimIds = normalizedCreatedIds.filter((id) => !visibleIdSet.has(id));
  return {
    ids,
    appendedToCurrentCompare: appendToCurrentCompare && currentIds.length > 0,
    omittedCreatedTrimIds,
    createdCompareTrimCount: normalizedCreatedIds.length,
    visibleCreatedCompareTrimCount,
    addedToCurrentCompareCount,
    omittedFromCurrentCompareCount: omittedCreatedTrimIds.length,
    currentCompareTrimCount: ids.length,
  };
}

function buildTrimDropdownOptions(trims: ComparableTrim[]): SearchDropdownOption[] {
  return sortOptionLabels(uniquePresent(trims.map((trim) => trim.trimName || trim.fullTrimName))).map((value) => ({
    value,
    label: value,
  }));
}

function buildSourceDropdownOptions(trims: ComparableTrim[]): SearchDropdownOption[] {
  const options: SearchDropdownOption[] = [];
  const seen = new Set<string>();
  const addOption = (value: string | null | undefined, meta: string): void => {
    const trimmed = value?.trim();
    if (!trimmed || seen.has(trimmed)) return;
    seen.add(trimmed);
    options.push({ value: trimmed, label: trimmed, meta });
  };
  trims.forEach((trim) => {
    const trimName = trim.trimName || trim.fullTrimName;
    addOption(trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath, `来源文件 · ${trimName}`);
    addOption(trim.sourceCreatedBy, `上传人 · ${trim.sourceFileName || trimName}`);
  });
  return options.sort((a, b) => a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" }));
}

function trimPickerLabel(trim: ComparableTrim): string {
  return [meaningfulTrimLabelPart(trim.brand), trim.modelName, trim.trimName || trim.fullTrimName]
    .map((value) => value?.trim())
    .filter((value): value is string => Boolean(value))
    .join(" · ") || trim.trimId;
}

function meaningfulTrimLabelPart(value: string | null | undefined): string | null {
  const cleaned = value?.trim();
  if (!cleaned) return null;
  return ["unknown", "n/a", "na", "-"].includes(cleaned.toLowerCase()) ? null : cleaned;
}

function trimPickerMeta(trim: ComparableTrim, selected: boolean): string {
  const marketYear = [
    trim.market || trim.country || "市场待补",
    trim.modelYear ? `MY ${trim.modelYear}` : "年款待补",
  ].join(" / ");
  const identity = `${trimOriginLabel(trim)} / ${trimIdentityAnchorLabel(trim)}`;
  return [
    selected ? "已选，回车可移除" : "已建，可直接加入对比",
    marketYear,
    identity,
    trimSourceSnapshotLabel(trim),
    trimSourceCreatedAtLabel(trim),
  ].filter((value): value is string => Boolean(value)).join(" · ");
}

function trimPickerSearchText(trim: ComparableTrim): string {
  return [
    trim.brand,
    trim.modelName,
    trim.trimName,
    trim.fullTrimName,
    trim.market,
    trim.country,
    trim.modelYear,
    trim.energyType,
    trim.drivetrain,
    trim.engine,
    trim.materialNo,
    trim.vehicleCode,
    trim.salesVersion,
    trim.identityKey,
    trimSourceSnapshotLabel(trim),
    trimSourceCreatedAtLabel(trim),
    trimSourceOwnerLabel(trim),
  ].filter((value): value is string => Boolean(value?.trim())).join(" · ");
}

function dropdownPathParts(values: Array<string | null | undefined>): string {
  return values
    .map((value) => value?.replace(/\s+/g, " ").trim())
    .filter((value): value is string => Boolean(value))
    .join(" / ");
}

function trimPickerPath(trim: ComparableTrim): string {
  return dropdownPathParts([
    "配置列库",
    trimOriginLabel(trim),
    trim.brand || "品牌待补",
    trim.market || trim.country ? `市场 ${trim.market || trim.country}` : "市场待补",
    trim.modelYear ? `MY ${trim.modelYear}` : "年款待补",
    trim.modelName || "车型待补",
    trim.trimName || trim.fullTrimName,
    trim.sourceFileName ? `来源 ${trimSourceSnapshotLabel(trim)}` : null,
    trimSourceCreatedAtLabel(trim),
  ]);
}

function trimSelectionIdentityKeys(trim: ComparableTrim): string[] {
  const normalized = (value: string | null | undefined): string | null => {
    const cleaned = value?.replace(/\s+/g, " ").trim().toLowerCase();
    return cleaned || null;
  };
  const keyed = (prefix: string, value: string | null | undefined): string | null => {
    const cleaned = normalized(value);
    return cleaned ? `${prefix}:${cleaned}` : null;
  };
  const sourceId = normalized(trim.sourceUploadId);
  const sourceScoped = (prefix: string, value: string | null | undefined): string | null => {
    const cleaned = normalized(value);
    return sourceId && cleaned ? `${prefix}:${sourceId}|${cleaned}` : null;
  };
  const ownCatalog = trim.dataOrigin === "own_catalog";
  return [
    keyed("trim", trim.trimId),
    ownCatalog ? keyed("material", trim.materialNo || trim.vehicleCode) : null,
    ownCatalog ? keyed("identity", trim.identityKey) : null,
    sourceScoped("source-material", trim.materialNo || trim.vehicleCode),
    sourceScoped("source-sales", trim.salesVersion),
    sourceScoped("source-identity", trim.identityKey),
    sourceScoped("source-trim", trim.trimName || trim.fullTrimName),
  ].filter((value): value is string => Boolean(value));
}

function configLibraryDirectBrandOptionValue(query: string): string {
  return `${CONFIG_LIBRARY_DIRECT_BRAND_OPTION_PREFIX}${encodeURIComponent(query)}`;
}

function configLibraryDirectSourceOptionValue(query: string): string {
  return `${CONFIG_LIBRARY_DIRECT_SOURCE_OPTION_PREFIX}${encodeURIComponent(query)}`;
}

function configLibraryDirectSourceOptionQuery(value: string): string | null {
  if (!value.startsWith(CONFIG_LIBRARY_DIRECT_SOURCE_OPTION_PREFIX)) return null;
  try {
    return decodeURIComponent(value.slice(CONFIG_LIBRARY_DIRECT_SOURCE_OPTION_PREFIX.length));
  } catch {
    return null;
  }
}

function configLibraryDirectBrandOptionQuery(value: string): string | null {
  if (!value.startsWith(CONFIG_LIBRARY_DIRECT_BRAND_OPTION_PREFIX)) return null;
  try {
    return decodeURIComponent(value.slice(CONFIG_LIBRARY_DIRECT_BRAND_OPTION_PREFIX.length));
  } catch {
    return null;
  }
}

function configLibraryDirectModelOptionValue(query: string): string {
  return `${CONFIG_LIBRARY_DIRECT_MODEL_OPTION_PREFIX}${encodeURIComponent(query)}`;
}

function configLibraryDirectModelOptionQuery(value: string): string | null {
  if (!value.startsWith(CONFIG_LIBRARY_DIRECT_MODEL_OPTION_PREFIX)) return null;
  try {
    return decodeURIComponent(value.slice(CONFIG_LIBRARY_DIRECT_MODEL_OPTION_PREFIX.length));
  } catch {
    return null;
  }
}

function configLibraryDirectModelAddOptionValue(trimIds: string[]): string {
  return `${CONFIG_LIBRARY_DIRECT_MODEL_ADD_OPTION_PREFIX}${trimIds.map((trimId) => encodeURIComponent(trimId)).join(",")}`;
}

function configLibraryDirectModelAddOptionTrimIds(value: string): string[] | null {
  if (!value.startsWith(CONFIG_LIBRARY_DIRECT_MODEL_ADD_OPTION_PREFIX)) return null;
  try {
    return value
      .slice(CONFIG_LIBRARY_DIRECT_MODEL_ADD_OPTION_PREFIX.length)
      .split(",")
      .map((trimId) => decodeURIComponent(trimId).trim())
      .filter(Boolean)
      .slice(0, 4);
  } catch {
    return null;
  }
}

function configLibraryFocusSearchQuery(values: Array<string | null | undefined>): string {
  return uniquePresent(values).join(" ");
}

function configLibraryModelFocusParts(brandValues: string[], modelLabel: string): string[] {
  const normalizedModel = normalizedMatchText(modelLabel);
  return brandValues.filter((brand) => {
    const normalizedBrand = normalizedMatchText(brand);
    return normalizedBrand.length > 0 && !normalizedModel.includes(normalizedBrand);
  });
}

function buildDirectTrimFocusDropdownOptions(trims: VehicleTrimItem[]): SearchDropdownOption[] {
  const sourceGroups = new Map<string, VehicleTrimItem[]>();
  const brandGroups = new Map<string, VehicleTrimItem[]>();
  const modelGroups = new Map<string, VehicleTrimItem[]>();
  uniqueVehicleTrims(trims).forEach((trim) => {
    const sourceValue = trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath;
    if (sourceValue?.trim()) {
      const sourceKey = sourceValue.trim().toLowerCase();
      sourceGroups.set(sourceKey, [...(sourceGroups.get(sourceKey) ?? []), trim]);
    }

    const brandKey = [
      trim.brand || "品牌待补",
      trim.market || trim.country || "市场待补",
    ].map((value) => value.trim().toLowerCase()).join("|");
    brandGroups.set(brandKey, [...(brandGroups.get(brandKey) ?? []), trim]);

    const modelKey = [
      trim.brand || "品牌待补",
      trim.modelName || "车型待补",
      trim.market || trim.country || "市场待补",
      trim.modelYear || "年款待补",
    ].map((value) => value.trim().toLowerCase()).join("|");
    modelGroups.set(modelKey, [...(modelGroups.get(modelKey) ?? []), trim]);
  });

  const sourceOptions = Array.from(sourceGroups.values()).map((items) => {
    const sourceValues = uniquePresent(items.map((trim) => trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath));
    const brandValues = uniquePresent(items.map((trim) => trim.brand));
    const marketValues = uniquePresent(items.map((trim) => trim.market || trim.country));
    const modelValues = uniquePresent(items.map((trim) => trim.modelName));
    const sourceLabel = sourceValues.length > 0 ? compactList(sourceValues) : "来源待补";
    const brandLabel = brandValues.length > 0 ? compactList(brandValues) : "品牌待补";
    const marketLabel = marketValues.length > 0 ? compactList(marketValues) : "市场待补";
    const modelLabel = modelValues.length > 0 ? compactList(modelValues) : "车型待补";
    const query = configLibraryFocusSearchQuery(sourceValues);
    return {
      value: configLibraryDirectSourceOptionValue(query || sourceLabel),
      label: `聚焦来源 · ${sourceLabel}`,
      badge: "来源",
      badgeTone: "library" as const,
      path: dropdownPathParts([
        "配置列库",
        `来源 ${sourceLabel}`,
        `品牌 ${brandLabel}`,
        `市场 ${marketLabel}`,
        `车型 ${modelLabel}`,
      ]),
      group: "配置列库 · 已建配置列 · 先聚焦来源 / 品牌 / 车型",
      groupRank: CONFIG_LIBRARY_SOURCE_FOCUS_DROPDOWN_GROUP_RANK,
      matchRankBoost: 9500,
      meta: [
        "正式库已建范围",
        `${modelValues.length} 车型`,
        `${items.length} 已建配置列`,
        brandValues.length > 0 ? `${brandValues.length} 品牌` : "品牌待补",
      ].join(" · "),
      searchText: items.map(trimPickerSearchText).join(" · "),
    };
  });

  const brandOptions = Array.from(brandGroups.values()).map((items) => {
    const brandValues = uniquePresent(items.map((trim) => trim.brand));
    const marketValues = uniquePresent(items.map((trim) => trim.market || trim.country));
    const modelValues = uniquePresent(items.map((trim) => trim.modelName));
    const sourceCount = uniquePresent(items.map((trim) => trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)).length;
    const brandLabel = brandValues.length > 0 ? compactList(brandValues) : "品牌待补";
    const marketLabel = marketValues.length > 0 ? compactList(marketValues) : "市场待补";
    const query = configLibraryFocusSearchQuery([...brandValues, ...marketValues]);
    return {
      value: configLibraryDirectBrandOptionValue(query || brandLabel),
      label: `聚焦品牌 · ${brandLabel}`,
      badge: "品牌",
      badgeTone: "library" as const,
      path: dropdownPathParts([
        "配置列库",
        `品牌 ${brandLabel}`,
        `市场 ${marketLabel}`,
      ]),
      group: "配置列库 · 已建配置列 · 先聚焦来源 / 品牌 / 车型",
      groupRank: CONFIG_LIBRARY_BRAND_FOCUS_DROPDOWN_GROUP_RANK,
      matchRankBoost: 9000,
      meta: [
        "正式库已建范围",
        `${modelValues.length} 车型`,
        `${items.length} 已建配置列`,
        sourceCount > 0 ? `${sourceCount} 来源` : "来源待补",
      ].join(" · "),
      searchText: items.map(trimPickerSearchText).join(" · "),
    };
  });

  const modelOptions = Array.from(modelGroups.values()).map((items) => {
    const sample = items[0];
    const brandValues = uniquePresent(items.map((trim) => trim.brand));
    const marketValues = uniquePresent(items.map((trim) => trim.market || trim.country));
    const modelYearValues = uniquePresent(items.map((trim) => trim.modelYear));
    const sourceCount = uniquePresent(items.map((trim) => trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)).length;
    const missingSourceCount = items.filter((trim) => !(trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)?.trim()).length;
    const brandLabel = brandValues.length > 0 ? compactList(brandValues) : "品牌待补";
    const marketLabel = marketValues.length > 0 ? compactList(marketValues) : "市场待补";
    const modelYearLabel = modelYearValues.length > 0 ? `MY ${compactList(modelYearValues)}` : "年款待补";
    const modelLabel = sample?.modelName?.trim() || "车型待补";
    const modelBrandParts = configLibraryModelFocusParts(brandValues, modelLabel);
    const query = configLibraryFocusSearchQuery([...modelBrandParts, modelLabel]);
    const displayLabel = [...modelBrandParts, modelLabel].join(" ") || modelLabel;
    return {
      value: configLibraryDirectModelOptionValue(query || modelLabel),
      label: `聚焦车型 · ${displayLabel}`,
      badge: "车型",
      badgeTone: "library" as const,
      path: dropdownPathParts([
        "配置列库",
        `品牌 ${brandLabel}`,
        `市场 ${marketLabel}`,
        modelYearLabel,
        modelLabel,
      ]),
      group: "配置列库 · 已建配置列 · 先聚焦来源 / 品牌 / 车型",
      groupRank: CONFIG_LIBRARY_MODEL_FOCUS_DROPDOWN_GROUP_RANK,
      matchRankBoost: 8500,
      meta: [
        "正式库已建范围",
        `${items.length} 已建配置列`,
        sourceCount > 1 ? `${sourceCount} 来源，先聚焦核对` : sourceCount > 0 ? `${sourceCount} 来源` : "来源待补",
        missingSourceCount > 0 ? `${missingSourceCount} 列缺来源` : null,
      ].filter((part): part is string => Boolean(part)).join(" · "),
      searchText: items.map(trimPickerSearchText).join(" · "),
    };
  });
  const modelAddOptions = Array.from(modelGroups.values()).flatMap((items) => {
    const uniqueItems = uniqueVehicleTrims(items);
    if (uniqueItems.length < 2 || uniqueItems.length > 4) return [];
    const sourceValues = uniquePresent(uniqueItems.map((trim) => trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath));
    const hasMissingSource = uniqueItems.some((trim) => !(trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)?.trim());
    if (sourceValues.length !== 1 || hasMissingSource) return [];
    const sample = uniqueItems[0];
    const brandValues = uniquePresent(uniqueItems.map((trim) => trim.brand));
    const marketValues = uniquePresent(uniqueItems.map((trim) => trim.market || trim.country));
    const modelYearValues = uniquePresent(uniqueItems.map((trim) => trim.modelYear));
    const brandLabel = brandValues.length > 0 ? compactList(brandValues) : "品牌待补";
    const marketLabel = marketValues.length > 0 ? compactList(marketValues) : "市场待补";
    const modelYearLabel = modelYearValues.length > 0 ? `MY ${compactList(modelYearValues)}` : "年款待补";
    const modelLabel = sample?.modelName?.trim() || "车型待补";
    const modelBrandParts = configLibraryModelFocusParts(brandValues, modelLabel);
    const displayLabel = [...modelBrandParts, modelLabel].join(" ") || modelLabel;
    const trimPreview = compactList(uniquePresent(uniqueItems.map((trim) => trim.trimName || trim.fullTrimName || trim.salesVersion)));
    return [{
      value: configLibraryDirectModelAddOptionValue(uniqueItems.map((trim) => trim.trimId)),
      label: `加入车型配置列 · ${displayLabel}`,
      badge: "整车加入",
      badgeTone: "source" as const,
      path: dropdownPathParts([
        "配置列库",
        `品牌 ${brandLabel}`,
        `市场 ${marketLabel}`,
        modelYearLabel,
        modelLabel,
      ]),
      group: "配置列库 · 车型一键加入",
      groupRank: CONFIG_LIBRARY_MODEL_FOCUS_DROPDOWN_GROUP_RANK - 1,
      matchRankBoost: 9800,
      meta: [
        `${uniqueItems.length} 个配置列，选择后直接加入对比`,
        trimPreview,
        `单一来源 ${sourceValues[0]}`,
      ].filter((part): part is string => Boolean(part)).join(" · "),
      searchText: uniqueItems.map(trimPickerSearchText).join(" · "),
    }];
  });

  return sortDropdownOptions([...sourceOptions, ...brandOptions, ...modelAddOptions, ...modelOptions]);
}

function buildDirectTrimDropdownOptions(trims: VehicleTrimItem[], selectedIds: string[], selectionFull: boolean): SearchDropdownOption[] {
  const selected = new Set(selectedIds);
  const focusOptions = buildDirectTrimFocusDropdownOptions(trims);
  const trimOptions = uniqueVehicleTrims(trims)
    .map((trim) => {
      const selectedTrim = selected.has(trim.trimId);
      const disabledByLimit = selectionFull && !selectedTrim;
      const badgeTone: SearchDropdownOption["badgeTone"] = selectedTrim ? "library" : disabledByLimit ? "muted" : "source";
      const trimMetaText = trimPickerMeta(trim, selectedTrim);
      return {
        value: trim.trimId,
        label: trimPickerLabel(trim),
        badge: selectedTrim ? "已选" : disabledByLimit ? "已满" : "已建",
        badgeTone,
        disabled: disabledByLimit,
        path: trimPickerPath(trim),
        meta: disabledByLimit ? `已满 4 列，先移除一个配置列 · ${trimMetaText}` : trimMetaText,
        group: "配置列库 · 已发布 / 草稿",
        groupRank: CONFIG_LIBRARY_DROPDOWN_GROUP_RANK,
        keepOpenOnSelect: true,
        preserveQueryOnSelect: true,
        searchText: trimPickerSearchText(trim),
      };
    })
    .sort((a, b) => {
      const selectedRank = Number(b.meta.startsWith("已选")) - Number(a.meta.startsWith("已选"));
      if (selectedRank !== 0) return selectedRank;
      return compareDropdownOption(a, b);
    });
  return [...focusOptions, ...trimOptions];
}

function mergeDropdownOptions(...optionLists: SearchDropdownOption[][]): SearchDropdownOption[] {
  const byValue = new Map<string, SearchDropdownOption>();
  optionLists.flat().forEach((option) => {
    const value = option.value.trim();
    if (!value || byValue.has(value)) return;
    byValue.set(value, { ...option, value });
  });
  return sortDropdownOptions(Array.from(byValue.values()));
}

function sourceDigestFormatLabel(sourceFormat: SourceDigestGroupCandidate["sourceFormat"]): string | null {
  if (sourceFormat === "workbook") return "Excel";
  if (sourceFormat === "tabular") return "表格文本";
  if (sourceFormat === "pdf_text") return "文本 PDF";
  if (sourceFormat === "pdf_ocr") return "扫描 PDF OCR";
  if (sourceFormat === "image_ocr") return "图片 OCR";
  return sourceFormat?.trim() || null;
}

function sourceDigestFileExtension(candidate: SourceDigestGroupCandidate): string {
  const match = candidate.sourceFileName.toLowerCase().match(/\.([a-z0-9]+)$/);
  return match?.[1] ?? "";
}

function sourceDigestCandidateIsExcel(candidate: SourceDigestGroupCandidate): boolean {
  const sourceFormat = candidate.sourceFormat;
  const extension = sourceDigestFileExtension(candidate);
  return sourceFormat === "workbook" || ["xlsx", "xls", "xlsm"].includes(extension);
}

function sourceDigestCandidateIsPdf(candidate: SourceDigestGroupCandidate): boolean {
  const sourceFormat = candidate.sourceFormat;
  return sourceFormat === "pdf_text" || sourceFormat === "pdf_ocr" || sourceDigestFileExtension(candidate) === "pdf";
}

function sourceDigestCandidateIsTabular(candidate: SourceDigestGroupCandidate): boolean {
  const sourceFormat = candidate.sourceFormat;
  const extension = sourceDigestFileExtension(candidate);
  return candidate.sourceDigestType === "tabular"
    || sourceFormat === "tabular"
    || ["csv", "tsv", "html", "htm"].includes(extension);
}

function sourceDigestCandidateIsOcr(candidate: SourceDigestGroupCandidate): boolean {
  return candidate.sourceFormat === "pdf_ocr"
    || candidate.sourceFormat === "image_ocr"
    || Boolean(candidate.ocrEngine || candidate.ocrEvaluation);
}

function sourceDigestTrimHasTemporaryOcrIdentity(trim: EngineeringConfigSourceDigestGroup["trims"][number]): boolean {
  return trim.identityStatus === "temporary_ocr_column"
    || (trim.trimName || trim.fullTrimName || "").trim().toLowerCase().startsWith("ocr column");
}

function sourceDigestCandidateHasTemporaryOcrIdentity(candidate: SourceDigestGroupCandidate): boolean {
  return candidate.group.identityStatus === "temporary_ocr_column"
    || candidate.group.sourceKind === "ocr_headerless"
    || candidate.group.trims.some(sourceDigestTrimHasTemporaryOcrIdentity);
}

function sourceDigestCandidateIsPriceList(candidate: SourceDigestGroupCandidate): boolean {
  return candidate.group.sourceKind === "price_list";
}

function sourceDigestCandidateMatchesQualityFilter(
  candidate: SourceDigestGroupCandidate,
  filter: SourceDigestQualityFilterKey,
): boolean {
  if (filter === "all") return true;
  if (filter === "library") return candidate.sourceKind === "library";
  if (filter === "local") return candidate.sourceKind === "local";
  if (filter === "excel") return sourceDigestCandidateIsExcel(candidate);
  if (filter === "tabular") return sourceDigestCandidateIsTabular(candidate);
  if (filter === "pdf") return sourceDigestCandidateIsPdf(candidate);
  if (filter === "ocr") return sourceDigestCandidateIsOcr(candidate);
  if (filter === "ocr_temporary") return sourceDigestCandidateHasTemporaryOcrIdentity(candidate);
  if (filter === "review") return sourceDigestReviewRows(candidate).length > 0;
  if (filter === "price_list") return sourceDigestCandidateIsPriceList(candidate);
  return true;
}

function sourceDigestQualityFilterItems(candidates: SourceDigestGroupCandidate[]): SourceDigestQualityFilterItem[] {
  return SOURCE_DIGEST_QUALITY_FILTERS.map((filter) => ({
    ...filter,
    count: candidates.filter((candidate) => sourceDigestCandidateMatchesQualityFilter(candidate, filter.key)).length,
  }));
}

function sourceDigestMarketValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([
    candidate.sourceContext?.country,
    candidate.sourceContext?.market,
    ...candidate.group.trims.flatMap((trim) => [trim.market, trim.country, trim.profile?.country]),
  ]);
}

function sourceDigestModelYearValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([
    candidate.sourceContext?.modelYear,
    ...candidate.group.trims.flatMap((trim) => [trim.profile?.modelYear]),
  ]);
}

function sourceDigestSegmentValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([candidate.sourceContext?.segment]);
}

function sourceDigestBrandValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([
    candidate.sourceContext?.brand,
    ...candidate.group.trims.map((trim) => trim.profile?.brand),
  ]);
}

function inferSourceDigestPowertrainFromText(values: Array<string | null | undefined>): string[] {
  const haystack = values.filter((value): value is string => Boolean(value?.trim())).join(" ").toUpperCase();
  const inferred: string[] = [];
  if (/\bPHEV\b|PLUG[-\s]?IN\s+HYBRID|插电|插混/.test(haystack)) inferred.push("PHEV");
  if (/\bBEV\b|ELECTRIC|100KWH|纯电/.test(haystack)) inferred.push("BEV");
  if (/\bHEV\b|HYBRID|混动/.test(haystack)) inferred.push("HEV");
  if (/\bICE\b|GASOLINE|PETROL|DIESEL|燃油/.test(haystack)) inferred.push("ICE");
  return uniquePresent(inferred);
}

function sourceDigestContextValues(candidate: SourceDigestGroupCandidate): string[] {
  const context = candidate.sourceContext;
  if (!context) return [];
  return uniquePresent([
    context.brand,
    context.model,
    context.market,
    context.country,
    context.segment,
    context.modelYear,
    context.contextType,
    context.scenario,
    context.identityAnchor,
  ]);
}

function sourceDigestGroupModelValues(candidate: SourceDigestGroupCandidate): string[] {
  const contextModel = candidate.sourceContext?.model?.trim();
  if (contextModel && candidate.sourceGroupCount === 1) return [contextModel];
  const modelValues = uniquePresent([
    candidate.group.modelName,
    ...candidate.group.trims.map((trim) => trim.modelName),
  ]);
  return modelValues.length > 0 ? modelValues : uniquePresent([candidate.group.title]);
}

function sourceDigestCandidateModelLabel(candidate: SourceDigestGroupCandidate): string {
  return sourceDigestGroupModelValues(candidate)[0] || candidate.group.title.trim() || "车型待补";
}

function sourceDigestModelValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([
    candidate.sourceContext?.model,
    ...sourceDigestGroupModelValues(candidate),
    candidate.group.title,
  ]);
}

function sourceDigestPowertrainValues(candidate: SourceDigestGroupCandidate): string[] {
  const explicitValues = uniquePresent(candidate.group.trims.flatMap((trim) => [
    trim.powertrain,
    trim.energyType,
    trim.energy_type,
    trim.drivetrain,
    trim.engine,
    trim.fuel,
    trim.fuelType,
    trim.fuel_type,
    trim.profile?.powertrain,
    trim.profile?.energyType,
    trim.profile?.energy_type,
    trim.profile?.drivetrain,
    trim.profile?.engine,
    trim.profile?.fuel,
    trim.profile?.fuelType,
    trim.profile?.fuel_type,
  ]));
  if (explicitValues.length > 0) return explicitValues;
  return inferSourceDigestPowertrainFromText([
    candidate.sourceContext?.powertrain,
    candidate.group.modelName,
    candidate.group.title,
    candidate.group.sourceSheet,
    candidate.sourceFileName,
    ...candidate.group.trims.flatMap((trim) => [
      trim.modelName,
      trim.trimName,
      trim.fullTrimName,
      trim.salesVersion,
      trim.profile?.configurationVersion,
    ]),
  ]);
}

function sourceDigestTrimNameValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent(candidate.group.trims.map((trim) => trim.trimName || trim.fullTrimName || trim.trimId));
}

function sourceDigestAnchorValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent(candidate.group.trims.flatMap((trim) => [
    trim.materialNo,
    trim.profile?.materialNo,
    trim.salesVersion,
    trim.profile?.configurationVersion,
    trim.fullTrimName,
    trim.trimId,
  ]));
}

function sourceDigestQualityValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([
    sourceDigestFormatLabel(candidate.sourceFormat),
    candidate.ocrEngine,
    candidate.ocrEvaluation?.selectedEngine,
    candidate.ocrEvaluation?.strategy,
    candidate.ocrEvaluation?.reason,
    ...(candidate.ocrEvaluation?.selectedReasonDetails ?? []),
    sourceDigestReviewMeta(candidate),
    candidate.group.sourceKind === "price_list" ? "价格单" : null,
    sourceDigestCandidateHasTemporaryOcrIdentity(candidate) ? "临时 OCR 列 待补身份 OCR Column temporary identity" : null,
    candidate.sourceKind === "local" ? "本地样例" : "来源库",
  ]);
}

function sourceDigestOwnerLabel(candidate: SourceDigestGroupCandidate): string | null {
  if (candidate.sourceKind === "local") return null;
  const createdBy = candidate.createdBy?.trim();
  return createdBy ? `上传人 ${createdBy}` : "上传人待补";
}

function sourceDigestCandidateSearchValues(candidate: SourceDigestGroupCandidate): string[] {
  const { group, sourceFileName } = candidate;
  return uniquePresent([
    group.modelName,
    group.title,
    group.sourceSheet,
    sourceFileName,
    candidate.createdBy,
    sourceDigestOwnerLabel(candidate),
    sourceDigestCandidateDropdownValue(candidate),
    ...sourceDigestBrandValues(candidate),
    ...sourceDigestContextValues(candidate),
    ...sourceDigestMarketValues(candidate),
    ...sourceDigestModelYearValues(candidate),
    ...sourceDigestSegmentValues(candidate),
    ...sourceDigestPowertrainValues(candidate),
    ...sourceDigestTrimNameValues(candidate),
    ...sourceDigestAnchorValues(candidate),
    ...sourceDigestQualityValues(candidate),
    ...(candidate.sourceSearchMatches ?? []),
  ]);
}

function sourceDigestCandidateFilterValues(candidate: SourceDigestGroupCandidate): string[] {
  const { group, sourceFileName } = candidate;
  return uniquePresent([
    group.modelName,
    group.title,
    group.sourceSheet,
    sourceFileName,
    candidate.createdBy,
    sourceDigestOwnerLabel(candidate),
    sourceDigestCandidateDropdownValue(candidate),
    ...sourceDigestBrandValues(candidate),
    ...sourceDigestContextValues(candidate),
    ...sourceDigestMarketValues(candidate),
    ...sourceDigestModelYearValues(candidate),
    ...sourceDigestSegmentValues(candidate),
    ...sourceDigestPowertrainValues(candidate),
    ...sourceDigestTrimNameValues(candidate),
    ...sourceDigestAnchorValues(candidate),
    ...sourceDigestQualityValues(candidate),
  ]);
}

function sourceDigestSourceValues(candidate: SourceDigestGroupCandidate): string[] {
  return uniquePresent([
    candidate.sourceFileName,
    candidate.group.sourceSheet,
    candidate.createdBy,
    sourceDigestOwnerLabel(candidate),
    ...sourceDigestContextValues(candidate),
  ]);
}

function buildSourceDigestSourceDropdownOptions(candidates: SourceDigestGroupCandidate[]): SearchDropdownOption[] {
  const byValue = new Map<string, SearchDropdownOption>();
  const addOption = (value: string | null | undefined, label: string | null | undefined, meta: string): void => {
    const normalizedValue = value?.trim();
    const normalizedLabel = label?.trim() || normalizedValue;
    if (!normalizedValue || !normalizedLabel || byValue.has(normalizedValue)) return;
    byValue.set(normalizedValue, { value: normalizedValue, label: normalizedLabel, meta });
  };

  candidates.forEach((candidate) => {
    const { group, sourceFileName } = candidate;
    addOption(sourceFileName, sourceFileName, `Source Digest 文件 · ${group.modelName}`);
    addOption(group.sourceSheet, group.sourceSheet, `Source Digest sheet · ${sourceFileName}`);
    addOption(candidate.createdBy, candidate.createdBy, `Source Digest 上传人 · ${sourceFileName}`);
  });

  return Array.from(byValue.values()).sort((a, b) => (
    a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" })
  ));
}

function buildSourceDigestKeywordOptions(candidates: SourceDigestGroupCandidate[]): SearchDropdownOption[] {
  const byValue = new Map<string, SearchDropdownOption>();
  const addOption = (value: string | null | undefined, label: string | null | undefined, meta: string): void => {
    const normalizedValue = value?.trim();
    const normalizedLabel = label?.trim() || normalizedValue;
    if (!normalizedValue || !normalizedLabel || byValue.has(normalizedValue)) return;
    byValue.set(normalizedValue, { value: normalizedValue, label: normalizedLabel, meta });
  };

  candidates.forEach((candidate) => {
    const { group, sourceFileName } = candidate;
    const ownerLabel = sourceDigestOwnerLabel(candidate);
    const ownerMeta = ownerLabel ? ` · ${ownerLabel}` : "";
    addOption(group.modelName, group.modelName, `Source Digest · ${sourceFileName} · ${group.trimCount} 配置列${ownerMeta}`);
    addOption(group.title, group.title, `Source Digest · ${group.sourceSheet}${ownerMeta}`);
    addOption(group.sourceSheet, group.sourceSheet, `Source sheet · ${sourceFileName}${ownerMeta}`);
    addOption(candidate.createdBy, candidate.createdBy, `Source Digest 上传人 · ${sourceFileName}`);
    sourceDigestBrandValues(candidate).forEach((brand) => {
      addOption(brand, brand, `Source Digest 品牌 · ${group.modelName}`);
    });
    sourceDigestContextValues(candidate).forEach((contextValue) => {
      addOption(contextValue, contextValue, `Source Digest 上下文 · ${sourceFileName}`);
    });
    sourceDigestMarketValues(candidate).forEach((market) => {
      addOption(market, market, `Source Digest 市场 · ${group.modelName}`);
    });
    sourceDigestModelYearValues(candidate).forEach((modelYear) => {
      addOption(modelYear, modelYear, `Source Digest 年款 · ${group.modelName}`);
    });
    sourceDigestSegmentValues(candidate).forEach((segment) => {
      addOption(segment, segment, `Source Digest Segment · ${group.modelName}`);
    });
    sourceDigestPowertrainValues(candidate).forEach((powertrain) => {
      addOption(powertrain, powertrain, `Source Digest 动力 · ${group.modelName}`);
    });
    sourceDigestQualityValues(candidate).forEach((qualityValue) => {
      addOption(qualityValue, qualityValue, `Source Digest 来源质量 · ${group.modelName}`);
    });
    candidate.sourceSearchMatches?.forEach((match) => {
      addOption(match, match, `Source Digest 命中 · ${sourceFileName}`);
    });
    group.trims.forEach((trim) => {
      const trimName = trim.trimName || trim.fullTrimName || trim.trimId;
      addOption(trim.materialNo, trim.materialNo, `Source Digest 物料号 · ${group.modelName}`);
      addOption(trim.profile?.materialNo, trim.profile?.materialNo, `Source Digest 物料号 · ${group.modelName}`);
      addOption(trim.salesVersion, trim.salesVersion, `Source Digest sales version · ${group.modelName}`);
      addOption(trim.profile?.configurationVersion, trim.profile?.configurationVersion, `Source Digest sales version · ${group.modelName}`);
      addOption(trimName, trimName, `Source Digest 配置列 · ${group.modelName}`);
    });
  });

  return Array.from(byValue.values()).sort((a, b) => (
    a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" })
  ));
}

function sourceDigestCandidateDropdownValue(candidate: SourceDigestGroupCandidate): string {
  return uniquePresent([
    sourceDigestCandidateModelLabel(candidate),
    ...sourceDigestModelValues(candidate),
    ...sourceDigestBrandValues(candidate),
    ...sourceDigestMarketValues(candidate),
    ...sourceDigestModelYearValues(candidate),
    ...sourceDigestPowertrainValues(candidate),
    candidate.sourceFileName,
    candidate.group.sourceSheet,
    candidate.createdBy,
    ...candidate.group.trims.flatMap((trim) => [
      trim.trimName,
      trim.fullTrimName,
      trim.materialNo,
      trim.profile?.materialNo,
      trim.salesVersion,
      trim.profile?.configurationVersion,
    ]),
  ]).join(" ");
}

function buildSourceDigestCandidateDropdownOptions(candidates: SourceDigestGroupCandidate[]): SearchDropdownOption[] {
  const byValue = new Map<string, SearchDropdownOption>();

  candidates.forEach((candidate) => {
    const modelLabel = sourceDigestCandidateModelLabel(candidate);
    const trimPreview = sourceDigestTrimPreview(candidate);
    const searchValue = sourceDigestCandidateDropdownValue(candidate);
    if (!searchValue || byValue.has(searchValue)) return;

    byValue.set(searchValue, {
      value: searchValue,
      label: modelLabel,
      badge: "整组",
      badgeTone: candidate.sourceKind === "library" ? "library" : "local",
      group: "来源 / 车型路径",
      groupRank: SOURCE_DIGEST_MODEL_PATH_DROPDOWN_GROUP_RANK,
      matchRankBoost: 1400,
      meta: `${sourceDigestDropdownScopeLabel(candidate)} · ${candidate.group.trimCount} 配置列 · ${trimPreview}`,
      searchText: sourceDigestCandidateSearchValues(candidate).join(" "),
    });
  });

  return sortDropdownOptions(Array.from(byValue.values()));
}

function sourceDigestGroupIdentityKey(group: EngineeringConfigSourceDigestGroup, sourceGroupIndex?: number): string {
  return [
    group.groupId,
    group.sourceSheet,
    group.modelName,
    group.title,
    typeof sourceGroupIndex === "number" ? `idx-${sourceGroupIndex}` : null,
  ]
    .map((value) => value?.toString().replace(/\s+/g, " ").trim())
    .filter((value): value is string => Boolean(value))
    .map(encodeURIComponent)
    .join(":");
}

function localDigestGroupKey(group: EngineeringConfigSourceDigestGroup, sourceGroupIndex?: number): string {
  return [
    "local",
    "local",
    sourceDigestGroupIdentityKey(group, sourceGroupIndex),
  ].filter(Boolean).join(":");
}

function sourceDigestCandidateKey(candidate: SourceDigestGroupCandidate): string {
  const sourceScope = candidate.sourceKind === "local"
    ? "local"
    : candidate.sourceId ?? candidate.sourceFileName;
  return [
    candidate.sourceKind,
    sourceScope,
    sourceDigestGroupIdentityKey(candidate.group, candidate.sourceGroupIndex),
  ].filter(Boolean).join(":");
}

function sourceDigestTrimId(trim: EngineeringConfigSourceDigestGroup["trims"][number]): string {
  return trim.trimId.trim();
}

function sourceDigestTrimLabel(trim: EngineeringConfigSourceDigestGroup["trims"][number]): string {
  return trim.trimName || trim.fullTrimName || trim.trimId;
}

const SOURCE_DIGEST_TRIM_IDENTITY_FIELDS: SourceDigestTrimIdentityFieldKey[] = [
  "brand",
  "modelName",
  "trimName",
  "fullTrimName",
  "market",
  "country",
  "modelYear",
  "energyType",
  "drivetrain",
  "engine",
  "materialNo",
  "salesVersion",
];

const SOURCE_DIGEST_TRIM_IDENTITY_FORM_FIELDS: Array<{
  key: SourceDigestTrimIdentityFieldKey;
  label: string;
  required?: boolean;
  placeholder: string;
}> = [
  { key: "brand", label: "Brand", placeholder: "品牌" },
  { key: "modelName", label: "车型", required: true, placeholder: "真实车型" },
  { key: "trimName", label: "配置列", required: true, placeholder: "真实配置列名称" },
  { key: "market", label: "Market", placeholder: "市场" },
  { key: "modelYear", label: "MY", placeholder: "年款" },
  { key: "energyType", label: "Powertrain", placeholder: "ICE / BEV / HEV" },
  { key: "drivetrain", label: "Drive", placeholder: "FWD / AWD" },
  { key: "materialNo", label: "Material No.", placeholder: "本品物料号" },
  { key: "salesVersion", label: "Sales version", placeholder: "销售版本" },
];

function cleanSourceDigestIdentityValue(value: string | null | undefined): string | undefined {
  const cleaned = value?.trim();
  return cleaned ? cleaned : undefined;
}

function sourceDigestTrimIdentityDefaults(
  candidate: SourceDigestGroupCandidate,
  trim: EngineeringConfigSourceDigestGroup["trims"][number],
): EngineeringConfigDigestTrimIdentityOverride {
  const temporaryOcrIdentity = sourceDigestTrimHasTemporaryOcrIdentity(trim);
  const market = sourceDigestMarketValues(candidate)[0] || "";
  const powertrain = sourceDigestPowertrainValues(candidate)[0] || "";
  const modelName = trim.modelName || candidate.group.modelName || "";
  const trimName = temporaryOcrIdentity ? "" : sourceDigestTrimLabel(trim);
  return {
    trimId: sourceDigestTrimId(trim),
    brand: sourceDigestBrandValues(candidate)[0] || "",
    modelName,
    trimName,
    fullTrimName: temporaryOcrIdentity ? "" : [modelName, trimName].filter(Boolean).join(" "),
    market,
    country: candidate.sourceContext?.country || trim.country || trim.market || trim.profile?.country || market,
    modelYear: sourceDigestModelYearValues(candidate)[0] || "",
    energyType: trim.energyType || trim.energy_type || powertrain,
    drivetrain: trim.drivetrain || trim.profile?.drivetrain || "",
    engine: trim.engine || trim.profile?.engine || "",
    materialNo: trim.materialNo || trim.profile?.materialNo || "",
    salesVersion: trim.salesVersion || trim.profile?.configurationVersion || "",
  };
}

function sourceDigestTrimIdentityDraftValue(
  candidate: SourceDigestGroupCandidate,
  trim: EngineeringConfigSourceDigestGroup["trims"][number],
  drafts: SourceDigestTrimIdentityDraftMap,
): EngineeringConfigDigestTrimIdentityOverride {
  const candidateKey = sourceDigestCandidateKey(candidate);
  return {
    ...sourceDigestTrimIdentityDefaults(candidate, trim),
    ...(drafts[candidateKey]?.[sourceDigestTrimId(trim)] ?? {}),
    trimId: sourceDigestTrimId(trim),
  };
}

function sourceDigestSelectedTemporaryOcrTrims(
  candidate: SourceDigestGroupCandidate,
  selectedTrimIds: string[],
): EngineeringConfigSourceDigestGroup["trims"] {
  const selectedIdSet = new Set(selectedTrimIds);
  return candidate.group.trims.filter((trim) => (
    selectedIdSet.has(sourceDigestTrimId(trim)) && sourceDigestTrimHasTemporaryOcrIdentity(trim)
  ));
}

function sourceDigestTemporaryIdentityReady(
  candidate: SourceDigestGroupCandidate,
  selectedTrimIds: string[],
  drafts: SourceDigestTrimIdentityDraftMap,
): boolean {
  const temporaryTrims = sourceDigestSelectedTemporaryOcrTrims(candidate, selectedTrimIds);
  if (temporaryTrims.length === 0) return true;
  return temporaryTrims.every((trim) => {
    const draft = sourceDigestTrimIdentityDraftValue(candidate, trim, drafts);
    return Boolean(draft.modelName?.trim() && draft.trimName?.trim());
  });
}

function sourceDigestTrimIdentityOverridePayload(
  candidate: SourceDigestGroupCandidate,
  selectedTrimIds: string[],
  drafts: SourceDigestTrimIdentityDraftMap,
): EngineeringConfigDigestTrimIdentityOverride[] {
  return sourceDigestSelectedTemporaryOcrTrims(candidate, selectedTrimIds).map((trim) => {
    const draft = sourceDigestTrimIdentityDraftValue(candidate, trim, drafts);
    const override: EngineeringConfigDigestTrimIdentityOverride = { trimId: sourceDigestTrimId(trim) };
    SOURCE_DIGEST_TRIM_IDENTITY_FIELDS.forEach((field) => {
      const value = cleanSourceDigestIdentityValue(draft[field]);
      if (value) override[field] = value;
    });
    return override;
  });
}

function defaultSourceDigestTrimIds(group: EngineeringConfigSourceDigestGroup): string[] {
  return group.trims.map(sourceDigestTrimId).filter(Boolean).slice(0, 4);
}

function normaliseSourceDigestTrimSelection(
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[] | undefined,
): string[] {
  const availableIds = new Set(group.trims.map(sourceDigestTrimId).filter(Boolean));
  const selected = (selectedTrimIds ?? [])
    .map((value) => value.trim())
    .filter((value, index, values) => Boolean(value) && availableIds.has(value) && values.indexOf(value) === index)
    .slice(0, 4);
  return selected.length >= 2 ? selected : defaultSourceDigestTrimIds(group);
}

function selectedSourceDigestTrimIds(
  candidate: SourceDigestGroupCandidate,
  selectionMap: SourceDigestTrimSelectionMap,
): string[] {
  return normaliseSourceDigestTrimSelection(candidate.group, selectionMap[sourceDigestCandidateKey(candidate)]);
}

function directSourceDigestPendingTrimIds(candidate: SourceDigestGroupCandidate, selectedTrimIds: string[] | undefined): string[] {
  const availableIds = new Set(candidate.group.trims.map(sourceDigestTrimId).filter(Boolean));
  return (selectedTrimIds ?? [])
    .map((value) => value.trim())
    .filter((value, index, values) => Boolean(value) && availableIds.has(value) && values.indexOf(value) === index)
    .slice(0, 4);
}

function buildDirectSourceDigestPendingItems(
  pendingCandidates: SourceDigestPendingCandidateMap,
  selectionMap: SourceDigestTrimSelectionMap,
  candidates: SourceDigestGroupCandidate[],
): DirectSourceDigestPendingItem[] {
  const candidateByKey = new Map(candidates.map((candidate) => [sourceDigestCandidateKey(candidate), candidate]));
  return Object.entries(selectionMap).flatMap(([key, selectedTrimIds]) => {
    const candidate = pendingCandidates[key] ?? candidateByKey.get(key);
    if (!candidate) return [];
    const normalizedIds = directSourceDigestPendingTrimIds(candidate, selectedTrimIds);
    if (normalizedIds.length === 0) return [];
    const normalizedIdSet = new Set(normalizedIds);
    return [{
      candidate,
      key,
      selectedTrimIds: normalizedIds,
      selectedTrims: candidate.group.trims.filter((trim) => normalizedIdSet.has(sourceDigestTrimId(trim))),
    }];
  });
}

function sourceDigestSelectedTrimPayload(
  candidate: SourceDigestGroupCandidate,
  selectionMap: SourceDigestTrimSelectionMap,
): { trimIds: string[] } | undefined {
  const selected = selectedSourceDigestTrimIds(candidate, selectionMap);
  return sourceDigestSelectedTrimPayloadFromIds(candidate, selected);
}

function sourceDigestSelectedTrimPayloadFromIds(
  candidate: SourceDigestGroupCandidate,
  selected: string[],
): { trimIds: string[] } | undefined {
  const all = candidate.group.trims.map(sourceDigestTrimId).filter(Boolean);
  const selectedDiffersFromAll = selected.length !== all.length || selected.some((trimId, index) => trimId !== all[index]);
  return selectedDiffersFromAll ? { trimIds: selected } : undefined;
}

function sourceDigestGroupWithSelectedTrims(
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[],
): EngineeringConfigSourceDigestGroup {
  const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
  const indexByTrimId = new Map(group.trims.map((trim, index) => [sourceDigestTrimId(trim), index]));
  const selectedIndexes = selected
    .map((trimId) => indexByTrimId.get(trimId))
    .filter((index): index is number => typeof index === "number");
  if (selectedIndexes.length === group.trims.length) return group;
  return {
    ...group,
    trimCount: selectedIndexes.length,
    trims: selectedIndexes.map((index) => group.trims[index]),
    rows: group.rows.map((row) => ({
      ...row,
      uniqueTrimIds: row.uniqueTrimIds.filter((trimId) => selected.includes(trimId)),
      values: selectedIndexes.map((index) => row.values[index] ?? null),
    })),
    summary: {
      ...group.summary,
      shownFeatures: group.rows.length,
    },
  };
}

function sourceSnapshotComparableGroupCount(snapshot: EngineeringConfigSourceSnapshot): number {
  const directGroupCount = snapshot.sourceDigest?.compareGroups.filter((group) => group.trimCount >= 2).length ?? 0;
  return directGroupCount || snapshot.sourceDigestStatus?.summary?.comparableGroupCount || 0;
}

function sourceSnapshotHasComparableDigest(snapshot: EngineeringConfigSourceSnapshot): boolean {
  if (snapshot.sourceDigest?.compareGroups.some((group) => group.trimCount >= 2)) return true;
  return snapshot.extractStatus === "digest_ready" && (snapshot.sourceDigestStatus?.summary?.comparableGroupCount ?? 0) > 0;
}

function sourceSnapshotDigestReadinessLabel(snapshot: EngineeringConfigSourceSnapshot): string {
  if (sourceSnapshotHasComparableDigest(snapshot)) return "可转配置列";
  const status = snapshot.sourceDigestStatus?.status;
  const comparableGroupCount = snapshot.sourceDigestStatus?.summary?.comparableGroupCount ?? 0;
  if (snapshot.extractStatus === "pending" || status === "pending") return "已入库，等待解析";
  if (snapshot.extractStatus === "digest_ready" && comparableGroupCount === 0) return "Digest 已完成，暂无可比配置组";
  if (status === "failed" || snapshot.errorMessage || snapshot.sourceDigestStatus?.errorMessage) return "解析失败，需检查来源";
  if (snapshot.extractStatus === "not_applicable") return "不可转配置表";
  return "暂不可转配置列";
}

function sourceSnapshotDigestIssuePreview(snapshot: EngineeringConfigSourceSnapshot): string | null {
  const issue = snapshot.sourceDigestStatus?.message
    || snapshot.sourceDigestStatus?.errorMessage
    || snapshot.errorMessage;
  const normalizedIssue = issue?.replace(/\s+/g, " ").trim();
  if (!normalizedIssue) return null;
  return normalizedIssue.length > 160 ? `${normalizedIssue.slice(0, 157)}...` : normalizedIssue;
}

function sourceDigestCandidatesFromSnapshot(snapshot: EngineeringConfigSourceSnapshot): SourceDigestGroupCandidate[] {
  const digest = snapshot.sourceDigest;
  if (!digest) return [];
  const sourceSearchMatches = snapshot.sourceSearchMatches?.filter(Boolean) ?? [];
  return digest.compareGroups
    .filter((group) => group.trimCount >= 2)
    .map((group, sourceGroupIndex) => ({
      group,
      ocrEngine: digest.ocrEngine,
      ocrEngineCandidates: digest.ocrEngineCandidates,
      ocrEvaluation: digest.ocrEvaluation,
      sourceContext: snapshot.relatedContext,
      sourceDigestType: digest.digestType,
      sourceId: snapshot.sourceId,
      sourceFileName: snapshot.sourceFileName,
      sourceFormat: digest.sourceFormat,
      sourceGroupIndex,
      sourceGroupCount: digest.compareGroups.filter((item) => item.trimCount >= 2).length,
      sourceKind: "library" as const,
      sourceSearchMatches,
      createdBy: snapshot.createdBy,
    }));
}

function sourceSnapshotExtractStatusLabel(status: EngineeringConfigSourceSnapshot["extractStatus"]): string {
  if (status === "digest_ready") return "Digest ready";
  if (status === "pending") return "Digest 待处理";
  if (status === "not_applicable") return "不适用 digest";
  return status.trim() || "状态待补";
}

function sourceSnapshotDigestStatusPreview(snapshot: EngineeringConfigSourceSnapshot): string {
  const summary = snapshot.sourceDigestStatus?.summary;
  const metrics = [
    typeof summary?.comparableGroupCount === "number" ? `可比组 ${summary.comparableGroupCount}` : null,
    typeof summary?.candidateTrimCount === "number" ? `候选配置列 ${summary.candidateTrimCount}` : null,
    typeof summary?.featureCount === "number" ? `配置项 ${summary.featureCount}` : null,
    typeof summary?.differenceCount === "number" ? `差异 ${summary.differenceCount}` : null,
  ].filter((value): value is string => Boolean(value));
  return [sourceSnapshotExtractStatusLabel(snapshot.extractStatus), ...metrics].join(" · ");
}

function sourceSnapshotContextPreview(snapshot: EngineeringConfigSourceSnapshot): string {
  const context = snapshot.relatedContext;
  return uniquePresent([
    context.brand,
    context.model,
    context.market || context.country,
    context.powertrain,
    context.modelYear ? `MY ${context.modelYear}` : null,
    context.segment,
  ]).join(" · ") || "上下文待补";
}

function sourceSnapshotOwnerPreview(snapshot: EngineeringConfigSourceSnapshot): string {
  return snapshot.createdBy?.trim() ? `上传人 ${snapshot.createdBy.trim()}` : "上传人待补";
}

function sourceSnapshotMatchPreview(snapshot: EngineeringConfigSourceSnapshot): string | null {
  const matches = snapshot.sourceSearchMatches?.map((match) => match.trim()).filter(Boolean).slice(0, 3) ?? [];
  return matches.length > 0 ? `命中 ${matches.join(" / ")}` : null;
}

function countTargetDifferences(data: CompareResponse, baseTrimId: string | null, targetTrimId: string): number {
  if (!baseTrimId) return 0;
  return data.rows.filter((row) => rowDeltasForBase(data, row, baseTrimId).some((delta) => (
    delta.targetTrim.trimId === targetTrimId && delta.deltaType !== "SAME"
  ))).length;
}

function buildTargetTrimDropdownOptions(
  trims: ComparableTrim[],
  baseTrimId: string | null,
  data: CompareResponse | null,
  allTargetLabel = "全部目标 trim",
): SearchDropdownOption[] {
  const targetOptions = trims
    .filter((trim) => trim.trimId !== baseTrimId)
    .map((trim) => {
      const differenceCount = data ? countTargetDifferences(data, baseTrimId, trim.trimId) : 0;
      return {
        value: trim.trimId,
        label: compareTrimLabel(trim),
        meta: [
          trimOriginLabel(trim),
          trimIdentityAnchorLabel(trim),
          trimSourceSnapshotLabel(trim),
          trimSourceCreatedAtLabel(trim),
          `${differenceCount} 项差异`,
        ].filter((value): value is string => Boolean(value)).join(" · "),
      };
    });

  return [
    {
      value: ALL_TARGET_TRIMS_VALUE,
      label: allTargetLabel,
      meta: "汇总所有非基准配置列的当前范围",
    },
    ...targetOptions,
  ];
}

function buildKeywordDropdownOptions(trims: ComparableTrim[], rows: CompareRow[]): SearchDropdownOption[] {
  const byValue = new Map<string, SearchDropdownOption>();
  const addOption = (value: string | null | undefined, label: string | null | undefined, meta: string): void => {
    const normalizedValue = value?.trim();
    const normalizedLabel = label?.trim() || normalizedValue;
    if (!normalizedValue || !normalizedLabel || byValue.has(normalizedValue)) return;
    byValue.set(normalizedValue, { value: normalizedValue, label: normalizedLabel, meta });
  };

  trims.forEach((trim) => {
    const trimName = trim.trimName || trim.fullTrimName || trim.trimId;
    const modelContext = [trim.brand, trim.modelName, trim.market || trim.country].filter(Boolean).join(" · ");
    addOption(trim.materialNo || trim.vehicleCode, trim.materialNo || trim.vehicleCode, `物料号 · ${modelContext || trimName}`);
    addOption(trim.salesVersion, trim.salesVersion, `Sales version · ${trimName}`);
    addOption(trim.identityKey, trim.identityKey, `身份锚点 · ${trimName}`);
    addOption(trimName, trimName, `配置列 · ${modelContext || "当前候选"}`);
    addOption(trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath, trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath, `来源 · ${trimName}`);
  });

  rows.forEach((row) => {
    addOption(row.featureName, row.featureName, `配置项 · ${row.category}`);
    addOption(row.featureCode, row.featureCode, `配置编码 · ${row.category}`);
  });

  return Array.from(byValue.values()).sort((a, b) => (
    a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" })
  ));
}

function mergeTrimOptionPool(previous: VehicleTrimItem[], incoming: VehicleTrimItem[]): VehicleTrimItem[] {
  const byId = new Map<string, VehicleTrimItem>();
  previous.forEach((trim) => byId.set(trim.trimId, trim));
  incoming.forEach((trim) => byId.set(trim.trimId, trim));
  return Array.from(byId.values()).slice(-500);
}

function sourceContextValue(filterValue: string, values: Array<string | null | undefined>): string | null {
  if (filterValue.trim()) return filterValue.trim();
  const present = uniquePresent(values);
  if (present.length === 0) return null;
  return present.slice(0, 3).join(" / ");
}

function sourceContextSingleScopeValue(filterValue: string, values: Array<string | null | undefined>): string | null {
  if (filterValue.trim()) return filterValue.trim();
  const present = uniquePresent(values);
  return present.length === 1 ? present[0] : null;
}

function singleSourceContextValue(filterValue: string, values: Array<string | null | undefined>): string | null {
  if (filterValue.trim()) return filterValue.trim();
  const present = uniquePresent(values);
  return present[0] ?? null;
}

function compactNumber(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 0 }).format(value);
}

function signedCompactNumber(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${compactNumber(value)}`;
}

function percentLabel(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function signedPercentLabel(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${percentLabel(value)}`;
}

function similarityLabel(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "匹配度待补";
  return `${value.toFixed(0)}% match`;
}

function competitorRecommendationRelevanceLabel(
  recommendation: EngineeringConfigCompetitorRecommendation,
  source: CompetitorRecommendationSource | null,
): string {
  const similarity = recommendation.similarityScore;
  const hasSharedDimensions = recommendation.sharedDimensions.length > 0;
  if (source?.analysisMode === "profile" && (!hasSharedDimensions || !similarity || similarity <= 0)) {
    return "AA 排序";
  }
  return similarityLabel(similarity);
}

function competitorMigrationMetricItems(
  recommendation: EngineeringConfigCompetitorRecommendation,
): Array<{ label: string; value: string }> {
  return [
    { label: "dV", value: signedCompactNumber(recommendation.deltaVolume) },
    { label: "目标份额", value: percentLabel(recommendation.shareTarget) },
    { label: "份额变化", value: signedPercentLabel(recommendation.shareChange) },
    { label: "纯份额迁移", value: signedCompactNumber(recommendation.pureShareShift) },
    { label: "估算流向", value: signedCompactNumber(recommendation.estimatedFlow) },
  ].filter((item) => item.value !== "-");
}

function competitorRecommendationSourceLabel(source: CompetitorRecommendationSource | null): string {
  if (!source) return "高级分析来源待补";
  const mode = source.analysisMode ? `模式 ${source.analysisMode}` : null;
  const period = source.targetPeriod && source.basePeriod
    ? `${source.basePeriod} → ${source.targetPeriod}`
    : source.targetPeriod || source.basePeriod || null;
  const scope = typeof source.scopeModelCount === "number" ? `样本 ${source.scopeModelCount} models` : null;
  const advancedScope = [source.advancedAnalysisCountry, source.advancedAnalysisSegment].filter(Boolean).join(" / ");
  return [mode, period, scope, advancedScope ? `AA ${advancedScope}` : null].filter(Boolean).join(" · ") || "高级分析 competitor set";
}

function trimMeta(trim: ComparableTrim): string {
  return [trim.market || trim.country, trim.modelYear, trim.energyType || trim.drivetrain, trim.salesVersion]
    .filter(Boolean)
    .join(" · ") || "市场 / 年款 / sales version 待补";
}

function trimProfileLabel(trim: ComparableTrim): string | null {
  const profile = "profile" in trim ? trim.profile : null;
  const parts = [
    profile?.familyIdentifier ? `Family ${profile.familyIdentifier}` : null,
    profile?.variantVersion ? `Variant ${profile.variantVersion}` : null,
  ].filter((value): value is string => Boolean(value));
  return parts.length > 0 ? parts.join(" · ") : null;
}

function trimPowertrain(trim: ComparableTrim): string {
  return [trim.energyType, trim.drivetrain, trim.engine].filter(Boolean).join(" + ") || "动力信息待补";
}

function directSelectedTrimMeta(trim: ComparableTrim): string {
  return [
    trim.brand || "Brand",
    trim.modelName || "Model",
    trim.market || trim.country || "市场待补",
    trim.modelYear ? `MY ${trim.modelYear}` : "年款待补",
    trimPowertrain(trim),
    trimIdentityAnchorLabel(trim),
    trimSourceLabel(trim),
    trimSourceOwnerLabel(trim),
  ].filter((value): value is string => Boolean(value)).join(" · ");
}

function trimIdentityAnchorLabel(trim: ComparableTrim): string {
  if (trim.materialNo || trim.vehicleCode) return `物料号 ${trim.materialNo || trim.vehicleCode}`;
  if (trim.salesVersion) return `Sales version ${trim.salesVersion}`;
  if (trim.identityKey) return `Identity ${trim.identityKey}`;
  return "品牌 / 车型 / 市场";
}

function sourceContextIdentityAnchor(trim: ComparableTrim): string | null {
  if (trim.materialNo || trim.vehicleCode) return `物料号 ${trim.materialNo || trim.vehicleCode}`;
  if (trim.salesVersion) return `Sales version ${trim.salesVersion}`;
  if (trim.identityKey) return `Identity ${trim.identityKey}`;
  return trim.trimName || trim.fullTrimName || trim.trimId;
}

function sourceContextIdentityAnchorType(trims: ComparableTrim[]): string {
  if (trims.some((trim) => Boolean(trim.materialNo || trim.vehicleCode || trim.hasMaterialNo))) return "material_no";
  if (trims.some((trim) => Boolean(trim.salesVersion || trim.identityKey))) return "sales_version";
  return "brand_model_market";
}

function hasMaterialAnchor(trim: ComparableTrim): boolean {
  return Boolean(trim.materialNo || trim.hasMaterialNo);
}

function trimOriginLabel(trim: ComparableTrim): string {
  if (trim.dataOrigin === "own_catalog" || hasMaterialAnchor(trim)) return "本品";
  if (trim.dataOrigin === "external_or_scraped") return "竞品 / 外部";
  return "身份待确认";
}

function trimOriginClassName(trim: ComparableTrim): string {
  if (trim.dataOrigin === "own_catalog" || hasMaterialAnchor(trim)) return "is-own";
  if (trim.dataOrigin === "external_or_scraped") return "is-external";
  return "is-unknown";
}

function trimMaterialAnchorLabel(trim: ComparableTrim): string {
  return hasMaterialAnchor(trim) ? "物料号" : "无物料号";
}

function TrimIdentityBadges({ trim }: { trim: ComparableTrim }) {
  return (
    <div className="product-config-trim-badges" aria-label={`${trim.trimName || trim.fullTrimName} 身份锚点`}>
      <span className={`product-config-origin-badge ${trimOriginClassName(trim)}`}>{trimOriginLabel(trim)}</span>
      <span className={`product-config-origin-badge ${hasMaterialAnchor(trim) ? "is-material" : "is-no-material"}`}>{trimMaterialAnchorLabel(trim)}</span>
    </div>
  );
}

function trimSourceLabel(trim: ComparableTrim): string {
  return trim.sourceFileName || trim.sourceUploadId || "来源待补";
}

function trimSourceSnapshotLabel(trim: ComparableTrim): string {
  const sourceFileName = trim.sourceFileName?.trim();
  const sourceId = trim.sourceUploadId?.trim();
  if (sourceFileName && sourceId) return `${sourceFileName} · 快照 ${sourceId.slice(-8)}`;
  return sourceFileName || sourceId || "来源待补";
}

function trimSourceCreatedAtLabel(trim: ComparableTrim): string | null {
  const createdAt = trim.sourceCreatedAt?.trim();
  if (!createdAt) return null;
  return `上传 ${createdAt.slice(0, 10)}`;
}

function sourceDigestSearchQueryForTrim(trim: ComparableTrim | null): string {
  if (!trim) return "";
  return uniquePresent([
    trim.brand,
    trim.modelName,
    trim.market || trim.country,
    trim.modelYear,
    trim.energyType,
    trim.drivetrain,
    trim.materialNo || trim.vehicleCode,
    trim.salesVersion,
    trim.identityKey,
    trim.trimName || trim.fullTrimName,
  ]).join(" ");
}

function trimSourceOwnerLabel(trim: ComparableTrim): string | null {
  const sourceCreatedBy = trim.sourceCreatedBy?.trim();
  return sourceCreatedBy ? `来源人 ${sourceCreatedBy}` : null;
}

function selectedConfigPathKey(trim: ComparableTrim): string {
  return [
    trim.sourceUploadId || trimSourceSnapshotLabel(trim),
    trimSourceCreatedAtLabel(trim) || "上传时间待补",
    trim.brand || "品牌待补",
    trim.market || trim.country || "市场待补",
    trim.modelYear || "年款待补",
    trim.modelName || "车型待补",
  ].map(normalizedMatchText).join("|");
}

function selectedConfigPathAnchorLabel(trims: ComparableTrim[]): string {
  const materialCount = trims.filter((trim) => hasMaterialAnchor(trim)).length;
  const salesVersionCount = trims.filter((trim) => Boolean(trim.salesVersion || trim.identityKey)).length;
  const missingMaterialCount = Math.max(trims.length - materialCount, 0);
  const parts = [
    materialCount > 0 ? `物料号 ${materialCount}` : null,
    missingMaterialCount > 0
      ? salesVersionCount > 0
        ? `无物料号 ${missingMaterialCount}，Sales version ${salesVersionCount}`
        : `无物料号 ${missingMaterialCount}`
      : null,
  ].filter((part): part is string => Boolean(part));
  return parts.join(" · ") || "身份锚点待补";
}

function buildSelectedConfigPathGroups(trims: ComparableTrim[]): SelectedConfigPathGroup[] {
  const grouped = new Map<string, ComparableTrim[]>();
  trims.forEach((trim) => {
    const key = selectedConfigPathKey(trim);
    grouped.set(key, [...(grouped.get(key) ?? []), trim]);
  });
  return Array.from(grouped.entries()).map(([key, items]) => {
    const sourceValues = uniquePresent(items.map(trimSourceSnapshotLabel));
    const brandValues = uniquePresent(items.map((trim) => trim.brand));
    const marketValues = uniquePresent(items.map((trim) => trim.market || trim.country));
    const modelYearValues = uniquePresent(items.map((trim) => trim.modelYear));
    const modelValues = uniquePresent(items.map((trim) => trim.modelName));
    const originValues = uniquePresent(items.map(trimOriginLabel));
    const ownerValues = uniquePresent(items.map(trimSourceOwnerLabel));
    const sourceCreatedAtValues = uniquePresent(items.map(trimSourceCreatedAtLabel));
    return {
      key,
      sourceLabel: sourceValues.length > 0 ? compactList(sourceValues) : "来源待补",
      brandLabel: brandValues.length > 0 ? compactList(brandValues) : "品牌待补",
      marketLabel: marketValues.length > 0 ? compactList(marketValues) : "市场待补",
      modelYearLabel: modelYearValues.length > 0 ? `MY ${compactList(modelYearValues)}` : "年款待补",
      modelLabel: modelValues.length > 0 ? compactList(modelValues) : "车型待补",
      originLabel: originValues.length > 0 ? compactList(originValues) : "身份待确认",
      anchorLabel: selectedConfigPathAnchorLabel(items),
      ownerLabel: ownerValues.length > 0 ? compactList(ownerValues) : null,
      sourceCreatedAtLabel: sourceCreatedAtValues.length > 0 ? compactList(sourceCreatedAtValues) : null,
      trimLabels: items.map((trim) => trim.trimName || trim.fullTrimName || trim.trimId),
      trimCount: items.length,
    };
  }).sort((a, b) => {
    const sourceCompare = a.sourceLabel.localeCompare(b.sourceLabel, undefined, { numeric: true, sensitivity: "base" });
    if (sourceCompare !== 0) return sourceCompare;
    const modelCompare = a.modelLabel.localeCompare(b.modelLabel, undefined, { numeric: true, sensitivity: "base" });
    if (modelCompare !== 0) return modelCompare;
    return a.key.localeCompare(b.key);
  });
}

function selectedTrimSecondaryLabel(trim: ComparableTrim): string {
  const trimName = trim.trimName || trim.fullTrimName || trim.trimId;
  const scopedFullName = trim.fullTrimName && trim.fullTrimName !== trimName ? trim.fullTrimName : null;
  const source = trimSourceSnapshotLabel(trim);
  const sourceLabel = source !== "来源待补" ? `来源 ${source}` : null;
  const shouldShowSourceScope = Boolean(scopedFullName) || !hasMaterialAnchor(trim) || trim.dataOrigin === "external_or_scraped";
  const parts = uniquePresent([
    scopedFullName || trimName,
    trim.market || trim.country,
    trim.modelYear ? `MY ${trim.modelYear}` : null,
    shouldShowSourceScope ? sourceLabel : null,
    shouldShowSourceScope ? trimSourceOwnerLabel(trim) : null,
    shouldShowSourceScope ? trimSourceCreatedAtLabel(trim) : null,
  ]);
  return parts.join(" · ") || trimName;
}

function compactList(values: string[]): string {
  const visible = values.slice(0, 3);
  return `${visible.join(" / ")}${values.length > visible.length ? ` / +${values.length - visible.length}` : ""}`;
}

function valueMatchesFilter(values: Array<string | null | undefined>, filterValue: string): boolean {
  const needle = filterValue.trim().toLowerCase();
  if (!needle) return true;
  return values.some((value) => value?.toLowerCase().includes(needle));
}

function normalizedMatchText(value: string | null | undefined): string {
  return (value ?? "").replace(/\s+/g, " ").trim().toLowerCase();
}

function valuesMatchSearchTokens(values: Array<string | null | undefined>, query: string): boolean {
  const tokens = normalizedMatchText(query).split(" ").filter(Boolean);
  if (tokens.length === 0) return true;
  const haystack = values.map(normalizedMatchText).filter(Boolean).join(" ");
  return tokens.every((token) => haystack.includes(token));
}

function textMatchScore(values: Array<string | null | undefined>, query: string, exactScore: number, prefixScore: number, containsScore: number): number {
  const needle = normalizedMatchText(query);
  if (!needle) return 0;
  return values.reduce((score, value) => {
    const haystack = normalizedMatchText(value);
    if (!haystack) return score;
    if (haystack === needle) return Math.max(score, exactScore);
    if (haystack.startsWith(needle)) return Math.max(score, prefixScore);
    if (haystack.includes(needle)) return Math.max(score, containsScore);
    return score;
  }, 0);
}

function sourceSnapshotSearchValues(snapshot: EngineeringConfigSourceSnapshot): string[] {
  const context = snapshot.relatedContext;
  return uniquePresent([
    snapshot.sourceFileName,
    snapshot.sourceFilePath,
    snapshot.createdBy,
    context.brand,
    context.model,
    context.market,
    context.country,
    context.powertrain,
    context.modelYear,
    context.segment,
    context.identityAnchor,
    context.scenario,
    ...(context.salesVersionIds ?? []),
    ...snapshot.contexts.flatMap((item) => [
      item.brand,
      item.model,
      item.market,
      item.country,
      item.powertrain,
      item.modelYear,
      item.segment,
      item.identityAnchor,
      item.scenario,
      item.createdBy,
    ]),
    ...(snapshot.sourceSearchMatches ?? []),
  ]);
}

function sourceSnapshotScore(snapshot: EngineeringConfigSourceSnapshot, filters: TrimFilters, searchQuery: string): number {
  const context = snapshot.relatedContext;
  const contextModels = uniquePresent([
    context.model,
    ...snapshot.contexts.map((item) => item.model),
  ]);
  const sourceValues = uniquePresent([
    snapshot.sourceFileName,
    snapshot.sourceFilePath,
  ]);
  const brandValues = uniquePresent([
    context.brand,
    ...snapshot.contexts.map((item) => item.brand),
  ]);
  const marketValues = uniquePresent([
    context.market,
    context.country,
    ...snapshot.contexts.flatMap((item) => [item.market, item.country]),
  ]);
  const modelYearValues = uniquePresent([
    context.modelYear,
    ...snapshot.contexts.map((item) => item.modelYear),
  ]);
  const segmentValues = uniquePresent([
    context.segment,
    ...snapshot.contexts.map((item) => item.segment),
  ]);
  const powertrainValues = uniquePresent([
    context.powertrain,
    ...snapshot.contexts.map((item) => item.powertrain),
  ]);
  const identityValues = uniquePresent([
    context.identityAnchor,
    context.scenario,
    ...(context.salesVersionIds ?? []),
    ...snapshot.contexts.flatMap((item) => [item.identityAnchor, item.scenario, ...(item.salesVersionIds ?? [])]),
  ]);
  const searchValues = sourceSnapshotSearchValues(snapshot);
  const queryScore = [
    textMatchScore(contextModels, searchQuery, 1200, 950, 680),
    textMatchScore(sourceValues, searchQuery, 1050, 820, 620),
    textMatchScore(brandValues, searchQuery, 760, 620, 420),
    textMatchScore([...marketValues, ...modelYearValues, ...segmentValues, ...powertrainValues], searchQuery, 680, 520, 360),
    textMatchScore(identityValues, searchQuery, 640, 520, 380),
    textMatchScore(snapshot.sourceSearchMatches ?? [], searchQuery, 600, 480, 340),
    textMatchScore(searchValues, searchQuery, 300, 230, 160),
  ].reduce((total, score) => total + score, 0);
  const filterScore = [
    textMatchScore(brandValues, filters.brand, 220, 170, 110),
    textMatchScore(contextModels, filters.model, 260, 190, 120),
    textMatchScore(marketValues, filters.market, 180, 140, 90),
    textMatchScore(modelYearValues, filters.modelYear, 160, 120, 80),
    textMatchScore(segmentValues, filters.segment, 160, 120, 80),
    textMatchScore(powertrainValues, filters.powertrain, 160, 120, 80),
    textMatchScore(sourceValues, filters.source, 180, 140, 90),
    textMatchScore(searchValues, filters.keyword, 140, 100, 70),
  ].reduce((total, score) => total + score, 0);
  const digestReadyScore = snapshot.sourceDigestStatus?.status === "ready" || snapshot.extractStatus === "digest_ready" ? 40 : 0;
  return queryScore + filterScore + digestReadyScore;
}

function sortSourceDigestSnapshots(
  snapshots: EngineeringConfigSourceSnapshot[],
  filters: TrimFilters,
  searchQuery: string,
): EngineeringConfigSourceSnapshot[] {
  return [...snapshots].sort((a, b) => {
    const scoreDifference = sourceSnapshotScore(b, filters, searchQuery) - sourceSnapshotScore(a, filters, searchQuery);
    if (scoreDifference !== 0) return scoreDifference;
    const fileCompare = a.sourceFileName.localeCompare(b.sourceFileName, undefined, { numeric: true, sensitivity: "base" });
    if (fileCompare !== 0) return fileCompare;
    return a.sourceId.localeCompare(b.sourceId, undefined, { numeric: true, sensitivity: "base" });
  });
}

function sourceDigestGroupMatchesFilters(candidate: SourceDigestGroupCandidate, filters: TrimFilters): boolean {
  const { group } = candidate;
  const brands = sourceDigestBrandValues(candidate);
  const models = sourceDigestModelValues(candidate);
  const trimNames = sourceDigestTrimNameValues(candidate);
  const markets = sourceDigestMarketValues(candidate);
  const modelYears = sourceDigestModelYearValues(candidate);
  const segments = sourceDigestSegmentValues(candidate);
  const powertrains = sourceDigestPowertrainValues(candidate);
  const sourceValues = sourceDigestSourceValues(candidate);
  const keywords = [
    ...sourceDigestCandidateSearchValues(candidate),
  ];
  return (
    valueMatchesFilter(brands, filters.brand)
    && valueMatchesFilter(models, filters.model)
    && valueMatchesFilter(trimNames, filters.trim)
    && valueMatchesFilter(markets, filters.market)
    && valueMatchesFilter(modelYears, filters.modelYear)
    && valueMatchesFilter(segments, filters.segment)
    && valueMatchesFilter(powertrains, filters.powertrain)
    && valueMatchesFilter(sourceValues, filters.source)
    && valueMatchesFilter(keywords, filters.keyword)
  );
}

function sourceDigestGroupMatchesSearch(candidate: SourceDigestGroupCandidate, searchQuery: string): boolean {
  return valuesMatchSearchTokens(sourceDigestCandidateFilterValues(candidate), searchQuery);
}

function sourceDigestCandidateScore(candidate: SourceDigestGroupCandidate, filters: TrimFilters, searchQuery: string): number {
  const brandValues = sourceDigestBrandValues(candidate);
  const trimNames = sourceDigestTrimNameValues(candidate);
  const trimAnchors = sourceDigestAnchorValues(candidate);
  const sourceValues = sourceDigestSourceValues(candidate);
  const modelValues = sourceDigestModelValues(candidate);
  const marketValues = sourceDigestMarketValues(candidate);
  const modelYearValues = sourceDigestModelYearValues(candidate);
  const powertrainValues = sourceDigestPowertrainValues(candidate);
  const qualityValues = sourceDigestQualityValues(candidate);
  const searchValues = [
    ...sourceDigestCandidateSearchValues(candidate),
  ];
  const primarySearchScore = [
    textMatchScore(brandValues, searchQuery, 940, 800, 620),
    textMatchScore(modelValues, searchQuery, 1000, 850, 650),
    textMatchScore(sourceValues, searchQuery, 800, 680, 500),
    textMatchScore([...trimNames, ...trimAnchors], searchQuery, 760, 620, 460),
    textMatchScore([...marketValues, ...modelYearValues, ...sourceDigestSegmentValues(candidate), ...powertrainValues], searchQuery, 720, 600, 440),
    textMatchScore(qualityValues, searchQuery, 620, 520, 380),
    textMatchScore(candidate.sourceSearchMatches ?? [], searchQuery, 560, 460, 340),
    textMatchScore(searchValues, searchQuery, 280, 220, 160),
  ].reduce((total, score) => total + score, 0);
  const filterScore = [
    textMatchScore(brandValues, filters.brand, 220, 170, 110),
    textMatchScore(modelValues, filters.model, 240, 180, 120),
    textMatchScore(sourceValues, filters.source, 180, 140, 90),
    textMatchScore(trimNames, filters.trim, 180, 140, 90),
    textMatchScore(marketValues, filters.market, 120, 90, 60),
    textMatchScore(modelYearValues, filters.modelYear, 120, 90, 60),
    textMatchScore(sourceDigestSegmentValues(candidate), filters.segment, 120, 90, 60),
    textMatchScore(powertrainValues, filters.powertrain, 120, 90, 60),
    textMatchScore(searchValues, filters.keyword, 140, 100, 70),
  ].reduce((total, score) => total + score, 0);
  const sourceKindScore = candidate.sourceKind === "local" ? 10 : 0;
  return primarySearchScore + filterScore + sourceKindScore;
}

function sortSourceDigestCandidates(
  candidates: SourceDigestGroupCandidate[],
  filters: TrimFilters,
  searchQuery: string,
): SourceDigestGroupCandidate[] {
  return [...candidates].sort((a, b) => {
    const scoreDifference = sourceDigestCandidateScore(b, filters, searchQuery) - sourceDigestCandidateScore(a, filters, searchQuery);
    if (scoreDifference !== 0) return scoreDifference;
    if (a.sourceKind !== b.sourceKind) return a.sourceKind === "local" ? -1 : 1;
    const sourceCompare = a.sourceFileName.localeCompare(b.sourceFileName, undefined, { numeric: true, sensitivity: "base" });
    if (sourceCompare !== 0) return sourceCompare;
    return sourceDigestCandidateModelLabel(a).localeCompare(sourceDigestCandidateModelLabel(b), undefined, { numeric: true, sensitivity: "base" });
  });
}

function sourceDigestGroupMeta(candidate: SourceDigestGroupCandidate): string {
  const { group, sourceFileName } = candidate;
  return `${group.trimCount} 配置列 · ${group.differenceCount} 差异 · ${sourceFileName} / ${group.sourceSheet}`;
}

function sourceDigestSourceScopeLabel(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library" ? "来源库" : "本地样例";
}

function sourceDigestDropdownScopeLabel(candidate: SourceDigestGroupCandidate): string {
  const markets = sourceDigestMarketValues(candidate);
  const modelYears = sourceDigestModelYearValues(candidate);
  const sourceParts = uniquePresent([candidate.sourceFileName, candidate.group.sourceSheet]);
  const contextParts = [
    markets.length > 0 ? compactList(markets) : null,
    modelYears.length > 0 ? `MY ${compactList(modelYears)}` : null,
    ...sourceParts,
    sourceDigestOwnerLabel(candidate),
  ].filter((part): part is string => Boolean(part));
  return [
    sourceDigestSourceScopeLabel(candidate),
    ...contextParts,
  ].join(" · ");
}

function sourceDigestDropdownCompactScopeLabel(candidate: SourceDigestGroupCandidate): string {
  const markets = sourceDigestMarketValues(candidate);
  const modelYears = sourceDigestModelYearValues(candidate);
  const contextParts = [
    markets.length > 0 ? compactList(markets) : null,
    modelYears.length > 0 ? `MY ${compactList(modelYears)}` : null,
    candidate.sourceFileName,
  ].filter((part): part is string => Boolean(part));
  return [
    sourceDigestSourceScopeLabel(candidate),
    ...contextParts,
  ].join(" · ");
}

function sourceDigestDropdownContextLabel(candidate: SourceDigestGroupCandidate): string {
  return sourceDigestDropdownScopeLabel(candidate).replace(/^(来源库|本地样例) · /, "");
}

function sourceDigestCandidateScopedLabel(candidate: SourceDigestGroupCandidate): string {
  return `${sourceDigestCandidateModelLabel(candidate)} · ${sourceDigestDropdownContextLabel(candidate)}`;
}

function sourceDigestDropdownGroupLabel(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library"
    ? `来源库 Source Digest · ${sourceDigestDropdownCompactScopeLabel(candidate).replace(/^来源库 · /, "")}`
    : `本地 xlsx 样例 · ${sourceDigestDropdownCompactScopeLabel(candidate).replace(/^本地样例 · /, "")}`;
}

function sourceDigestSourceLine(candidate: SourceDigestGroupCandidate): string {
  const contextParts = sourceDigestContextValues(candidate).filter((value) => (
    value !== candidate.sourceContext?.contextType
    && value !== candidate.sourceContext?.scenario
    && value !== candidate.sourceContext?.identityAnchor
  ));
  return [
    `${candidate.sourceFileName} / ${candidate.group.sourceSheet}`,
    contextParts.length > 0 ? `上下文 ${compactList(contextParts)}` : null,
    sourceDigestOwnerLabel(candidate),
  ].filter((part): part is string => Boolean(part)).join(" · ");
}

function sourceDigestTrimPreview(candidate: SourceDigestGroupCandidate, selectedTrimIds?: string[]): string {
  const selected = selectedTrimIds ? new Set(selectedTrimIds) : null;
  return candidate.group.trims
    .filter((trim) => !selected || selected.has(sourceDigestTrimId(trim)))
    .slice(0, 4)
    .map(sourceDigestTrimLabel)
    .join(" / ");
}

function sourceDigestDirectPreviewLabel(candidate: SourceDigestGroupCandidate, selectedTrimIds?: string[]): string {
  const selected = selectedTrimIds ? new Set(selectedTrimIds) : null;
  const labels = candidate.group.trims
    .filter((trim) => !selected || selected.has(sourceDigestTrimId(trim)))
    .map(sourceDigestTrimLabel);
  const uniqueLabels = uniquePresent(labels);
  if (uniqueLabels.length === 0) return `${candidate.group.trimCount} 配置列`;
  if (uniqueLabels.length === 1 && labels.length > 1) return `${uniqueLabels[0]} · ${labels.length} 配置列`;
  return compactList(uniqueLabels);
}

function sourceDigestQualityMeta(candidate: SourceDigestGroupCandidate): string | null {
  const parts: string[] = [];
  const formatLabel = sourceDigestFormatLabel(candidate.sourceFormat || candidate.sourceDigestType);
  if (formatLabel) parts.push(formatLabel);
  const selectedEngine = candidate.ocrEvaluation?.selectedEngine || candidate.ocrEngine;
  if (selectedEngine) parts.push(`OCR ${selectedEngine}`);
  if (candidate.ocrEvaluation?.candidateCount) parts.push(`候选 ${candidate.ocrEvaluation.candidateCount}`);
  if (
    candidate.ocrEvaluation?.candidateCount
    && typeof candidate.ocrEvaluation.comparableCandidateCount === "number"
  ) {
    parts.push(`可比候选 ${candidate.ocrEvaluation.comparableCandidateCount}/${candidate.ocrEvaluation.candidateCount}`);
  }
  if (isOcrSemanticStrategy(candidate.ocrEvaluation?.reason) || isOcrSemanticStrategy(candidate.ocrEvaluation?.strategy)) {
    parts.push("按配置表语义选优");
  }
  return parts.length > 0 ? parts.join(" · ") : null;
}

function sourceDigestTemporaryOcrIdentityMeta(candidate: SourceDigestGroupCandidate): string | null {
  if (!sourceDigestCandidateHasTemporaryOcrIdentity(candidate)) return null;
  const temporaryCount = candidate.group.trims.filter(sourceDigestTrimHasTemporaryOcrIdentity).length;
  const countLabel = temporaryCount > 0 ? `${temporaryCount}/${candidate.group.trimCount} 列` : `${candidate.group.trimCount} 列`;
  return `OCR 临时列身份 ${countLabel}：未识别到真实配置列表头，创建前需补真实车型 / 配置列身份。`;
}

function sourceDigestReviewRows(candidate: SourceDigestGroupCandidate): EngineeringConfigSourceDigestGroup["rows"] {
  return candidate.group.rows.filter((row) => (
    row.reviewNotes?.some((note) => note.trim().length > 0)
  ));
}

function sourceDigestReviewMeta(candidate: SourceDigestGroupCandidate): string | null {
  const reviewRows = sourceDigestReviewRows(candidate);
  const firstNote = reviewRows.find((row) => row.reviewNotes?.some((note) => note.trim().length > 0))
    ?.reviewNotes?.find((note) => note.trim().length > 0)
    ?.trim();
  if (reviewRows.length === 0 || !firstNote) return null;
  return `需核对 ${reviewRows.length} 行 OCR 对齐：${firstNote}`;
}

function sourceDigestReviewRowKey(
  row: EngineeringConfigSourceDigestGroup["rows"][number],
  rowIndex: number,
): string {
  return `${row.featureCode || row.featureName || "review-row"}::${rowIndex}`;
}

function firstSourceDigestReviewRow(group: EngineeringConfigSourceDigestGroup): EngineeringConfigSourceDigestGroup["rows"][number] | null {
  return group.rows.find((row) => (
    row.reviewNotes?.some((note) => note.trim().length > 0)
  )) ?? null;
}

function sourceDigestSelectedReviewRow(
  candidate: SourceDigestGroupCandidate,
  reviewFocuses: SourceDigestReviewFocusMap,
): EngineeringConfigSourceDigestGroup["rows"][number] | null {
  const reviewRows = sourceDigestReviewRows(candidate);
  if (reviewRows.length === 0) return null;
  const focusedRowKey = reviewFocuses[sourceDigestCandidateKey(candidate)];
  if (focusedRowKey) {
    const focusedRow = reviewRows.find((row, rowIndex) => (
      sourceDigestReviewRowKey(row, rowIndex) === focusedRowKey
    ));
    if (focusedRow) return focusedRow;
  }
  return reviewRows[0] ?? null;
}

function sourceDigestSourceTypeLabel(candidate: SourceDigestGroupCandidate): string {
  if (sourceDigestCandidateHasTemporaryOcrIdentity(candidate)) return "OCR 临时列";
  if (candidate.group.sourceKind === "price_list") return "价格单";
  return sourceDigestFormatLabel(candidate.sourceFormat || candidate.sourceDigestType) || "Source Digest";
}

function sourceDigestOcrScoreMetrics(candidate: SourceDigestGroupCandidate): Array<{ label: string; value: string }> {
  const score = candidate.ocrEvaluation?.selectedScore;
  if (!score) return [];
  return [
    { label: "配置项", value: String(score.featureCount ?? score.totalFeatureCount ?? 0) },
    { label: "配置列", value: String(score.candidateTrimCount ?? score.totalCandidateTrimCount ?? 0) },
    { label: "差异", value: String(score.differenceCount ?? score.totalDifferenceCount ?? 0) },
    { label: "表格", value: score.rowCount > 0 && score.columnCount > 0 ? `${score.rowCount}x${score.columnCount}` : "-" },
  ].filter((item) => item.value !== "0" && item.value !== "-");
}

function sourceDigestOcrReasonDetails(candidate: SourceDigestGroupCandidate): string[] {
  const details = candidate.ocrEvaluation?.selectedReasonDetails;
  if (!details || details.length === 0) return [];
  return uniquePresent(details).slice(0, 2);
}

function sourceDigestOcrComparisonText(candidate: SourceDigestGroupCandidate): string | null {
  return engineeringConfigOcrComparisonText(candidate);
}

function sourceDigestOcrQualityText(candidate: SourceDigestGroupCandidate): string | null {
  const selectedEngine = candidate.ocrEvaluation?.selectedEngine || candidate.ocrEngine;
  if (!selectedEngine && !candidate.ocrEvaluation) return null;
  const parts = [
    selectedEngine ? `选用 ${selectedEngine}` : null,
    candidate.ocrEvaluation
      ? `可比候选 ${candidate.ocrEvaluation.comparableCandidateCount}/${candidate.ocrEvaluation.candidateCount}`
      : null,
    isOcrSemanticStrategy(candidate.ocrEvaluation?.reason) || isOcrSemanticStrategy(candidate.ocrEvaluation?.strategy)
      ? "按配置表语义评分"
      : null,
  ].filter((part): part is string => Boolean(part));
  return parts.length > 0 ? parts.join(" · ") : null;
}

function sourceDigestSelectedOcrPreview(candidate: SourceDigestGroupCandidate): string | null {
  const candidates = candidate.ocrEngineCandidates ?? [];
  if (candidates.length === 0) return null;
  const selectedEngine = candidate.ocrEvaluation?.selectedEngine || candidate.ocrEngine;
  const selectedCandidate = candidates.find((item) => item.selected)
    ?? candidates.find((item) => selectedEngine && item.engine === selectedEngine)
    ?? null;
  if (!selectedCandidate?.textPreview) return null;
  return `识别原文${selectedCandidate.lineCount ? ` ${selectedCandidate.lineCount} 行` : ""}：${selectedCandidate.textPreview}`;
}

function sourceDigestPrimaryActionLabel(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library" ? "生成配置列" : "预览配置列";
}

function sourceDigestTrimSelectionActionLabel(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library" ? "暂选配置列" : "暂选预览列";
}

function sourceDigestPendingActionText(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library" ? "生成可编辑配置列" : "预览配置列";
}

function sourceDigestDirectSelectionVerb(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library" ? "生成" : "预览";
}

function sourceDigestDirectSelectedMeta(candidate: SourceDigestGroupCandidate, selectedCount: number): string | null {
  if (selectedCount <= 0) return null;
  const suffix = selectedCount >= 2 ? `，可${sourceDigestDirectSelectionVerb(candidate)}` : "";
  return `同组已暂选 ${selectedCount}/4${suffix}`;
}

function sourceDigestDirectActionMeta(candidate: SourceDigestGroupCandidate): string | null {
  return candidate.sourceKind === "library" ? "可直接生成在线表，生成后加入对比" : null;
}

function sourceDigestDirectFocusMeta(candidate: SourceDigestGroupCandidate): string {
  return candidate.sourceKind === "library" ? "Source Digest 待生成范围" : "本地预览范围";
}

function sourceDigestDirectStatsMeta(candidate: SourceDigestGroupCandidate): string {
  return `${candidate.group.trimCount} 配置列 · ${candidate.group.differenceCount} 差异`;
}

function sourceDigestDirectSourceModelPreview(candidates: SourceDigestGroupCandidate[]): string | null {
  const modelLabels = uniquePresent(candidates.map(sourceDigestCandidateModelLabel));
  if (modelLabels.length === 0) return null;
  return `Model ${compactList(modelLabels)}`;
}

function sourceDigestDirectSourceTrimPreview(candidates: SourceDigestGroupCandidate[]): string | null {
  const trimLabels = uniquePresent(candidates.flatMap((candidate) => (
    candidate.group.trims.map(sourceDigestTrimLabel)
  )));
  if (trimLabels.length === 0) return null;
  return `配置列 ${compactList(trimLabels)}`;
}

function sourceDigestDirectAggregateSearchText(candidates: SourceDigestGroupCandidate[]): string {
  return [
    ...candidates.map((candidate) => sourceDigestDirectSearchText(candidate)),
    ...candidates.flatMap(sourceDigestGroupModelValues),
    ...candidates.flatMap((candidate) => candidate.group.trims.map(sourceDigestTrimLabel)),
    ...candidates.map((candidate) => candidate.sourceFileName),
    ...candidates.map((candidate) => candidate.group.sourceSheet),
    ...candidates.flatMap(sourceDigestMarketValues),
    ...candidates.flatMap(sourceDigestModelYearValues),
  ].filter((part): part is string => Boolean(part)).join(" · ");
}

function sourceDigestDirectTrimAnchorMeta(trim: EngineeringConfigSourceDigestGroup["trims"][number]): string {
  if (sourceDigestTrimHasTemporaryOcrIdentity(trim)) {
    return trim.identityNote?.trim() || "临时 OCR 列 · 待补真实配置列身份";
  }
  const materialNo = trim.materialNo || trim.profile?.materialNo;
  const salesVersion = trim.salesVersion || trim.profile?.configurationVersion;
  return [
    materialNo ? `物料号 ${materialNo}` : salesVersion ? "无物料号" : null,
    salesVersion ? `Sales version ${salesVersion}` : null,
  ].filter((part): part is string => Boolean(part)).join(" · ");
}

function sourceDigestAnchorQuery(values: Array<string | null | undefined>): string {
  return uniquePresent(values)
    .filter((value) => !value.endsWith("待补"))
    .join(" ");
}

function sourceDigestDirectPath(
  candidate: SourceDigestGroupCandidate,
  trim?: EngineeringConfigSourceDigestGroup["trims"][number],
): string {
  const brands = sourceDigestBrandValues(candidate);
  const markets = sourceDigestMarketValues(candidate);
  const modelYears = sourceDigestModelYearValues(candidate);
  const powertrains = sourceDigestPowertrainValues(candidate);
  const modelLabel = sourceDigestCandidateModelLabel(candidate);
  const sourceSheet = candidate.group.sourceSheet && candidate.group.sourceSheet !== modelLabel
    ? candidate.group.sourceSheet
    : null;
  return dropdownPathParts([
    `${sourceDigestSourceScopeLabel(candidate)}来源`,
    brands.length > 0 ? `品牌 ${compactList(brands)}` : "品牌待补",
    markets.length > 0 ? `市场 ${compactList(markets)}` : null,
    modelYears.length > 0 ? `MY ${compactList(modelYears)}` : null,
    powertrains.length > 0 ? `动力 ${compactList(powertrains)}` : null,
    candidate.sourceFileName,
    sourceSheet,
    modelLabel,
    trim ? `配置列 ${sourceDigestTrimLabel(trim)}` : "整组配置列",
  ]);
}

function sourceDigestDirectSearchText(
  candidate: SourceDigestGroupCandidate,
  trim?: EngineeringConfigSourceDigestGroup["trims"][number],
): string {
  const trimValues = trim
    ? [
        sourceDigestTrimLabel(trim),
        trim.materialNo,
        trim.profile?.materialNo,
        trim.salesVersion,
        trim.profile?.configurationVersion,
        trim.modelName,
      ]
    : candidate.group.trims.flatMap((candidateTrim) => [
        sourceDigestTrimLabel(candidateTrim),
        candidateTrim.materialNo,
        candidateTrim.profile?.materialNo,
        candidateTrim.salesVersion,
        candidateTrim.profile?.configurationVersion,
        candidateTrim.modelName,
      ]);
  return [
    sourceDigestDropdownScopeLabel(candidate),
    sourceDigestDropdownCompactScopeLabel(candidate),
    sourceDigestDirectActionMeta(candidate),
    sourceDigestDirectStatsMeta(candidate),
    sourceDigestIdentityMeta(candidate),
    sourceDigestQualityMeta(candidate),
    sourceDigestReviewMeta(candidate),
    sourceDigestGroupMeta(candidate),
    sourceDigestSourceLine(candidate),
    candidate.group.title,
    sourceDigestCandidateModelLabel(candidate),
    candidate.group.modelName,
    candidate.group.sourceSheet,
    candidate.sourceFileName,
    candidate.createdBy,
    ...(candidate.sourceSearchMatches ?? []),
    ...sourceDigestBrandValues(candidate),
    ...sourceDigestMarketValues(candidate),
    ...sourceDigestModelYearValues(candidate),
    ...sourceDigestPowertrainValues(candidate),
    ...sourceDigestSegmentValues(candidate),
    ...trimValues,
  ].filter((part): part is string => Boolean(part)).join(" · ");
}

function sourceDigestIdentityMeta(candidate: SourceDigestGroupCandidate): string {
  const { group } = candidate;
  const markets = sourceDigestMarketValues(candidate);
  const modelYears = sourceDigestModelYearValues(candidate);
  const segments = sourceDigestSegmentValues(candidate);
  const powertrains = sourceDigestPowertrainValues(candidate);
  const temporaryOcrIdentity = sourceDigestTemporaryOcrIdentityMeta(candidate);
  const materialCount = group.trims.filter((trim) => Boolean(trim.materialNo || trim.hasMaterialNo || trim.profile?.materialNo)).length;
  const noMaterialCount = Math.max(group.trimCount - materialCount, 0);
  const salesVersionCount = group.trims.filter((trim) => Boolean(trim.salesVersion || trim.profile?.configurationVersion)).length;
  const identityParts: string[] = [];
  if (materialCount === group.trimCount && group.trimCount > 0) {
    identityParts.push("物料号齐全");
  } else if (materialCount > 0) {
    identityParts.push(`物料号 ${materialCount}/${group.trimCount}`);
  }
  if (noMaterialCount > 0) {
    identityParts.push(salesVersionCount > 0 ? `无物料号 ${noMaterialCount}，Sales version ${salesVersionCount}` : `无物料号 ${noMaterialCount}`);
  }
  return [
    markets.length > 0 ? compactList(markets) : "市场待补",
    modelYears.length > 0 ? `MY ${compactList(modelYears)}` : "年款待补",
    segments.length > 0 ? `Segment ${compactList(segments)}` : null,
    powertrains.length > 0 ? `动力 ${compactList(powertrains)}` : "动力待补",
    group.sourceKind === "price_list" ? "价格单" : null,
    temporaryOcrIdentity ? "临时 OCR 列身份待补" : null,
    ...identityParts,
  ].filter((part): part is string => Boolean(part)).join(" · ");
}

function sourceDigestDirectOptionValue(candidate: SourceDigestGroupCandidate): string {
  return `${SOURCE_DIGEST_DIRECT_OPTION_PREFIX}${sourceDigestCandidateKey(candidate)}`;
}

function sourceDigestDirectOptionKey(value: string): string | null {
  return value.startsWith(SOURCE_DIGEST_DIRECT_OPTION_PREFIX)
    ? value.slice(SOURCE_DIGEST_DIRECT_OPTION_PREFIX.length)
    : null;
}

function sourceDigestSourceFocusKey(candidate: SourceDigestGroupCandidate): string {
  return `${candidate.sourceKind}:${candidate.sourceId ?? candidate.sourceFileName}`;
}

function sourceDigestDirectSourceOptionValue(sourceKey: string): string {
  return `${SOURCE_DIGEST_DIRECT_SOURCE_OPTION_PREFIX}${encodeURIComponent(sourceKey)}`;
}

function sourceDigestDirectSourceOptionKey(value: string): string | null {
  if (!value.startsWith(SOURCE_DIGEST_DIRECT_SOURCE_OPTION_PREFIX)) return null;
  try {
    return decodeURIComponent(value.slice(SOURCE_DIGEST_DIRECT_SOURCE_OPTION_PREFIX.length));
  } catch {
    return null;
  }
}

function sourceDigestModelFocusKey(candidate: SourceDigestGroupCandidate): string {
  const modelKey = normalizedMatchText(sourceDigestCandidateModelLabel(candidate) || candidate.group.title || candidate.group.groupId);
  return `${sourceDigestSourceFocusKey(candidate)}:${modelKey || candidate.group.groupId}`;
}

function sourceDigestCrossModelFocusKey(candidate: SourceDigestGroupCandidate): string {
  const modelKey = normalizedMatchText(sourceDigestCandidateModelLabel(candidate) || candidate.group.title || candidate.group.groupId);
  return modelKey || candidate.group.groupId;
}

function sourceDigestDirectModelOptionValue(modelKey: string): string {
  return `${SOURCE_DIGEST_DIRECT_MODEL_OPTION_PREFIX}${encodeURIComponent(modelKey)}`;
}

function sourceDigestDirectModelOptionKey(value: string): string | null {
  if (!value.startsWith(SOURCE_DIGEST_DIRECT_MODEL_OPTION_PREFIX)) return null;
  try {
    return decodeURIComponent(value.slice(SOURCE_DIGEST_DIRECT_MODEL_OPTION_PREFIX.length));
  } catch {
    return null;
  }
}

function sourceDigestDirectCrossModelOptionValue(modelKey: string): string {
  return `${SOURCE_DIGEST_DIRECT_CROSS_MODEL_OPTION_PREFIX}${encodeURIComponent(modelKey)}`;
}

function sourceDigestDirectCrossModelOptionKey(value: string): string | null {
  if (!value.startsWith(SOURCE_DIGEST_DIRECT_CROSS_MODEL_OPTION_PREFIX)) return null;
  try {
    return decodeURIComponent(value.slice(SOURCE_DIGEST_DIRECT_CROSS_MODEL_OPTION_PREFIX.length));
  } catch {
    return null;
  }
}

function sourceDigestDirectTrimOptionValue(candidate: SourceDigestGroupCandidate, trimId: string): string {
  return `${SOURCE_DIGEST_DIRECT_TRIM_OPTION_PREFIX}${encodeURIComponent(sourceDigestCandidateKey(candidate))}:${encodeURIComponent(trimId)}`;
}

function sourceDigestDirectTrimOptionSelection(value: string): { candidateKey: string; trimId: string } | null {
  if (!value.startsWith(SOURCE_DIGEST_DIRECT_TRIM_OPTION_PREFIX)) return null;
  const payload = value.slice(SOURCE_DIGEST_DIRECT_TRIM_OPTION_PREFIX.length);
  const separatorIndex = payload.indexOf(":");
  if (separatorIndex <= 0 || separatorIndex >= payload.length - 1) return null;
  try {
    return {
      candidateKey: decodeURIComponent(payload.slice(0, separatorIndex)),
      trimId: decodeURIComponent(payload.slice(separatorIndex + 1)),
    };
  } catch {
    return null;
  }
}

function buildSourceDigestDirectDropdownOptions(
  candidates: SourceDigestGroupCandidate[],
  selectionMap: SourceDigestTrimSelectionMap,
  directSelectionMap: SourceDigestTrimSelectionMap,
): SearchDropdownOption[] {
  const options: SearchDropdownOption[] = [];
  const sourceGroups = new Map<string, SourceDigestGroupCandidate[]>();
  const modelGroups = new Map<string, SourceDigestGroupCandidate[]>();
  const crossSourceModelGroups = new Map<string, SourceDigestGroupCandidate[]>();

  candidates.forEach((candidate) => {
    const sourceKey = sourceDigestSourceFocusKey(candidate);
    sourceGroups.set(sourceKey, [...(sourceGroups.get(sourceKey) ?? []), candidate]);
    const modelKey = sourceDigestModelFocusKey(candidate);
    modelGroups.set(modelKey, [...(modelGroups.get(modelKey) ?? []), candidate]);
    const crossModelKey = sourceDigestCrossModelFocusKey(candidate);
    crossSourceModelGroups.set(crossModelKey, [...(crossSourceModelGroups.get(crossModelKey) ?? []), candidate]);
  });

  Array.from(sourceGroups.entries()).forEach(([sourceKey, sourceCandidates]) => {
    const firstCandidate = sourceCandidates[0];
    if (!firstCandidate) return;
    const coverage = sourceDigestCandidateCoverage(sourceCandidates);
    const ownerLabels = uniquePresent(sourceCandidates.map(sourceDigestOwnerLabel));
    const sourceScopeLabel = sourceDigestSourceScopeLabel(firstCandidate);
    const modelPreview = sourceDigestDirectSourceModelPreview(sourceCandidates);
    const trimPreview = sourceDigestDirectSourceTrimPreview(sourceCandidates);
    const optionGroupRank = firstCandidate.sourceKind === "library"
      ? SOURCE_DIGEST_LIBRARY_SOURCE_FOCUS_DROPDOWN_GROUP_RANK
      : LOCAL_DIGEST_SOURCE_FOCUS_DROPDOWN_GROUP_RANK;
    options.push({
      value: sourceDigestDirectSourceOptionValue(sourceKey),
      label: `聚焦来源 · ${firstCandidate.sourceFileName}`,
      badge: "来源",
      badgeTone: "source",
      path: dropdownPathParts([
        `${sourceScopeLabel} 来源`,
        firstCandidate.sourceFileName,
      ]),
      group: `${sourceScopeLabel} 来源文件 · 先聚焦来源 / 车型`,
      groupRank: optionGroupRank,
      matchRankBoost: 12000,
      meta: [
        sourceDigestDirectFocusMeta(firstCandidate),
        `${coverage.modelCount} 车型`,
        `${coverage.trimCount} 可比配置列`,
        `${coverage.differenceCount} 差异`,
        modelPreview,
        trimPreview,
        ownerLabels.length > 0 ? compactList(ownerLabels) : null,
      ].filter((part): part is string => Boolean(part)).join(" · "),
      searchText: sourceDigestDirectAggregateSearchText(sourceCandidates),
    });
  });

  Array.from(modelGroups.entries()).forEach(([modelKey, modelCandidates]) => {
    const firstCandidate = modelCandidates[0];
    if (!firstCandidate) return;
    const coverage = sourceDigestCandidateCoverage(modelCandidates);
    const crossModelCandidates = crossSourceModelGroups.get(sourceDigestCrossModelFocusKey(firstCandidate)) ?? [];
    const crossModelCoverage = sourceDigestCandidateCoverage(crossModelCandidates);
    const needsSourceLabel = crossModelCoverage.sourceCount > 1;
    const modelLabel = sourceDigestCandidateModelLabel(firstCandidate);
    const markets = uniquePresent(modelCandidates.flatMap(sourceDigestMarketValues));
    const modelYears = uniquePresent(modelCandidates.flatMap(sourceDigestModelYearValues));
    const brands = uniquePresent(modelCandidates.flatMap(sourceDigestBrandValues));
    const sourceScopeLabel = sourceDigestSourceScopeLabel(firstCandidate);
    const trimPreview = sourceDigestDirectSourceTrimPreview(modelCandidates);
    const optionGroupRank = firstCandidate.sourceKind === "library"
      ? SOURCE_DIGEST_LIBRARY_MODEL_FOCUS_DROPDOWN_GROUP_RANK
      : LOCAL_DIGEST_MODEL_FOCUS_DROPDOWN_GROUP_RANK;
    options.push({
      value: sourceDigestDirectModelOptionValue(modelKey),
      label: needsSourceLabel
        ? `聚焦车型 · ${modelLabel} · ${firstCandidate.sourceFileName}`
        : `聚焦车型 · ${modelLabel}`,
      badge: "车型",
      badgeTone: "muted",
      path: dropdownPathParts([
        `${sourceScopeLabel} 车型`,
        firstCandidate.sourceFileName,
        modelLabel,
      ]),
      group: `${sourceScopeLabel} 来源文件 · 先聚焦来源 / 车型`,
      groupRank: optionGroupRank,
      matchRankBoost: 8000,
      meta: [
        sourceDigestDirectFocusMeta(firstCandidate),
        `${coverage.trimCount} 可比配置列`,
        `${coverage.differenceCount} 差异`,
        trimPreview,
        brands.length > 0 ? `Brand ${compactList(brands)}` : null,
        markets.length > 0 ? `Market ${compactList(markets)}` : null,
        modelYears.length > 0 ? `MY ${compactList(modelYears)}` : null,
      ].filter((part): part is string => Boolean(part)).join(" · "),
      searchText: sourceDigestDirectAggregateSearchText(modelCandidates),
    });
  });

  Array.from(crossSourceModelGroups.entries()).forEach(([modelKey, modelCandidates]) => {
    const firstCandidate = modelCandidates[0];
    if (!firstCandidate) return;
    const coverage = sourceDigestCandidateCoverage(modelCandidates);
    if (coverage.sourceCount < 2) return;
    const modelLabel = sourceDigestCandidateModelLabel(firstCandidate);
    const sourceLabels = uniquePresent(modelCandidates.map((candidate) => candidate.sourceFileName));
    const trimPreview = sourceDigestDirectSourceTrimPreview(modelCandidates);
    const crossSourceReviewMeta = sourceDigestCrossSourceReviewMeta(modelCandidates);
    options.push({
      value: sourceDigestDirectCrossModelOptionValue(modelKey),
      label: `聚焦同名车型 · ${modelLabel}`,
      badge: "同名车型",
      badgeTone: "source",
      path: dropdownPathParts([
        "跨来源车型",
        modelLabel,
      ]),
      group: "跨来源配置组 · 先聚焦车型",
      groupRank: SOURCE_DIGEST_LIBRARY_MODEL_FOCUS_DROPDOWN_GROUP_RANK - 1,
      matchRankBoost: 10000,
      meta: [
        sourceDigestDirectFocusMeta(firstCandidate),
        crossSourceReviewMeta,
        `${coverage.sourceCount} 来源`,
        `${coverage.trimCount} 配置列`,
        `${coverage.differenceCount} 差异`,
        trimPreview,
        sourceLabels.length > 0 ? compactList(sourceLabels) : null,
      ].filter((part): part is string => Boolean(part)).join(" · "),
      searchText: sourceDigestDirectAggregateSearchText(modelCandidates),
    });
  });

  candidates.forEach((candidate) => {
    const candidateKey = sourceDigestCandidateKey(candidate);
    const selectedTrimIds = selectedSourceDigestTrimIds(candidate, selectionMap);
    const directSelectedTrimIds = directSelectionMap[candidateKey] ?? [];
    const brands = sourceDigestBrandValues(candidate);
    const brandLabel = brands.length > 0 ? compactList(brands) : "品牌待补";
    const modelLabel = sourceDigestCandidateModelLabel(candidate);
    const qualityMeta = sourceDigestQualityMeta(candidate);
    const temporaryOcrIdentityMeta = sourceDigestTemporaryOcrIdentityMeta(candidate);
    const dropdownScopeLabel = sourceDigestDropdownScopeLabel(candidate);
    const previewLabel = sourceDigestDirectPreviewLabel(candidate, selectedTrimIds);
    const optionGroup = sourceDigestDropdownGroupLabel(candidate);
    const optionGroupRank = candidate.sourceKind === "library"
      ? SOURCE_DIGEST_LIBRARY_DROPDOWN_GROUP_RANK
      : LOCAL_DIGEST_DROPDOWN_GROUP_RANK;
    options.push({
      value: sourceDigestDirectOptionValue(candidate),
      label: `${sourceDigestPrimaryActionLabel(candidate)} · ${modelLabel} · ${previewLabel}`,
      badge: candidate.sourceKind === "library" ? "配置组" : "预览组",
      badgeTone: candidate.sourceKind === "library" ? "source" : "local",
      path: sourceDigestDirectPath(candidate),
      group: optionGroup,
      groupRank: optionGroupRank,
      meta: [
        sourceDigestDirectActionMeta(candidate),
        dropdownScopeLabel,
        `Brand ${brandLabel}`,
        sourceDigestDirectStatsMeta(candidate),
        temporaryOcrIdentityMeta,
        qualityMeta,
      ].filter((part): part is string => Boolean(part)).join(" · "),
      searchText: sourceDigestDirectSearchText(candidate),
    });
    candidate.group.trims.forEach((trim) => {
      const trimId = sourceDigestTrimId(trim);
      if (!trimId) return;
      const isDirectSelected = directSelectedTrimIds.includes(trimId);
      options.push({
        value: sourceDigestDirectTrimOptionValue(candidate, trimId),
        label: `${sourceDigestTrimSelectionActionLabel(candidate)} · ${modelLabel} · ${sourceDigestTrimLabel(trim)}`,
        badge: candidate.sourceKind === "library" ? (isDirectSelected ? "已暂选" : "配置列") : "预览列",
        badgeTone: candidate.sourceKind === "library" ? "pending" : "local",
        path: sourceDigestDirectPath(candidate, trim),
        group: optionGroup,
        groupRank: optionGroupRank,
        keepOpenOnSelect: true,
        preserveQueryOnSelect: true,
        meta: [
          sourceDigestDirectActionMeta(candidate),
          dropdownScopeLabel,
          `Brand ${brandLabel}`,
          isDirectSelected ? `已暂选，可${sourceDigestDirectSelectionVerb(candidate)}` : null,
          sourceDigestDirectSelectedMeta(candidate, directSelectedTrimIds.length),
          sourceDigestDirectTrimAnchorMeta(trim),
          temporaryOcrIdentityMeta,
          qualityMeta,
        ].filter((part): part is string => Boolean(part)).join(" · "),
        searchText: sourceDigestDirectSearchText(candidate, trim),
      });
    });
  });
  return sortDropdownOptions(options);
}

function sourceDigestLibraryPathItems(
  candidate: SourceDigestGroupCandidate,
  options?: { includeSourceSheetInSourceValue?: boolean },
): SourceDigestLibraryPathItem[] {
  const { group } = candidate;
  const brands = sourceDigestBrandValues(candidate);
  const modelNames = sourceDigestGroupModelValues(candidate);
  const markets = sourceDigestMarketValues(candidate);
  const modelYears = sourceDigestModelYearValues(candidate);
  const segments = sourceDigestSegmentValues(candidate);
  const sourceValues = uniquePresent(options?.includeSourceSheetInSourceValue
    ? [candidate.sourceFileName, group.sourceSheet]
    : [candidate.sourceFileName]);
  return [
    { key: "brand", label: "品牌", value: brands.length > 0 ? compactList(brands) : "品牌待补", query: sourceDigestAnchorQuery(brands) },
    { key: "model", label: "车型", value: modelNames.length > 0 ? compactList(modelNames) : "车型待补", query: sourceDigestAnchorQuery(modelNames) },
    { key: "market", label: "市场", value: markets.length > 0 ? compactList(markets) : "市场待补", query: sourceDigestAnchorQuery(markets) },
    { key: "model-year", label: "年款", value: modelYears.length > 0 ? compactList(modelYears) : "年款待补", query: sourceDigestAnchorQuery(modelYears) },
    { key: "segment", label: "级别", value: segments.length > 0 ? compactList(segments) : "级别待补", query: sourceDigestAnchorQuery(segments) },
    {
      key: "source",
      label: "来源",
      value: sourceValues.length > 0 ? compactList(sourceValues) : "来源待补",
      query: sourceDigestAnchorQuery(sourceValues),
      sourceId: candidate.sourceId,
    },
  ];
}

function sourceDigestBrowsePathStages(group: SourceDigestBrowseGroup): SourceDigestPathStageItem[] {
  const trimLabels = uniquePresent(group.candidates.flatMap((candidate) => candidate.group.trims.map(sourceDigestTrimLabel)));
  const trimValue = trimLabels.length > 0 ? compactList(trimLabels) : `${group.coverage.trimCount} 可比配置列`;
  return [
    {
      key: "source",
      label: "来源",
      value: group.sourceFileName,
      meta: [group.sourceScopeLabel, group.ownerLabel].filter((value): value is string => Boolean(value)).join(" · ") || "来源范围待补",
    },
    {
      key: "model",
      label: "车型",
      value: group.modelLabel,
      meta: `${group.coverage.modelCount} 车型 · ${group.marketLabel}`,
    },
    {
      key: "config-columns",
      label: "配置列",
      value: trimValue,
      meta: `${group.coverage.trimCount} 可比配置列 · ${group.coverage.differenceCount} 差异`,
    },
  ];
}

function sourceDigestDraftOcrTransparency(
  result: EngineeringConfigDigestDraftResult,
  candidate?: SourceDigestGroupCandidate,
): SourceDigestDraftSuccessSummary["ocrTransparency"] {
  const evaluation = result.ocrEvaluation ?? candidate?.ocrEvaluation ?? null;
  const selectedScore = evaluation?.selectedScore ?? null;
  const selectedEngine = evaluation?.selectedEngine || result.ocrEngine || candidate?.ocrEngine || null;
  const ocrEngineCandidates = result.ocrEngineCandidates ?? candidate?.ocrEngineCandidates;
  const candidateCount = evaluation?.candidateCount ?? ocrEngineCandidates?.length ?? 0;
  const comparableCandidateCount = evaluation?.comparableCandidateCount;
  if (!selectedEngine && !evaluation && candidateCount === 0) return null;
  const comparison = engineeringConfigOcrComparisonText({
    ocrEngine: result.ocrEngine || candidate?.ocrEngine,
    ocrEngineCandidates,
    ocrEvaluation: evaluation,
  });

  const parts = [
    selectedEngine ? `OCR 采用 ${selectedEngine}` : null,
    candidateCount > 0 ? `候选 ${candidateCount}` : null,
    candidateCount > 0 && typeof comparableCandidateCount === "number"
      ? `可比候选 ${comparableCandidateCount}/${candidateCount}`
      : null,
    isOcrSemanticStrategy(evaluation?.reason) || isOcrSemanticStrategy(evaluation?.strategy) ? "按配置表语义选优" : null,
    selectedScore && selectedScore.rowCount > 0 && selectedScore.columnCount > 0
      ? `选中表格 ${selectedScore.rowCount} x ${selectedScore.columnCount}`
      : null,
    selectedScore?.featureCount ? `配置项 ${selectedScore.featureCount}` : null,
  ].filter((part): part is string => Boolean(part));

  return {
    meta: parts.length > 0 ? parts.join(" · ") : "OCR 来源已转成正式配置列",
    comparison,
    reviewNote: "OCR 来源已转为正式配置列；引用卖点前仍建议点开 evidence 核对原 PDF / 图片。",
  };
}

function sourceDigestDraftFeatureCatalogMatch(
  result: EngineeringConfigDigestDraftResult,
): SourceDigestDraftSuccessSummary["featureCatalogMatch"] {
  const hasFeatureCatalogAudit = result.aliasMatchedFeatureCount !== undefined
    || result.semanticAliasMatchedFeatureCount !== undefined
    || result.featureMatchReasonCounts !== undefined
    || result.featureMatchSamples !== undefined;
  if (!hasFeatureCatalogAudit) return null;

  const aliasMatchedCount = result.aliasMatchedFeatureCount ?? 0;
  const semanticAliasMatchedCount = result.semanticAliasMatchedFeatureCount ?? 0;
  const reusedFeatureCount = result.reusedFeatureCount ?? 0;
  const createdFeatureCount = result.createdFeatureCount ?? 0;
  const samples = (result.featureMatchSamples ?? []).slice(0, 3).map((sample) => (
    `${sample.sourceFeatureName} -> ${sample.matchedFeatureName}`
  ));
  if (aliasMatchedCount > 0) {
    return {
      meta: `FeatureCatalog 归并 ${reusedFeatureCount} 项；其中 alias/语义别名命中 ${aliasMatchedCount} 项${semanticAliasMatchedCount > 0 ? `，语义别名 ${semanticAliasMatchedCount} 项` : ""}。`,
      samples,
    };
  }
  if (createdFeatureCount > 0 || reusedFeatureCount > 0) {
    return {
      meta: `FeatureCatalog 复用 ${reusedFeatureCount} 项，新建 ${createdFeatureCount} 项；未返回逐行 alias 命中样例。`,
      samples: [],
    };
  }
  return null;
}

function sourceDigestDraftCompareHeadline(comparePlacement: SourceDigestDraftComparePlacement): string {
  if (comparePlacement.omittedFromCurrentCompareCount === 0) return "新配置列已加入当前对比表";
  if (comparePlacement.visibleCreatedCompareTrimCount > 0) return "新配置列已部分加入当前对比表";
  return "新配置列已建入库，当前对比待加入";
}

function sourceDigestDraftCompareActionLabel(comparePlacement: SourceDigestDraftComparePlacement): string {
  if (comparePlacement.omittedFromCurrentCompareCount > 0 && comparePlacement.visibleCreatedCompareTrimCount > 0) {
    return `已加入 ${comparePlacement.visibleCreatedCompareTrimCount}/${comparePlacement.createdCompareTrimCount} 列，${comparePlacement.omittedFromCurrentCompareCount} 列因最多 4 列暂未显示`;
  }
  if (comparePlacement.omittedFromCurrentCompareCount > 0) {
    return `${comparePlacement.createdCompareTrimCount} 列已建入库，当前对比已满暂未显示`;
  }
  if (comparePlacement.appendedToCurrentCompare) return "已追加到当前对比";
  return "已成为当前对比";
}

function sourceDigestDraftPlacementFeedback(comparePlacement: SourceDigestDraftComparePlacement): string | null {
  if (comparePlacement.omittedFromCurrentCompareCount > 0 && comparePlacement.visibleCreatedCompareTrimCount > 0) {
    return `已加入当前对比 ${comparePlacement.visibleCreatedCompareTrimCount}/${comparePlacement.createdCompareTrimCount} 列；${comparePlacement.omittedFromCurrentCompareCount} 列已建入库但因最多 4 列暂未显示。`;
  }
  if (comparePlacement.omittedFromCurrentCompareCount > 0) {
    return `当前对比已满，${comparePlacement.createdCompareTrimCount} 列已建入库但暂未显示；请先移除配置列后再从下拉加入。`;
  }
  if (comparePlacement.appendedToCurrentCompare) return "已追加到当前对比。";
  return null;
}

function sourceDigestDraftSuccessSummary(
  feedback: string,
  result: EngineeringConfigDigestDraftResult,
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[],
  comparePlacement: SourceDigestDraftComparePlacement,
  candidate?: SourceDigestGroupCandidate,
  sourceSnapshot?: EngineeringConfigSourceSnapshot | null,
): SourceDigestDraftSuccessSummary {
  const selectedTrimSet = new Set(selectedTrimIds);
  const selectedTrims = group.trims.filter((trim) => selectedTrimSet.has(sourceDigestTrimId(trim)));
  const scopedTrims = selectedTrims.length > 0 ? selectedTrims : group.trims;
  const trimLabels = uniquePresent(scopedTrims.map(sourceDigestTrimLabel));
  const createdCompareTrimIds = uniqueCompareTrimIds(result.compareTrimIds).slice(0, 4);
  const createdTrimLabelsById = new Map<string, string>();
  createdCompareTrimIds.forEach((trimId, index) => {
    createdTrimLabelsById.set(trimId, trimLabels[index] ?? trimId);
  });
  const omittedCompareTrims = comparePlacement.omittedCreatedTrimIds.map((trimId) => ({
    trimId,
    label: createdTrimLabelsById.get(trimId) ?? trimId,
  }));
  const sourceParts = uniquePresent([
    result.sourceFileName,
    candidate?.sourceFileName,
    sourceSnapshot?.sourceFileName,
    group.sourceSheet,
  ]);
  const modelValues = uniquePresent([
    candidate ? sourceDigestCandidateModelLabel(candidate) : null,
    result.groupTitle,
    group.modelName,
    group.title,
    ...scopedTrims.map((trim) => trim.modelName),
  ]);
  const sourceLabel = sourceParts.length > 0 ? compactList(sourceParts) : "来源待补";
  const modelLabel = modelValues.length > 0 ? compactList(modelValues) : "车型待补";
  const configColumnsLabel = trimLabels.length > 0 ? compactList(trimLabels) : `${result.trimCount} 配置列`;
  const baseColumnLabel = trimLabels[0] ?? "基准列待补";
  const targetColumnLabels = trimLabels.slice(1);
  const targetColumnsLabel = targetColumnLabels.length > 0 ? compactList(targetColumnLabels) : "目标列待补";
  const compareActionLabel = sourceDigestDraftCompareActionLabel(comparePlacement);
  return {
    feedback,
    currentCompare: {
      headline: sourceDigestDraftCompareHeadline(comparePlacement),
      label: comparePlacement.appendedToCurrentCompare || comparePlacement.omittedFromCurrentCompareCount > 0
        ? targetColumnLabels.length > 0
          ? `生成 ${baseColumnLabel} / ${targetColumnsLabel}`
          : `生成 ${baseColumnLabel}`
        : targetColumnLabels.length > 0
          ? `基准 ${baseColumnLabel}；目标 ${targetColumnsLabel}`
          : `${baseColumnLabel} 已加入当前对比`,
      meta: `${compareActionLabel} · 当前 ${comparePlacement.currentCompareTrimCount} 个配置列`,
    },
    createdCompareTrimCount: comparePlacement.createdCompareTrimCount,
    omittedCompareTrims,
    featureCatalogMatch: sourceDigestDraftFeatureCatalogMatch(result),
    ocrTransparency: sourceDigestDraftOcrTransparency(result, candidate),
    pathStages: [
      {
        key: "source",
        label: "Source",
        value: sourceLabel,
        meta: candidate
          ? [sourceDigestSourceScopeLabel(candidate), sourceDigestOwnerLabel(candidate)].filter((value): value is string => Boolean(value)).join(" · ") || "来源范围待补"
          : [sourceSnapshot?.fileType, sourceSnapshot?.createdBy ? `上传人 ${sourceSnapshot.createdBy}` : null].filter((value): value is string => Boolean(value)).join(" · ") || "上传来源",
      },
      {
        key: "model",
        label: "Model",
        value: modelLabel,
        meta: candidate
          ? [sourceDigestSourceTypeLabel(candidate), sourceDigestDropdownContextLabel(candidate)].filter(Boolean).join(" · ")
          : group.sourceSheet || "Source Digest",
      },
      {
        key: "config-columns",
        label: "配置列",
        value: configColumnsLabel,
        meta: `${result.trimCount} 配置列 · ${result.featureCount} 配置项`,
      },
    ],
    metrics: [
      { key: "created", label: "新建列", value: String(result.createdTrimCount) },
      { key: "reused", label: "复用列", value: String(result.reusedTrimCount) },
      { key: "visible", label: "加入当前", value: `${comparePlacement.visibleCreatedCompareTrimCount}/${comparePlacement.createdCompareTrimCount}` },
      ...(comparePlacement.omittedFromCurrentCompareCount > 0
        ? [{ key: "omitted", label: "暂未显示", value: String(comparePlacement.omittedFromCurrentCompareCount) }]
        : []),
      { key: "features", label: "配置项", value: String(result.featureCount) },
      { key: "values", label: "配置值", value: String(result.valueRecordCount) },
    ],
  };
}

function sourceDigestAnchorActionLabel(anchor: SourceDigestSearchAnchor): string {
  if (anchor.key === "source") return "聚焦来源";
  if (anchor.key === "model") return "聚焦车型";
  if (anchor.key === "brand") return "筛选品牌";
  if (anchor.key === "market") return "筛选市场";
  if (anchor.key === "model-year") return "筛选 MY";
  if (anchor.key === "segment") return "筛选 Segment";
  return `筛选 ${anchor.label}`;
}

function sourceDigestQueryScopeLabel(query: string, candidates: SourceDigestGroupCandidate[]): string {
  const normalizedQuery = normalizedMatchText(query);
  if (!normalizedQuery) return "关键词";
  const modelMatched = candidates.some((candidate) => (
    sourceDigestModelValues(candidate).some((value) => {
      const normalizedModel = normalizedMatchText(value);
      return normalizedModel === normalizedQuery
        || normalizedModel.includes(normalizedQuery)
      || normalizedQuery.includes(normalizedModel);
    })
  ));
  if (!modelMatched) return "关键词";
  const coverage = sourceDigestCandidateCoverage(candidates);
  if (coverage.modelCount !== 1) return "关键词";
  return coverage.sourceCount > 1 ? "跨来源车型" : "聚焦车型";
}

function sourceDigestGroupMatchMeta(candidate: SourceDigestGroupCandidate): string | null {
  const matches = candidate.sourceSearchMatches?.filter(Boolean).slice(0, 3) ?? [];
  if (matches.length === 0) return null;
  return `命中 ${matches.join(" / ")}`;
}

function sourceDigestCandidateCoverage(candidates: SourceDigestGroupCandidate[]): SourceDigestCandidateCoverage {
  const sourceKeys = new Set<string>();
  const modelNames = new Set<string>();
  return candidates.reduce<SourceDigestCandidateCoverage>((coverage, candidate) => {
    sourceKeys.add(`${candidate.sourceKind}:${candidate.sourceId ?? candidate.sourceFileName}`);
    modelNames.add(sourceDigestCandidateModelLabel(candidate));
    return {
      differenceCount: coverage.differenceCount + candidate.group.differenceCount,
      modelCount: modelNames.size,
      rowCount: coverage.rowCount + candidate.group.rows.length,
      sourceCount: sourceKeys.size,
      trimCount: coverage.trimCount + candidate.group.trimCount,
    };
  }, {
    differenceCount: 0,
    modelCount: 0,
    rowCount: 0,
    sourceCount: 0,
    trimCount: 0,
  });
}

function sourceDigestDirectOptionCoverage(candidates: SourceDigestGroupCandidate[]): SourceDigestDirectOptionCoverage {
  const librarySourceKeys = new Set<string>();
  const libraryModelNames = new Set<string>();
  const localSourceKeys = new Set<string>();
  const localModelNames = new Set<string>();
  const coverage: SourceDigestDirectOptionCoverage = {
    libraryGroupCount: 0,
    libraryModelCount: 0,
    libraryOptionCount: 0,
    librarySourceCount: 0,
    localGroupCount: 0,
    localModelCount: 0,
    localOptionCount: 0,
    localSourceCount: 0,
  };
  candidates.forEach((candidate) => {
    const optionCount = 1 + candidate.group.trims.filter((trim) => Boolean(sourceDigestTrimId(trim))).length;
    const sourceKey = `${candidate.sourceKind}:${candidate.sourceId ?? candidate.sourceFileName}`;
    const modelLabel = sourceDigestCandidateModelLabel(candidate);
    if (candidate.sourceKind === "library") {
      librarySourceKeys.add(sourceKey);
      libraryModelNames.add(modelLabel);
      coverage.libraryGroupCount += 1;
      coverage.libraryOptionCount += optionCount;
      coverage.librarySourceCount = librarySourceKeys.size;
      coverage.libraryModelCount = libraryModelNames.size;
      return;
    }
    localSourceKeys.add(sourceKey);
    localModelNames.add(modelLabel);
    coverage.localGroupCount += 1;
    coverage.localOptionCount += optionCount;
    coverage.localSourceCount = localSourceKeys.size;
    coverage.localModelCount = localModelNames.size;
  });
  return coverage;
}

function sourceDigestDirectResultHint(coverage: SourceDigestDirectOptionCoverage): string | null {
  const parts = [
    coverage.libraryOptionCount > 0
      ? `来源库 ${coverage.libraryGroupCount} 组 / ${coverage.libraryOptionCount} 个可生成在线表选项（覆盖 ${coverage.librarySourceCount} 个来源 / ${coverage.libraryModelCount} 个车型）`
      : null,
    coverage.localOptionCount > 0
      ? `本地样例 ${coverage.localGroupCount} 组 / ${coverage.localOptionCount} 个可预览选项（覆盖 ${coverage.localSourceCount} 个来源 / ${coverage.localModelCount} 个车型）`
      : null,
  ].filter((part): part is string => Boolean(part));
  if (parts.length === 0) return null;
  const actionHint = coverage.libraryOptionCount > 0
    ? "点击生成配置列后会写入共享配置列库，并进入当前对比；结果太宽时继续按来源 / 车型 / 配置列收窄。"
    : "本地样例仅用于预览；上传或搜索来源库后可生成正式配置列。";
  return `${parts.join("；")}。${actionHint}`;
}

function sourceDigestDirectCoverageItems(coverage: SourceDigestDirectOptionCoverage): SourceDigestDirectCoverageItem[] {
  const items: SourceDigestDirectCoverageItem[] = [];
  if (coverage.libraryOptionCount > 0) {
    items.push({
      key: "library",
      label: "来源库",
      groupCount: coverage.libraryGroupCount,
      modelCount: coverage.libraryModelCount,
      optionCount: coverage.libraryOptionCount,
      sourceCount: coverage.librarySourceCount,
      status: "可生成在线配置列",
    });
  }
  if (coverage.localOptionCount > 0) {
    items.push({
      key: "local",
      label: "本地样例",
      groupCount: coverage.localGroupCount,
      modelCount: coverage.localModelCount,
      optionCount: coverage.localOptionCount,
      sourceCount: coverage.localSourceCount,
      status: "仅预览",
    });
  }
  return items;
}

function buildDirectConfigSearchSummaryItems({
  formalOptionCount,
  formalSearchActive,
  formalTotalRows,
  pendingColumnCount,
  pendingGroupCount,
  sourceCoverage,
}: {
  formalOptionCount: number;
  formalSearchActive: boolean;
  formalTotalRows: number;
  pendingColumnCount: number;
  pendingGroupCount: number;
  sourceCoverage: SourceDigestDirectOptionCoverage;
}): DirectConfigSearchSummaryItem[] {
  const formalValue = formalSearchActive && formalTotalRows > formalOptionCount
    ? `${formalOptionCount}/${formalTotalRows}`
    : String(formalOptionCount);
  const formalDescription = formalOptionCount > 0
    ? (formalSearchActive ? "库内命中，可直接加入" : "已加载，可直接加入")
    : (formalSearchActive ? "库内未命中，可搜索或上传来源" : "暂无可加入配置列");
  return [
    {
      key: "formal",
      label: "正式配置列",
      value: formalValue,
      description: formalDescription,
      tone: formalOptionCount > 0 ? "ready" : "muted",
    },
    {
      key: "source-digest",
      label: "Source Digest",
      value: sourceCoverage.libraryGroupCount > 0
        ? `${sourceCoverage.libraryGroupCount} 组 / ${sourceCoverage.librarySourceCount} 源`
        : "0",
      description: sourceCoverage.libraryOptionCount > 0
        ? `${sourceCoverage.libraryOptionCount} 个可生成在线表选项`
        : "搜索来源库或上传文件",
      tone: sourceCoverage.libraryOptionCount > 0 ? "ready" : "muted",
    },
    {
      key: "pending",
      label: "待生成",
      value: pendingColumnCount > 0 ? `${pendingColumnCount} 列 / ${pendingGroupCount} 组` : "0",
      description: pendingColumnCount >= 2 ? "确认后写入配置列库" : "同一来源至少暂选 2 列",
      tone: pendingColumnCount > 0 ? "pending" : "muted",
    },
  ];
}

function sourceDigestDirectAmbiguityItems(candidates: SourceDigestGroupCandidate[]): SourceDigestDirectAmbiguityItem[] {
  const groups = new Map<string, {
    label: string;
    candidates: SourceDigestGroupCandidate[];
    sourceKeys: Set<string>;
    sheetKeys: Set<string>;
    ownerKeys: Set<string>;
    searchQuery: string;
  }>();
  candidates.forEach((candidate) => {
    const brandLabel = compactList(sourceDigestBrandValues(candidate)) || "品牌待补";
    const modelLabel = sourceDigestCandidateModelLabel(candidate);
    const marketValues = sourceDigestMarketValues(candidate);
    const modelYearValues = sourceDigestModelYearValues(candidate);
    const marketLabel = marketValues.length > 0 ? compactList(marketValues) : "市场待补";
    const modelYearLabel = modelYearValues.length > 0 ? `MY ${compactList(modelYearValues)}` : "年款待补";
    const searchQuery = uniquePresent([
      modelLabel,
      ...marketValues,
      ...modelYearValues,
    ]).join(" ");
    const key = [brandLabel, modelLabel, marketLabel, modelYearLabel].map(normalizedMatchText).join("|");
    const current = groups.get(key) ?? {
      label: `${modelLabel} · ${marketLabel} · ${modelYearLabel}`,
      candidates: [],
      sourceKeys: new Set<string>(),
      sheetKeys: new Set<string>(),
      ownerKeys: new Set<string>(),
      searchQuery,
    };
    current.candidates.push(candidate);
    current.sourceKeys.add(`${candidate.sourceKind}:${candidate.sourceId ?? candidate.sourceFileName}:${candidate.sourceFileName}`);
    if (candidate.group.sourceSheet.trim()) current.sheetKeys.add(candidate.group.sourceSheet.trim());
    if (candidate.createdBy?.trim()) current.ownerKeys.add(candidate.createdBy.trim());
    groups.set(key, current);
  });
  return Array.from(groups.entries())
    .filter(([, group]) => (
      group.candidates.length > 1
      && (group.sourceKeys.size > 1 || group.sheetKeys.size > 1 || group.ownerKeys.size > 1)
    ))
    .map(([key, group]) => ({
      key,
      label: group.label,
      candidateCount: group.candidates.length,
      sourceCount: group.sourceKeys.size,
      sheetCount: group.sheetKeys.size,
      ownerCount: group.ownerKeys.size,
      searchQuery: group.searchQuery,
    }))
    .sort((a, b) => {
      const candidateDifference = b.candidateCount - a.candidateCount;
      if (candidateDifference !== 0) return candidateDifference;
      return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
    });
}

function formalLibraryModelAmbiguityItems(trims: VehicleTrimItem[]): DirectModelAmbiguityItem[] {
  const groups = new Map<string, {
    label: string;
    ownerKeys: Set<string>;
    searchQuery: string;
    sourceKeys: Set<string>;
    trims: VehicleTrimItem[];
  }>();
  uniqueVehicleTrims(trims).forEach((trim) => {
    const brandLabel = trim.brand?.trim() || "品牌待补";
    const modelLabel = trim.modelName?.trim() || "Model 待补";
    const marketLabel = (trim.market || trim.country)?.trim() || "市场待补";
    const modelYearLabel = trim.modelYear?.trim() ? `MY ${trim.modelYear.trim()}` : "年款待补";
    const sourceValue = (trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)?.trim() || "来源待补";
    const ownerValue = trim.sourceCreatedBy?.trim();
    const key = [brandLabel, modelLabel, marketLabel, modelYearLabel].map(normalizedMatchText).join("|");
    const current = groups.get(key) ?? {
      label: `${modelLabel} · ${marketLabel} · ${modelYearLabel}`,
      ownerKeys: new Set<string>(),
      searchQuery: uniquePresent([modelLabel, marketLabel, trim.modelYear]).join(" "),
      sourceKeys: new Set<string>(),
      trims: [],
    };
    current.trims.push(trim);
    current.sourceKeys.add(sourceValue);
    if (ownerValue) current.ownerKeys.add(ownerValue);
    groups.set(key, current);
  });
  return Array.from(groups.entries())
    .filter(([, group]) => group.trims.length > 1 && group.sourceKeys.size > 1)
    .map(([key, group]) => ({
      candidateCount: group.trims.length,
      itemUnitLabel: "配置列",
      key: `formal:${key}`,
      label: group.label,
      origin: "formal-library" as const,
      ownerCount: group.ownerKeys.size,
      searchQuery: group.searchQuery,
      sheetCount: 0,
      sourceCount: group.sourceKeys.size,
    }))
    .sort((a, b) => {
      const sourceDifference = b.sourceCount - a.sourceCount;
      if (sourceDifference !== 0) return sourceDifference;
      const candidateDifference = b.candidateCount - a.candidateCount;
      if (candidateDifference !== 0) return candidateDifference;
      return a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
    });
}

function sourceDigestAmbiguityToDirectModelAmbiguity(item: SourceDigestDirectAmbiguityItem): DirectModelAmbiguityItem {
  return {
    ...item,
    itemUnitLabel: "组",
    origin: "source-digest",
  };
}

function sourceDigestCrossSourceReviewMeta(candidates: SourceDigestGroupCandidate[]): string | null {
  const coverage = sourceDigestCandidateCoverage(candidates);
  if (coverage.sourceCount < 2) return null;
  const ambiguityItems = sourceDigestDirectAmbiguityItems(candidates);
  const strongestAmbiguity = ambiguityItems[0];
  if (strongestAmbiguity) {
    const parts = [
      `${strongestAmbiguity.sourceCount} 来源`,
      strongestAmbiguity.sheetCount > 1 ? `${strongestAmbiguity.sheetCount} sheet` : null,
      strongestAmbiguity.ownerCount > 1 ? `${strongestAmbiguity.ownerCount} 上传人` : null,
    ].filter((part): part is string => Boolean(part));
    return `跨来源核对：${strongestAmbiguity.label} 同国家同年款存在 ${parts.join(" / ")}`;
  }
  const markets = uniquePresent(candidates.flatMap(sourceDigestMarketValues));
  const modelYears = uniquePresent(candidates.flatMap(sourceDigestModelYearValues));
  const parts = [
    `${coverage.sourceCount} 来源`,
    markets.length > 0 ? `${markets.length} 市场` : null,
    modelYears.length > 0 ? `${modelYears.length} 年款` : null,
  ].filter((part): part is string => Boolean(part));
  return `跨来源核对：${parts.join(" / ")}，生成前确认市场、年款和来源`;
}

function coverageCountLabel(visible: number, total: number): string {
  return total > visible ? `${visible}/${total}` : String(visible);
}

function buildSourceDigestBrowseGroups(candidates: SourceDigestGroupCandidate[]): SourceDigestBrowseGroup[] {
  const groups = new Map<string, SourceDigestGroupCandidate[]>();
  candidates.forEach((candidate) => {
    const brandLabel = compactList(sourceDigestBrandValues(candidate)) || "品牌待补";
    const marketLabel = compactList(sourceDigestMarketValues(candidate)) || "市场待补";
    const modelLabel = sourceDigestCandidateModelLabel(candidate);
    const sourceKey = [
      candidate.sourceKind,
      candidate.sourceId ?? candidate.sourceFileName,
      brandLabel,
      marketLabel,
      modelLabel,
    ].join("::");
    const current = groups.get(sourceKey);
    if (current) current.push(candidate);
    else groups.set(sourceKey, [candidate]);
  });
  return Array.from(groups.entries()).map(([key, groupedCandidates]) => {
    const firstCandidate = groupedCandidates[0];
    const brandValues = uniquePresent(groupedCandidates.flatMap(sourceDigestBrandValues));
    const marketValues = uniquePresent(groupedCandidates.flatMap(sourceDigestMarketValues));
    const modelValues = uniquePresent(groupedCandidates.flatMap(sourceDigestGroupModelValues));
    const sourceValues = uniquePresent(groupedCandidates.map((candidate) => candidate.sourceFileName));
    const brandLabel = compactList(brandValues) || "品牌待补";
    const marketLabel = compactList(marketValues) || "市场待补";
    const modelLabel = compactList(modelValues) || "车型待补";
    return {
      brandLabel,
      candidates: groupedCandidates,
      coverage: sourceDigestCandidateCoverage(groupedCandidates),
      key,
      marketLabel,
      modelLabel,
      ownerLabel: firstCandidate ? sourceDigestOwnerLabel(firstCandidate) : null,
      pathAnchors: [
        {
          key: "source",
          label: "Source",
          value: firstCandidate?.sourceFileName ?? "来源待补",
          query: sourceDigestAnchorQuery(sourceValues),
          sourceId: firstCandidate?.sourceId,
        },
        { key: "brand", label: "品牌", value: brandLabel, query: sourceDigestAnchorQuery(brandValues) },
        { key: "market", label: "市场", value: marketLabel, query: sourceDigestAnchorQuery(marketValues) },
        { key: "model", label: "车型", value: modelLabel, query: sourceDigestAnchorQuery(modelValues) },
      ],
      sourceFileName: firstCandidate?.sourceFileName ?? "来源待补",
      sourceId: firstCandidate?.sourceId,
      sourceScopeLabel: firstCandidate ? sourceDigestSourceScopeLabel(firstCandidate) : "来源",
    };
  });
}

function selectedModelAnchorSet(selectedTrims: ComparableTrim[]): Set<string> {
  return new Set(selectedTrims.map((trim) => normalizedMatchText(trim.modelName)).filter(Boolean));
}

function sourceDigestCandidateAnchorScore(candidate: SourceDigestGroupCandidate, modelAnchors: Set<string>): number {
  if (modelAnchors.size === 0) return 0;
  const candidateModelValues = [
    candidate.group.modelName,
    candidate.group.title,
    ...candidate.group.trims.map((trim) => trim.modelName),
  ].map(normalizedMatchText).filter(Boolean);
  return candidateModelValues.filter((value) => modelAnchors.has(value)).length;
}

function sourceDigestBrowseGroupAnchorScore(group: SourceDigestBrowseGroup, modelAnchors: Set<string>): number {
  if (modelAnchors.size === 0) return 0;
  return group.candidates.reduce((score, candidate) => score + sourceDigestCandidateAnchorScore(candidate, modelAnchors), 0);
}

function prioritiseSourceDigestCandidatesBySelectedModels(
  candidates: SourceDigestGroupCandidate[],
  selectedTrims: ComparableTrim[],
): SourceDigestGroupCandidate[] {
  const modelAnchors = selectedModelAnchorSet(selectedTrims);
  if (modelAnchors.size === 0) return candidates;
  return candidates
    .map((candidate, index) => ({
      candidate,
      index,
      score: sourceDigestCandidateAnchorScore(candidate, modelAnchors),
    }))
    .sort((a, b) => {
      const scoreDifference = b.score - a.score;
      if (scoreDifference !== 0) return scoreDifference;
      return a.index - b.index;
    })
    .map((item) => item.candidate);
}

function prioritiseSourceDigestBrowseGroupsBySelectedModels(
  groups: SourceDigestBrowseGroup[],
  selectedTrims: ComparableTrim[],
): SourceDigestBrowseGroup[] {
  const modelAnchors = selectedModelAnchorSet(selectedTrims);
  if (modelAnchors.size === 0) return groups;
  return groups
    .map((group, index) => ({
      group,
      index,
      score: sourceDigestBrowseGroupAnchorScore(group, modelAnchors),
    }))
    .sort((a, b) => {
      const scoreDifference = b.score - a.score;
      if (scoreDifference !== 0) return scoreDifference;
      return a.index - b.index;
    })
    .map((item) => item.group);
}

function compareAnchorValue(
  baseValue: string | null | undefined,
  targetValue: string | null | undefined,
  samePrefix: string,
  differentPrefix: string,
  missingLabel: string,
): { value: string; tone: TargetAnchorTone } {
  const base = baseValue?.trim();
  const target = targetValue?.trim();
  if (!base || !target) return { value: missingLabel, tone: "warning" };
  if (base === target) return { value: `${samePrefix} ${target}`, tone: "ready" };
  return { value: `${differentPrefix} ${base} → ${target}`, tone: "warning" };
}

function buildAllTargetAnchorItems(baseTrim: ComparableTrim | null, trims: ComparableTrim[]): TargetAnchorItem[] {
  const targetTrims = baseTrim ? trims.filter((trim) => trim.trimId !== baseTrim.trimId) : trims;
  const noMaterialCount = targetTrims.filter((trim) => !hasMaterialAnchor(trim)).length;
  const markets = uniquePresent(targetTrims.map((trim) => trim.market || trim.country));
  const modelYears = uniquePresent(targetTrims.map((trim) => trim.modelYear));
  const sourceLabels = targetTrims.map((trim) => trimSourceLabel(trim));
  const missingSourceCount = sourceLabels.filter((source) => source === "来源待补").length;
  const sources = uniquePresent(sourceLabels.filter((source) => source !== "来源待补"));

  return [
    {
      key: "target-scope",
      label: "目标范围",
      value: baseTrim ? `全部 ${targetTrims.length} 个目标配置列` : `全部 ${targetTrims.length} 个对象`,
      tone: "neutral",
    },
    {
      key: "material-anchor",
      label: "物料锚点",
      value: noMaterialCount > 0 ? `无物料号 ${noMaterialCount}，需用来源 / sales version` : "目标均有物料号",
      tone: noMaterialCount > 0 ? "warning" : "ready",
    },
    {
      key: "market-anchor",
      label: "市场",
      value: markets.length > 1 ? `跨市场 ${compactList(markets)}` : markets[0] ? `同市场 ${markets[0]}` : "市场待补",
      tone: markets.length > 1 || markets.length === 0 ? "warning" : "ready",
    },
    {
      key: "model-year-anchor",
      label: "年款 / 改款",
      value: modelYears.length > 1 ? `跨年款 ${compactList(modelYears)}` : modelYears[0] ? `同年款 ${modelYears[0]}` : "年款待补",
      tone: modelYears.length > 1 ? "warning" : "neutral",
    },
    {
      key: "source-anchor",
      label: "来源",
      value: missingSourceCount > 0 ? `来源待补 ${missingSourceCount}` : sources.length > 1 ? `多来源 ${compactList(sources)}` : sources[0] ? `同来源 ${sources[0]}` : "来源待补",
      tone: missingSourceCount > 0 || sources.length > 1 || sources.length === 0 ? "warning" : "ready",
    },
  ];
}

function buildTargetAnchorItems(baseTrim: ComparableTrim | null, targetTrim: ComparableTrim | null, trims: ComparableTrim[]): TargetAnchorItem[] {
  if (!targetTrim) return buildAllTargetAnchorItems(baseTrim, trims);
  if (!baseTrim) {
    return [{
      key: "target-only",
      label: "目标对象",
      value: `${compareTrimLabel(targetTrim)} · ${trimIdentityAnchorLabel(targetTrim)}`,
      tone: hasMaterialAnchor(targetTrim) ? "ready" : "warning",
    }];
  }

  const market = compareAnchorValue(baseTrim.market || baseTrim.country, targetTrim.market || targetTrim.country, "同市场", "跨市场", "市场待补");
  const modelYear = compareAnchorValue(baseTrim.modelYear, targetTrim.modelYear, "同年款", "跨年款", "年款待补");
  const source = compareAnchorValue(
    baseTrim.sourceFileName || baseTrim.sourceUploadId || baseTrim.sourceFilePath,
    targetTrim.sourceFileName || targetTrim.sourceUploadId || targetTrim.sourceFilePath,
    "同来源",
    "多来源",
    "来源待补",
  );
  const targetHasMaterial = hasMaterialAnchor(targetTrim);
  const originMixed = trimOriginLabel(baseTrim) !== trimOriginLabel(targetTrim);

  return [
    {
      key: "identity",
      label: "身份",
      value: `${trimOriginLabel(baseTrim)} → ${trimOriginLabel(targetTrim)}`,
      tone: originMixed ? "warning" : "ready",
    },
    {
      key: "material",
      label: "目标锚点",
      value: targetHasMaterial ? trimIdentityAnchorLabel(targetTrim) : `${trimIdentityAnchorLabel(targetTrim)} · 无物料号`,
      tone: targetHasMaterial ? "ready" : "warning",
    },
    { key: "market", label: "市场", value: market.value, tone: market.tone },
    { key: "model-year", label: "年款 / 改款", value: modelYear.value, tone: modelYear.tone },
    { key: "source", label: "来源", value: source.value, tone: source.tone },
  ];
}

function buildComparisonIdentityNotes(trims: ComparableTrim[]): ComparisonIdentityNote[] {
  if (trims.length < 2) return [];
  const originLabels = uniquePresent(trims.map((trim) => trimOriginLabel(trim)));
  const noMaterialCount = trims.filter((trim) => !hasMaterialAnchor(trim)).length;
  const markets = uniquePresent(trims.map((trim) => trim.market || trim.country));
  const modelYears = uniquePresent(trims.map((trim) => trim.modelYear));
  const sourceLabels = trims.map((trim) => trimSourceLabel(trim));
  const missingSourceCount = sourceLabels.filter((value) => value === "来源待补").length;
  const sources = uniquePresent(sourceLabels.filter((value) => value !== "来源待补"));
  const notes: ComparisonIdentityNote[] = [];

  if (originLabels.includes("本品") && originLabels.some((label) => label !== "本品")) {
    notes.push({
      key: "origin-mix",
      label: "身份锚点混合",
      detail: "本品通常有物料号；竞品 / 网页抓取对象可能只能用 sales version、车型、市场和来源锚定。这里只提示证据口径，不需要先选择本品或竞品模式。",
    });
  }
  if (noMaterialCount > 0) {
    notes.push({
      key: "missing-material",
      label: `无物料号 ${noMaterialCount}`,
    detail: "无物料号对象不会按 BOM 直接对齐，需要结合来源证据判断配置差异。",
    });
  }
  if (markets.length > 1) {
    notes.push({
      key: "market-mix",
      label: "跨市场",
      detail: compactList(markets),
    });
  }
  if (modelYears.length > 1) {
    notes.push({
      key: "model-year-mix",
      label: "跨年款 / 改款",
      detail: compactList(modelYears),
    });
  }
  if (sources.length > 1) {
    notes.push({
      key: "source-mix",
      label: "多来源",
      detail: compactList(sources),
    });
  }
  if (missingSourceCount > 0) {
    notes.push({
      key: "missing-source",
      label: `来源待补 ${missingSourceCount}`,
      detail: "同车型不同网站或来源问题时，配置差异需要优先回看来源证据。",
    });
  }

  return notes;
}

function buildComparisonScenario(trims: ComparableTrim[]): ComparisonIdentityNote | null {
  if (trims.length < 2) return null;
  const originLabels = trims.map((trim) => trimOriginLabel(trim));
  const ownCount = originLabels.filter((label) => label === "本品").length;
  const externalCount = originLabels.filter((label) => label === "竞品 / 外部").length;

  if (ownCount === trims.length) {
    return {
      key: "scenario-own",
      label: "本品配置列",
      detail: "同一产品线不同配置列 / option，直接按已选列对比；优先用物料号、sales version 和配置版本判断。",
    };
  }
  if (ownCount > 0 && externalCount > 0) {
    return {
      key: "scenario-own-vs-competitor",
      label: "本品与外部配置列",
      detail: "无需切换本品 / 竞品模式；本品用物料号锚定，外部抓取对象用来源、车型、市场和 sales version 锚定。",
    };
  }
  if (externalCount === trims.length) {
    return {
      key: "scenario-external",
      label: "外部来源配置列",
      detail: "外部抓取对象通常缺少物料号，直接按已选配置列对比，优先核对来源、市场和年款。",
    };
  }
  return {
    key: "scenario-mixed",
    label: "混合身份配置列",
    detail: "部分对象身份待确认，不需要先选模式；建议先补来源证据再解释配置差异。",
  };
}

function categoryDisplayLabel(category: string): string {
  return category.replace(/\s+/g, " ").trim() || "未分类";
}

function compareTrimLabel(trim: ComparableTrim): string {
  return trim.trimName || trim.fullTrimName || trim.trimId;
}

function trimIdentityDraftFromTrim(trim: ComparableTrim | null): TrimIdentityDraft {
  return {
    brand: trim?.brand ?? "",
    modelName: trim?.modelName ?? "",
    trimName: trim?.trimName ?? "",
    fullTrimName: trim?.fullTrimName ?? "",
    market: trim?.market ?? trim?.country ?? "",
    modelYear: trim?.modelYear ?? "",
    energyType: trim?.energyType ?? "",
    drivetrain: trim?.drivetrain ?? "",
    engine: trim?.engine ?? "",
    materialNo: trim?.materialNo ?? "",
    vehicleCode: trim?.vehicleCode ?? "",
    identityKey: trim?.identityKey ?? "",
  };
}

function trimIdentityPayloadFromDraft(draft: TrimIdentityDraft, comment: string): {
  brand?: string;
  model_name?: string;
  trim_name?: string;
  full_trim_name?: string;
  market?: string;
  model_year?: string;
  energy_type?: string;
  drivetrain?: string;
  engine?: string;
  material_no?: string;
  vehicle_code?: string;
  identity_key?: string;
  comment: string;
} {
  const requiredText = (value: string): string | undefined => value.trim() || undefined;
  const optionalText = (value: string): string => value.trim();
  return {
    brand: requiredText(draft.brand),
    model_name: requiredText(draft.modelName),
    trim_name: requiredText(draft.trimName),
    full_trim_name: requiredText(draft.fullTrimName),
    market: optionalText(draft.market),
    model_year: optionalText(draft.modelYear),
    energy_type: optionalText(draft.energyType),
    drivetrain: optionalText(draft.drivetrain),
    engine: optionalText(draft.engine),
    material_no: optionalText(draft.materialNo),
    vehicle_code: optionalText(draft.vehicleCode),
    identity_key: optionalText(draft.identityKey),
    comment: comment.trim(),
  };
}

function trimIdentityPatchFromApi(response: Record<string, unknown>, fallback: ComparableTrim): TrimIdentityPatch {
  const materialNo = nullableStringFromRecord(response, "materialNo");
  const vehicleCode = nullableStringFromRecord(response, "vehicleCode");
  const identityKey = nullableStringFromRecord(response, "identityKey");
  const salesVersion = nullableStringFromRecord(response, "salesVersion");
  const nextMaterialNo = materialNo !== undefined ? materialNo : fallback.materialNo ?? null;
  return {
    brand: stringFromRecord(response, "brand") ?? fallback.brand,
    modelName: stringFromRecord(response, "modelName") ?? fallback.modelName,
    trimName: nullableStringFromRecord(response, "trimName") !== undefined ? nullableStringFromRecord(response, "trimName") : fallback.trimName ?? null,
    fullTrimName: stringFromRecord(response, "fullTrimName") ?? fallback.fullTrimName,
    market: nullableStringFromRecord(response, "market") !== undefined ? nullableStringFromRecord(response, "market") : fallback.market ?? null,
    country: nullableStringFromRecord(response, "country") !== undefined ? nullableStringFromRecord(response, "country") : fallback.country ?? null,
    modelYear: nullableStringFromRecord(response, "modelYear") !== undefined ? nullableStringFromRecord(response, "modelYear") : fallback.modelYear ?? null,
    energyType: nullableStringFromRecord(response, "energyType") !== undefined ? nullableStringFromRecord(response, "energyType") : fallback.energyType ?? null,
    drivetrain: nullableStringFromRecord(response, "drivetrain") !== undefined ? nullableStringFromRecord(response, "drivetrain") : fallback.drivetrain ?? null,
    engine: nullableStringFromRecord(response, "engine") !== undefined ? nullableStringFromRecord(response, "engine") : fallback.engine ?? null,
    materialNo: nextMaterialNo,
    vehicleCode: vehicleCode !== undefined ? vehicleCode : fallback.vehicleCode ?? null,
    identityKey: identityKey !== undefined ? identityKey : fallback.identityKey ?? null,
    salesVersion: salesVersion !== undefined ? salesVersion : fallback.salesVersion ?? null,
    hasMaterialNo: Boolean((nextMaterialNo ?? "").trim()),
    dataOrigin: Boolean((nextMaterialNo ?? "").trim()) ? "own_catalog" : "external_or_scraped",
  };
}

function applyTrimIdentityPatch<T extends ComparableTrim>(trim: T, trimId: string, patch: TrimIdentityPatch): T {
  if (trim.trimId !== trimId) return trim;
  return { ...trim, ...patch };
}

function updateCompareDataTrimIdentity(data: CompareResponse, trimId: string, patch: TrimIdentityPatch): CompareResponse {
  return {
    ...data,
    trims: data.trims.map((trim) => applyTrimIdentityPatch(trim, trimId, patch)),
  };
}

interface SelectedTrimCardProps {
  trim: ComparableTrim;
  isBaseTrim: boolean;
  isTargetTrim: boolean;
  actionsEnabled?: boolean;
  removeLabel: string;
  onFocusTarget: () => void;
  onOpenSource?: () => void;
  onMoveToTrash?: () => void;
  onRemove: () => void;
  onSetBase: () => void;
  trashDisabled?: boolean;
  trashLoading?: boolean;
}

function SelectedTrimCard({
  trim,
  isBaseTrim,
  isTargetTrim,
  actionsEnabled = true,
  removeLabel,
  onFocusTarget,
  onOpenSource,
  onMoveToTrash,
  onRemove,
  onSetBase,
  trashDisabled = false,
  trashLoading = false,
}: SelectedTrimCardProps): ReactElement {
  const displayTrimLabel = compareTrimLabel(trim);
  return (
    <article className={`product-config-trim-card ${isBaseTrim ? "is-base" : ""} ${isTargetTrim ? "is-target" : ""}`}>
      <div>
        <span>{trim.brand || "Brand"}</span>
        <strong>{trim.modelName || "Model"}</strong>
        <small>{selectedTrimSecondaryLabel(trim)}</small>
      </div>
      <div className="product-config-trim-card-compact-meta" aria-label={`${displayTrimLabel} 简要身份`}>
        <span className={`product-config-origin-badge ${trimOriginClassName(trim)}`}>{trimOriginLabel(trim)}</span>
        <span className={`product-config-origin-badge ${hasMaterialAnchor(trim) ? "is-material" : "is-no-material"}`}>{trimMaterialAnchorLabel(trim)}</span>
      </div>
      <TrimIdentityBadges trim={trim} />
      <p>{trimMeta(trim)}</p>
      <p>身份锚点 {trimIdentityAnchorLabel(trim)}</p>
      <p>来源 {trimSourceSnapshotLabel(trim)}{trimSourceCreatedAtLabel(trim) ? ` · ${trimSourceCreatedAtLabel(trim)}` : ""}</p>
      {trimProfileLabel(trim) ? <p>{trimProfileLabel(trim)}</p> : null}
      <p>{trimPowertrain(trim)}</p>
      {actionsEnabled ? (
        <div className="product-config-trim-actions">
          <button
            className="btn btn-sm btn-secondary"
            type="button"
            disabled={isBaseTrim}
            aria-label={isBaseTrim ? `当前基准列 ${displayTrimLabel}` : `设 ${displayTrimLabel} 为基准列`}
            aria-pressed={isBaseTrim}
            onClick={onSetBase}
          >
            {isBaseTrim ? "当前基准列" : "设为基准列"}
          </button>
          {!isBaseTrim ? (
            <button
              className="btn btn-sm btn-secondary product-config-trim-focus"
              type="button"
              aria-label={isTargetTrim ? `显示全部目标列，取消 ${displayTrimLabel} 目标列聚焦` : `查看 ${displayTrimLabel} 差异行`}
              aria-pressed={isTargetTrim}
              onClick={onFocusTarget}
            >
              {isTargetTrim ? "显示全部目标列" : "查看差异行"}
            </button>
          ) : null}
          {onOpenSource ? <button className="btn btn-sm btn-ghost" type="button" onClick={onOpenSource}>查看来源</button> : null}
          {onMoveToTrash ? (
            <button
              className="btn btn-sm btn-ghost"
              type="button"
              aria-label={`移入库垃圾桶 ${displayTrimLabel}`}
              disabled={trashDisabled || trashLoading}
              onClick={onMoveToTrash}
            >
              {trashLoading ? "移入中" : "移入库垃圾桶"}
            </button>
          ) : null}
          <button className="btn btn-sm btn-ghost" type="button" aria-label={`${removeLabel} ${displayTrimLabel}`} onClick={onRemove}>{removeLabel}</button>
        </div>
      ) : null}
    </article>
  );
}

function visibleCategoriesForDeltaFilter(
  data: CompareResponse,
  baseTrimId: string | null,
  deltaFilter: ConfigComparisonDeltaFilter,
  search: string,
  targetTrimId: string | null,
): string[] {
  return [...new Set(data.rows
    .filter((row) => (
      rowMatchesConfigScope(data, row, deltaFilter, baseTrimId, targetTrimId)
      && rowMatchesConfigSearch(row, search)
    ))
    .map((row) => row.category))];
}

function buildCategoryNavItems(rows: CompareRow[]): CategoryNavItem[] {
  const categoryCounts = new Map<string, number>();
  rows.forEach((row) => {
    categoryCounts.set(row.category, (categoryCounts.get(row.category) ?? 0) + 1);
  });
  return Array.from(categoryCounts.entries()).map(([category, count]) => ({ category, count }));
}

function scopeUnitLabel(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "UNKNOWN") return "项待确认";
  if (deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE") return "项证据";
  if (
    deltaFilter === "DIFFERENCE"
    || deltaFilter === "ADDED"
    || deltaFilter === "REMOVED"
    || deltaFilter === "VALUE_CHANGED"
    || deltaFilter === "OPTIONAL_CHANGED"
    || deltaFilter === "INFERRED"
  ) {
    return "项差异";
  }
  return "项配置";
}

function simpleRowScopeUnitLabel(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "UNKNOWN") return "待确认行";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题行";
  if (deltaFilter === "MERGED_SOURCE") return "合并格行";
  if (
    deltaFilter === "DIFFERENCE"
    || deltaFilter === "ADDED"
    || deltaFilter === "REMOVED"
    || deltaFilter === "VALUE_CHANGED"
    || deltaFilter === "OPTIONAL_CHANGED"
    || deltaFilter === "INFERRED"
  ) {
    return "差异行";
  }
  return "配置行";
}

function resultScopeLabel(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "全部配置";
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
  return "全部配置";
}

function simpleDisplayScopeLabel(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "全部配置行";
  if (deltaFilter === "DIFFERENCE") return "差异行";
  if (deltaFilter === "INFERRED") return "规则推断行";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题行";
  if (deltaFilter === "MERGED_SOURCE") return "合并格行";
  if (deltaFilter === "UNKNOWN") return "待确认行";
  if (deltaFilter === "COMMON") return "共同配置行";
  if (deltaFilter === "ADDED") return "新增配置行";
  if (deltaFilter === "REMOVED") return "减少配置行";
  if (deltaFilter === "VALUE_CHANGED") return "值变化行";
  if (deltaFilter === "OPTIONAL_CHANGED") return "选装变化行";
  return "配置行";
}

function displayScopeButtonText(option: DisplayScopeOption, count: number, simpleRowMode: boolean): string {
  if (simpleRowMode) return `${simpleDisplayScopeLabel(option.key)} ${count}`;
  return `${option.label} ${count} 项`;
}

function simpleDisplayScopeDescription(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "完整保留 xlsx 配置行，适合先通读原表。";
  if (deltaFilter === "DIFFERENCE") return "只看和基准配置列不一致的配置行。";
  if (deltaFilter === "INFERRED") return "只看规则推断出的行，例如不配备*，需要点单元格看来源。";
  if (deltaFilter === "MISSING_SOURCE") return "只看缺值或缺来源的行，用来补来源证据。";
  if (deltaFilter === "MERGED_SOURCE") return "只看 xlsx 合并格展开到各配置列的行。";
  if (deltaFilter === "UNKNOWN") return "只看空白无法判断的行，不直接等于无配置。";
  if (deltaFilter === "COMMON") return "只看已选配置列保持一致的配置行。";
  return "按当前口径查看配置行。";
}

function displayScopeDescription(option: DisplayScopeOption, simpleRowMode: boolean): string {
  if (simpleRowMode) return simpleDisplayScopeDescription(option.key);
  return option.description;
}

function buildSameModelTrimGroups(trims: VehicleTrimItem[]): SameModelTrimGroup[] {
  const groups = new Map<string, VehicleTrimItem[]>();
  trims.forEach((trim) => {
    const key = [trim.brand, trim.modelName, trim.market || trim.country, trim.modelYear]
      .map((value) => value?.trim().toLowerCase() || "-")
      .join("|");
    groups.set(key, [...(groups.get(key) ?? []), trim]);
  });
  return Array.from(groups.entries())
    .map(([key, items]) => {
      const sample = items[0];
      return {
        key,
        label: [sample.brand, sample.modelName].filter(Boolean).join(" ") || sample.modelName || "同车型",
        meta: [sample.market || sample.country, sample.modelYear, `${items.length} 配置列`].filter(Boolean).join(" · "),
        items: [...items].sort((a, b) => {
          const materialRank = Number(Boolean(b.materialNo || b.hasMaterialNo)) - Number(Boolean(a.materialNo || a.hasMaterialNo));
          if (materialRank !== 0) return materialRank;
          return (a.trimName || a.fullTrimName).localeCompare(b.trimName || b.fullTrimName, undefined, { numeric: true, sensitivity: "base" });
        }),
      };
    })
    .filter((group) => group.items.length >= 2)
    .sort((a, b) => b.items.length - a.items.length)
    .slice(0, 5);
}

function buildLibraryTrimGroups(trims: VehicleTrimItem[]): SameModelTrimGroup[] {
  const groups = new Map<string, VehicleTrimItem[]>();
  trims.forEach((trim) => {
    const key = [trim.brand, trim.modelName, trim.market || trim.country, trim.modelYear]
      .map((value) => value?.trim().toLowerCase() || "-")
      .join("|");
    groups.set(key, [...(groups.get(key) ?? []), trim]);
  });
  return Array.from(groups.entries())
    .map(([key, items]) => {
      const sample = items[0];
      const sortedItems = [...items].sort((a, b) => {
        const materialRank = Number(Boolean(b.materialNo || b.hasMaterialNo)) - Number(Boolean(a.materialNo || a.hasMaterialNo));
        if (materialRank !== 0) return materialRank;
        return (a.trimName || a.fullTrimName).localeCompare(b.trimName || b.fullTrimName, undefined, { numeric: true, sensitivity: "base" });
      });
      const sourceCount = uniquePresent(sortedItems.map((trim) => trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)).length;
      return {
        key,
        label: [sample.brand, sample.modelName].filter(Boolean).join(" ") || sample.modelName || "未知车型",
        meta: [
          sample.market || sample.country || "市场待补",
          sample.modelYear || "年款待补",
          `${sortedItems.length} 配置列`,
          sourceCount > 0 ? `${sourceCount} 来源` : "来源待补",
        ].join(" · "),
        items: sortedItems,
      };
    })
    .sort((a, b) => {
      const modelCompare = a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" });
      if (modelCompare !== 0) return modelCompare;
      return b.items.length - a.items.length;
    })
    .slice(0, 12);
}

function buildLibraryBrandTrimGroups(groups: SameModelTrimGroup[]): LibraryBrandTrimGroup[] {
  const brandGroups = new Map<string, SameModelTrimGroup[]>();
  groups.forEach((group) => {
    const sample = group.items[0];
    const key = [sample?.brand, sample?.market || sample?.country]
      .map((value) => value?.trim().toLowerCase() || "-")
      .join("|");
    brandGroups.set(key, [...(brandGroups.get(key) ?? []), group]);
  });
  return Array.from(brandGroups.entries())
    .map(([key, groupedModels]) => {
      const trims = groupedModels.flatMap((group) => group.items);
      const brandValues = uniquePresent(trims.map((trim) => trim.brand));
      const marketValues = uniquePresent(trims.map((trim) => trim.market || trim.country));
      const modelYearValues = uniquePresent(trims.map((trim) => trim.modelYear));
      const sourceCount = uniquePresent(trims.map((trim) => trim.sourceFileName || trim.sourceUploadId || trim.sourceFilePath)).length;
      return {
        key,
        brandLabel: brandValues.length > 0 ? compactList(brandValues) : "品牌待补",
        marketLabel: marketValues.length > 0 ? compactList(marketValues) : "市场待补",
        modelYearLabel: modelYearValues.length > 0 ? `MY ${compactList(modelYearValues)}` : "年款待补",
        modelCount: groupedModels.length,
        trimCount: trims.length,
        sourceCount,
        groups: [...groupedModels].sort((a, b) => a.label.localeCompare(b.label, undefined, { numeric: true, sensitivity: "base" })),
      };
    })
    .sort((a, b) => {
      const brandCompare = a.brandLabel.localeCompare(b.brandLabel, undefined, { numeric: true, sensitivity: "base" });
      if (brandCompare !== 0) return brandCompare;
      return b.trimCount - a.trimCount;
    });
}

function buildFallbackSummary(data: CompareResponse): CompareSummary {
  const categoryCounts = data.rows.reduce<Record<string, number>>((acc, row) => {
    acc[row.category] = (acc[row.category] ?? 0) + 1;
    return acc;
  }, {});
  const uniqueFeatureCount = data.rows.filter((row) => row.comparisonType === "UNIQUE_TO_TRIM").length;
  const partialAvailableCount = data.rows.filter((row) => row.comparisonType === "PARTIAL_AVAILABLE" || row.comparisonType === "UNIQUE_OR_PARTIAL").length;
  const valueDifferentCount = data.rows.filter((row) => row.comparisonType === "DIFFERENT_VALUE").length;
  const availabilityDifferentCount = data.rows.filter((row) => (
    row.comparisonType === "AVAILABILITY_DIFFERENT"
    || row.comparisonType === "OPTIONAL_DIFFERENT"
    || row.comparisonType === "UNIQUE_OR_PARTIAL"
  )).length;
  const optionalDifferentCount = data.rows.filter((row) => row.comparisonType === "OPTIONAL_DIFFERENT").length;
  const missingOrUnknownCount = data.rows.filter((row) => row.comparisonType === "MISSING_OR_UNKNOWN" || row.comparisonType === "MISSING_UNKNOWN").length;
  const confirmedDifferenceCount = data.rows.filter((row) => row.comparisonType !== "COMMON_SAME" && row.comparisonType !== "MISSING_OR_UNKNOWN" && row.comparisonType !== "MISSING_UNKNOWN").length;
  const inferredDifferenceCount = data.rows.filter((row) => (
    row.comparisonType !== "COMMON_SAME"
    && row.comparisonType !== "MISSING_OR_UNKNOWN"
    && row.comparisonType !== "MISSING_UNKNOWN"
    && row.values.some((value) => Boolean(value?.inferred))
  )).length;
  return {
    totalFeatures: data.totalFeatures,
    shownFeatures: data.shownFeatures,
    commonSameCount: data.rows.filter((row) => row.comparisonType === "COMMON_SAME").length,
    differentValueCount: valueDifferentCount,
    valueDifferentCount,
    availabilityDifferentCount,
    optionalDifferentCount,
    confirmedDifferenceCount,
    rawConfirmedDifferenceCount: confirmedDifferenceCount - inferredDifferenceCount,
    inferredDifferenceCount,
    uniqueFeatureCount,
    partialAvailableCount,
    uniqueOrPartialCount: uniqueFeatureCount + partialAvailableCount,
    missingOrUnknownCount,
    notApplicableCount: data.rows.filter((row) => row.comparisonType === "NOT_APPLICABLE").length,
    cancelledOrRemovedCount: data.rows.filter((row) => row.comparisonType === "CANCELLED_OR_REMOVED").length,
    differenceCount: confirmedDifferenceCount,
    categoryCounts,
    differenceCategories: [...new Set(data.rows.filter((row) => row.comparisonType !== "COMMON_SAME" && row.comparisonType !== "MISSING_OR_UNKNOWN" && row.comparisonType !== "MISSING_UNKNOWN").map((row) => row.category))],
  };
}

function buildCompareGroups(rows: CompareRow[]): CompareGroup[] {
  const groups = new Map<string, CompareRow[]>();
  rows.forEach((row) => {
    groups.set(row.category, [...(groups.get(row.category) ?? []), row]);
  });
  return Array.from(groups.entries()).map(([category, items]) => ({ category, items }));
}

function buildCategoryCounts(rows: CompareRow[]): Record<string, number> {
  return rows.reduce<Record<string, number>>((acc, row) => {
    acc[row.category] = (acc[row.category] ?? 0) + 1;
    return acc;
  }, {});
}

function visibleDifferenceCategories(rows: CompareRow[]): string[] {
  return [...new Set(rows
    .filter((row) => row.comparisonType !== "COMMON_SAME" && row.comparisonType !== "MISSING_OR_UNKNOWN" && row.comparisonType !== "MISSING_UNKNOWN")
    .map((row) => row.category))];
}

function isConfirmedDifferenceRow(row: CompareRow): boolean {
  return row.comparisonType !== "COMMON_SAME" && row.comparisonType !== "MISSING_OR_UNKNOWN" && row.comparisonType !== "MISSING_UNKNOWN";
}

function isAvailabilityDifferenceRow(row: CompareRow): boolean {
  return row.comparisonType === "AVAILABILITY_DIFFERENT"
    || row.comparisonType === "OPTIONAL_DIFFERENT"
    || row.comparisonType === "UNIQUE_OR_PARTIAL"
    || row.comparisonType === "UNIQUE_TO_TRIM"
    || row.comparisonType === "PARTIAL_AVAILABLE";
}

function isMissingUnknownRow(row: CompareRow): boolean {
  return row.comparisonType === "MISSING_OR_UNKNOWN" || row.comparisonType === "MISSING_UNKNOWN";
}

function emptyScopedConfigSummaryMetrics(): ScopedConfigSummaryMetrics {
  return {
    confirmedDifferenceCount: 0,
    inferredDifferenceCount: 0,
    availabilityDifferenceCount: 0,
    commonSameCount: 0,
    valueChangedCount: 0,
    missingUnknownCount: 0,
  };
}

function isMergedSourceValue(value: CompareRow["values"][number]): boolean {
  const source = value?.source;
  return Boolean(source?.mergedRange && source.sourceCell && source.sourceCell !== source.cell);
}

function sourceEvidenceValueIndexes(data: CompareResponse, baseTrimId: string | null, targetTrimId: string | null): number[] | null {
  if (!targetTrimId) return null;
  const targetIndex = data.trims.findIndex((trim) => trim.trimId === targetTrimId);
  if (targetIndex < 0) return null;
  const baseIndex = baseTrimId ? data.trims.findIndex((trim) => trim.trimId === baseTrimId) : -1;
  const indexes = [baseIndex, targetIndex].filter((index, position, list): index is number => (
    index >= 0 && list.indexOf(index) === position
  ));
  return indexes.length > 0 ? indexes : null;
}

function summarizeSourceEvidenceRows(rows: CompareRow[], valueIndexes: number[] | null = null): SourceEvidenceSummaryMetrics {
  return rows.reduce<SourceEvidenceSummaryMetrics>((metrics, row) => {
    const values = valueIndexes ? valueIndexes.map((index) => row.values[index] ?? null) : row.values;
    values.forEach((value) => {
      metrics.totalCellCount += 1;
      if (!value) {
        metrics.missingValueCellCount += 1;
        metrics.sourceIssueCellCount += 1;
        return;
      }
      if (!value.source) {
        metrics.missingSourceValueCount += 1;
        metrics.sourceIssueCellCount += 1;
      }
      if (value?.inferred) metrics.inferredCellCount += 1;
      if (isMergedSourceValue(value)) metrics.mergedCellCount += 1;
    });
    return metrics;
  }, {
    inferredCellCount: 0,
    mergedCellCount: 0,
    missingSourceValueCount: 0,
    missingValueCellCount: 0,
    sourceIssueCellCount: 0,
    totalCellCount: 0,
  });
}

function summarizeScopedConfigRows(
  data: CompareResponse,
  rows: CompareRow[],
  baseTrimId: string | null,
  targetTrimId: string | null,
): ScopedConfigSummaryMetrics {
  const metrics = emptyScopedConfigSummaryMetrics();
  const baseModeActive = Boolean(baseTrimId && data.trims.some((trim) => trim.trimId === baseTrimId));

  rows.forEach((row) => {
    if (!baseModeActive) {
      if (isConfirmedDifferenceRow(row)) metrics.confirmedDifferenceCount += 1;
      if (isConfirmedDifferenceRow(row) && row.values.some((value) => Boolean(value?.inferred))) metrics.inferredDifferenceCount += 1;
      if (isAvailabilityDifferenceRow(row)) metrics.availabilityDifferenceCount += 1;
      if (row.comparisonType === "COMMON_SAME") metrics.commonSameCount += 1;
      if (row.comparisonType === "DIFFERENT_VALUE") metrics.valueChangedCount += 1;
      if (isMissingUnknownRow(row)) metrics.missingUnknownCount += 1;
      return;
    }

    const differingDeltas = rowDeltasForBase(data, row, baseTrimId).filter((delta) => (
      delta.deltaType !== "SAME"
      && (!targetTrimId || delta.targetTrim.trimId === targetTrimId)
    ));
    if (differingDeltas.length === 0) {
      metrics.commonSameCount += 1;
      return;
    }

    if (differingDeltas.some((delta) => delta.deltaType !== "UNKNOWN")) metrics.confirmedDifferenceCount += 1;
    if (differingDeltas.some((delta) => delta.inferred)) metrics.inferredDifferenceCount += 1;
    if (differingDeltas.some((delta) => (
      delta.deltaType === "ADDED"
      || delta.deltaType === "REMOVED"
      || delta.deltaType === "OPTIONAL_CHANGED"
    ))) {
      metrics.availabilityDifferenceCount += 1;
    }
    if (differingDeltas.some((delta) => delta.deltaType === "VALUE_CHANGED")) metrics.valueChangedCount += 1;
    if (differingDeltas.some((delta) => delta.deltaType === "UNKNOWN")) metrics.missingUnknownCount += 1;
  });

  return metrics;
}

function primaryScopeMetric(
  deltaFilter: ConfigComparisonDeltaFilter,
  metrics: ScopedConfigSummaryMetrics,
  sourceMetrics: SourceEvidenceSummaryMetrics,
  rowCount: number,
  simpleRowMode = false,
): PrimaryScopeMetric {
  const differenceLabel = simpleRowMode ? "差异行" : "差异项";
  const itemUnit = simpleRowMode ? "行" : "项";
  const configUnit = simpleRowMode ? simpleRowScopeUnitLabel(deltaFilter) : "项配置";
  if (deltaFilter === "COMMON") {
    return {
      label: "共同配置",
      value: metrics.commonSameCount,
      hint: `当前范围为一致配置，${differenceLabel} ${metrics.confirmedDifferenceCount} ${itemUnit}`,
    };
  }
  if (deltaFilter === "UNKNOWN") {
    return {
      label: "待确认",
      value: metrics.missingUnknownCount,
      hint: "空值 / 缺失需回看来源，不直接等于无配置",
    };
  }
  if (deltaFilter === "INFERRED") {
    return {
      label: "规则推断",
      value: metrics.inferredDifferenceCount,
      hint: `当前范围${differenceLabel} ${metrics.confirmedDifferenceCount} ${itemUnit}`,
    };
  }
  if (deltaFilter === "MISSING_SOURCE") {
    return {
      label: "来源问题",
      value: sourceMetrics.sourceIssueCellCount,
      hint: simpleRowMode
        ? `缺源值 ${sourceMetrics.missingSourceValueCount}，缺值 ${sourceMetrics.missingValueCellCount}；当前范围 ${rowCount} ${configUnit}，需补来源证据`
        : `缺源值 ${sourceMetrics.missingSourceValueCount}，缺值 ${sourceMetrics.missingValueCellCount}；当前范围 ${rowCount} ${configUnit}需补 evidence`,
    };
  }
  if (deltaFilter === "MERGED_SOURCE") {
    return {
      label: "合并格展开",
      value: sourceMetrics.mergedCellCount,
      hint: simpleRowMode
        ? `当前范围 ${rowCount} ${configUnit}，来自合并单元格`
        : `当前范围 ${rowCount} ${configUnit}来自合并单元格`,
    };
  }
  if (deltaFilter === "VALUE_CHANGED") {
    return {
      label: "值变化",
      value: metrics.valueChangedCount,
      hint: `当前范围${differenceLabel} ${metrics.confirmedDifferenceCount} ${itemUnit}`,
    };
  }
  if (deltaFilter === "ADDED") {
    return {
      label: "新增配置",
      value: metrics.confirmedDifferenceCount,
      hint: `当前范围含规则推断 ${metrics.inferredDifferenceCount} ${itemUnit}`,
    };
  }
  if (deltaFilter === "REMOVED") {
    return {
      label: "减少配置",
      value: metrics.confirmedDifferenceCount,
      hint: `当前范围含规则推断 ${metrics.inferredDifferenceCount} ${itemUnit}`,
    };
  }
  if (deltaFilter === "OPTIONAL_CHANGED") {
    return {
      label: "选装变化",
      value: metrics.confirmedDifferenceCount,
      hint: `当前范围含规则推断 ${metrics.inferredDifferenceCount} ${itemUnit}`,
    };
  }
  return {
    label: differenceLabel,
    value: metrics.confirmedDifferenceCount,
    hint: `当前范围含规则推断 ${metrics.inferredDifferenceCount} ${itemUnit}`,
  };
}

function compareDataFromDigestGroup(
  group: EngineeringConfigSourceDigestGroup,
  fileName: string,
): CompareResponse {
  const rows: CompareRow[] = group.rows;
  const trims: CompareTrimItem[] = group.trims.map((trim) => {
    const hasMaterialNo = Boolean(trim.materialNo || trim.hasMaterialNo);
    return {
      trimId: trim.trimId,
      fullTrimName: trim.fullTrimName || trim.trimName,
      brand: "本品资料",
      modelName: trim.modelName || group.modelName,
      trimName: trim.trimName,
      market: trim.market ?? null,
      country: trim.country ?? trim.market ?? null,
      modelYear: null,
      energyType: null,
      drivetrain: null,
      engine: null,
      vehicleCode: trim.materialNo ?? null,
      materialNo: trim.materialNo ?? null,
      identityKey: trim.materialNo ?? trim.trimId,
      salesVersion: trim.salesVersion ?? trim.trimName,
      sourceUploadId: null,
      sourceFileName: fileName,
      sourceFilePath: null,
      importStatus: trim.sourceStatus === "cancelled" ? "cancelled" : "digest_ready",
      hasMaterialNo,
      dataOrigin: trim.dataOrigin ?? (hasMaterialNo ? "own_catalog" : "external_or_scraped"),
      profile: trim.profile ?? null,
      msrp: null,
      targetPrice: null,
    };
  });
  return {
    trims,
    rows,
    groups: buildCompareGroups(rows),
    summary: {
      ...group.summary,
      shownFeatures: rows.length,
      categoryCounts: buildCategoryCounts(rows),
      differenceCategories: visibleDifferenceCategories(rows),
    },
    totalFeatures: group.summary.totalFeatures,
    shownFeatures: rows.length,
  };
}

function stringFromRecord(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

function nullableStringFromRecord(record: Record<string, unknown>, key: string): string | null | undefined {
  if (!(key in record)) return undefined;
  const value = record[key];
  return value === null || typeof value === "string" ? value : undefined;
}

function numberFromRecord(record: Record<string, unknown>, key: string): number | undefined {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function saveResultFromApiResponse(
  response: Record<string, unknown>,
  payload: ConfigComparisonCellSavePayload,
): ConfigComparisonCellSaveResult {
  return {
    valueId: stringFromRecord(response, "valueId") ?? payload.valueId ?? "",
    rawValue: stringFromRecord(response, "rawValue") ?? payload.rawValue,
    normalizedValue: nullableStringFromRecord(response, "normalizedValue"),
    availability: stringFromRecord(response, "availability") as AvailabilityState | undefined,
    valueState: stringFromRecord(response, "valueState") as ConfigValueState | undefined,
    displayValue: nullableStringFromRecord(response, "displayValue"),
    manualOverride: response.manualOverride !== false,
    version: numberFromRecord(response, "version") ?? (payload.expectedVersion != null ? payload.expectedVersion + 1 : 1),
    unchanged: response.unchanged === true,
  };
}

function applySavedCompareCell(
  data: CompareResponse,
  payload: ConfigComparisonCellSavePayload,
  result: ConfigComparisonCellSaveResult,
): CompareResponse {
  if (result.unchanged) return data;
  const updateRow = (row: CompareRow): CompareRow => ({
    ...row,
    values: row.values.map((cell, index) => {
      const targetTrimId = data.trims[index]?.trimId;
      const matchesExisting = Boolean(payload.valueId && cell?.valueId === payload.valueId);
      const matchesCreated = !payload.valueId
        && !cell
        && row.featureCode === payload.row.featureCode
        && targetTrimId === payload.trim.trimId;
      if (!matchesExisting && !matchesCreated) return cell;
      return {
        ...(cell ?? {
          valueId: result.valueId ?? "",
          normalizedValue: null,
          availability: "UNKNOWN" as AvailabilityState,
          unit: null,
          inferred: false,
          inferenceReason: null,
          confidence: null,
          source: null,
          manualOverride: true,
        }),
        valueId: result.valueId ?? cell?.valueId ?? "",
        rawValue: result.rawValue ?? payload.rawValue,
        normalizedValue: result.normalizedValue !== undefined ? result.normalizedValue : cell?.normalizedValue ?? null,
        availability: result.availability ?? cell?.availability ?? "UNKNOWN",
        valueState: result.valueState ?? cell?.valueState,
        displayValue: result.displayValue !== undefined ? result.displayValue : null,
        version: result.version ?? (payload.expectedVersion != null ? payload.expectedVersion + 1 : 1),
        manualOverride: result.manualOverride ?? true,
        inferred: false,
        inferenceReason: null,
        confidence: null,
        source: null,
      };
    }),
  });
  const rows = data.rows.map(updateRow);
  return {
    ...data,
    rows,
    groups: data.groups?.map((group) => ({
      ...group,
      items: group.items.map(updateRow),
    })),
  };
}

function compareDataBusinessSummaryExportKey(data: CompareResponse | null): string {
  if (!data) return "empty";
  const trimKey = data.trims.map((trim) => trim.trimId).join(",");
  const rowKey = data.rows.map((row) => {
    const valueKey = row.values.map((cell) => (
      cell
        ? [
            cell.valueId,
            cell.version,
            cell.rawValue,
            cell.displayValue,
            cell.availability,
            cell.inferred ? "inferred" : "raw",
          ].join("~")
        : "missing"
    )).join("|");
    return `${row.featureCode}:${row.comparisonType}:${valueKey}`;
  }).join(";");
  return `${trimKey}::${rowKey}`;
}

function categoryAnchorId(category: string): string {
  const normalized = category
    .trim()
    .toLowerCase()
    .replace(/[^0-9a-z\u4e00-\u9fff]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `config-category-${normalized || "uncategorized"}`;
}

function scrollToCategory(category: string): void {
  document.getElementById(categoryAnchorId(category))?.scrollIntoView({ behavior: "smooth", block: "start" });
}

function exportStatusEquals(
  current: ConfigComparisonTableExportStatus | null,
  next: ConfigComparisonTableExportStatus | null,
): boolean {
  if (current === next) return true;
  if (!current || !next) return false;
  return current.canExport === next.canExport
    && current.copyLabel === next.copyLabel
    && current.exportingPdf === next.exportingPdf
    && current.exportingXlsx === next.exportingXlsx
    && current.rangeLabel === next.rangeLabel
    && current.rowCount === next.rowCount
    && current.trimCount === next.trimCount;
}

export function ProductConfigComparePage() {
  const auth = useOptionalAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [filters, setFilters] = useState<TrimFilters>(() => initialTrimFiltersFromSearchParams(searchParams));
  const [trims, setTrims] = useState<VehicleTrimItem[]>([]);
  const [trimOptionPool, setTrimOptionPool] = useState<VehicleTrimItem[]>([]);
  const [compareIds, setCompareIds] = useState<string[]>(() => parseTrimIdsParam(searchParams.get("trimIds") || searchParams.get("trim_ids")));
  const [compareVersionScope, setCompareVersionScope] = useState<EngineeringConfigVersionScope>(() => (
    searchParams.get("versionScope") === "latest" ? "latest" : "published"
  ));
  const [baseTrimId, setBaseTrimId] = useState<string | null>(() => searchParams.get("baseTrimId"));
  const [compareData, setCompareData] = useState<CompareResponse | null>(null);
  const [localDigest, setLocalDigest] = useState<EngineeringConfigSourceDigest | null>(null);
  const [localDigestLoading, setLocalDigestLoading] = useState(false);
  const [localDigestError, setLocalDigestError] = useState<string | null>(null);
  const [activeDigestGroupId, setActiveDigestGroupId] = useState<string | null>(null);
  const [localDigestSampleRequested, setLocalDigestSampleRequested] = useState(false);
  const [trimLibraryTotalRows, setTrimLibraryTotalRows] = useState(0);
  const [activeCategoryFilter, setActiveCategoryFilter] = useState<string | null>(null);
  const [activeDeltaFilter, setActiveDeltaFilter] = useState<ConfigComparisonDeltaFilter>("ALL");
  const [activeTableSearch, setActiveTableSearch] = useState("");
  const [activeTargetTrimId, setActiveTargetTrimId] = useState<string | null>(null);
  const [focusedFeatureCode, setFocusedFeatureCode] = useState<string | null>(null);
  const [focusedFeatureRequestKey, setFocusedFeatureRequestKey] = useState(0);
  const [pendingDraftReviewFocus, setPendingDraftReviewFocus] = useState<SourceDigestDraftReviewFocus | null>(null);
  const [sourceDigestDraftReviewFocus, setSourceDigestDraftReviewFocus] = useState<SourceDigestDraftReviewFocus | null>(null);
  const [sourceDigestDraftSuccess, setSourceDigestDraftSuccess] = useState<SourceDigestDraftSuccessSummary | null>(null);
  const [businessSummaryExportItems, setBusinessSummaryExportItems] = useState<EngineeringConfigBusinessSummaryItem[]>([]);
  const [businessSummaryExportUsage, setBusinessSummaryExportUsage] = useState<EngineeringConfigBusinessSummaryUsage | null>(null);
  const configTableExportActionsRef = useRef<ConfigComparisonTableExportActions | null>(null);
  const compareRequestIdRef = useRef(0);
  const [configTableExportStatus, setConfigTableExportStatus] = useState<ConfigComparisonTableExportStatus | null>(null);
  const [configTableExportStatusKey, setConfigTableExportStatusKey] = useState<string | null>(null);
  const [deckExportingFormat, setDeckExportingFormat] = useState<"xlsx" | "pdf" | null>(null);
  const [deckExportFeedback, setDeckExportFeedback] = useState<string | null>(null);
  const [evidenceSelection, setEvidenceSelection] = useState<SourceEvidenceSelection | null>(null);
  const [sourceContextOverride, setSourceContextOverride] = useState<EngineeringConfigSourceContext | null>(null);
  const [sourceDigestSearchQuery, setSourceDigestSearchQuery] = useState("");
  const [librarySourceSnapshotMatches, setLibrarySourceSnapshotMatches] = useState<EngineeringConfigSourceSnapshot[]>([]);
  const [librarySourceDigestCandidates, setLibrarySourceDigestCandidates] = useState<SourceDigestGroupCandidate[]>([]);
  const [librarySourceDigestTotalRows, setLibrarySourceDigestTotalRows] = useState(0);
  const [librarySourceDigestLoading, setLibrarySourceDigestLoading] = useState(false);
  const [librarySourceDigestError, setLibrarySourceDigestError] = useState<string | null>(null);
  const [sourceDigestLibraryRefreshKey, setSourceDigestLibraryRefreshKey] = useState(0);
  const [focusedSourceDigestSourceId, setFocusedSourceDigestSourceId] = useState<string | null>(null);
  const [pendingSourceDigestFocusSourceId, setPendingSourceDigestFocusSourceId] = useState<string | null>(null);
  const [sourceDigestQualityFilter, setSourceDigestQualityFilter] = useState<SourceDigestQualityFilterKey>("all");
  const [sourceDigestBrowseExpanded, setSourceDigestBrowseExpanded] = useState(false);
  const [sourceDigestDetailBrowserOpen, setSourceDigestDetailBrowserOpen] = useState(false);
  const [sourceDigestTrimSelections, setSourceDigestTrimSelections] = useState<SourceDigestTrimSelectionMap>({});
  const [directSourceDigestTrimSelections, setDirectSourceDigestTrimSelections] = useState<SourceDigestTrimSelectionMap>({});
  const [directSourceDigestPendingCandidates, setDirectSourceDigestPendingCandidates] = useState<SourceDigestPendingCandidateMap>({});
  const [sourceDigestTrimIdentityDrafts, setSourceDigestTrimIdentityDrafts] = useState<SourceDigestTrimIdentityDraftMap>({});
  const [sourceDigestReviewFocuses, setSourceDigestReviewFocuses] = useState<SourceDigestReviewFocusMap>({});
  const [sourceDigestDraftActionKey, setSourceDigestDraftActionKey] = useState<string | null>(null);
  const [sourceDigestDraftFeedback, setSourceDigestDraftFeedback] = useState<string | null>(null);
  const [summaryMode, setSummaryMode] = useState<BusinessSummaryMode>(initialProductConfigSummaryMode);
  const [businessSummaryPanelReady, setBusinessSummaryPanelReady] = useState(false);
  const [businessSummaryReadiness, setBusinessSummaryReadiness] = useState<EngineeringConfigBusinessSummaryReadiness | null>(null);
  const [businessSummaryReadinessError, setBusinessSummaryReadinessError] = useState<string | null>(null);
  const businessSummaryReadinessRequestedRef = useRef(false);
  const [drawerAiReadinessOpen, setDrawerAiReadinessOpen] = useState(false);
  const [simpleSelectedStripOpen, setSimpleSelectedStripOpen] = useState(false);
  const [editModeEnabled, setEditModeEnabled] = useState(false);
  const [editAuditReason, setEditAuditReason] = useState("配置核对更正");
  const [trimIdentityEditId, setTrimIdentityEditId] = useState<string | null>(null);
  const [trimIdentityDraft, setTrimIdentityDraft] = useState<TrimIdentityDraft>(() => trimIdentityDraftFromTrim(null));
  const [trimIdentitySaving, setTrimIdentitySaving] = useState(false);
  const [trimIdentityFeedback, setTrimIdentityFeedback] = useState<string | null>(null);
  const [featureCatalogMappingFile, setFeatureCatalogMappingFile] = useState<File | null>(null);
  const [featureCatalogMappingUploading, setFeatureCatalogMappingUploading] = useState(false);
  const [featureCatalogMappingFeedback, setFeatureCatalogMappingFeedback] = useState<string | null>(null);
  const [featureCatalogMappingSummary, setFeatureCatalogMappingSummary] = useState<FeatureCatalogMappingUploadSummary | null>(null);
  const [featureCatalogMappingAudit, setFeatureCatalogMappingAudit] = useState<FeatureCatalogMappingUploadAudit | null>(null);
  const [directTrimPickerValue, setDirectTrimPickerValue] = useState("");
  const [directPickerFocusRequest, setDirectPickerFocusRequest] = useState(0);
  const [directPickerResetKey, setDirectPickerResetKey] = useState(0);
  const [sourceDigestDirectPickerValue, setSourceDigestDirectPickerValue] = useState("");
  const [directTrimSearchQuery, setDirectTrimSearchQuery] = useState("");
  const [directTrimSearchResults, setDirectTrimSearchResults] = useState<VehicleTrimItem[]>([]);
  const [directTrimSearchTotalRows, setDirectTrimSearchTotalRows] = useState(0);
  const [directTrimSearchLoading, setDirectTrimSearchLoading] = useState(false);
  const [directTrimSearchError, setDirectTrimSearchError] = useState<string | null>(null);
  const [trimTrashActionId, setTrimTrashActionId] = useState<string | null>(null);
  const [trimTrashFeedback, setTrimTrashFeedback] = useState<string | null>(null);
  const [trimTrashItems, setTrimTrashItems] = useState<VehicleTrimItem[]>([]);
  const [trimTrashLoading, setTrimTrashLoading] = useState(false);
  const [trimTrashClearConfirmKey, setTrimTrashClearConfirmKey] = useState<string | null>(null);
  const [sourceTrashActionId, setSourceTrashActionId] = useState<string | null>(null);
  const [sourceTrashFeedback, setSourceTrashFeedback] = useState<string | null>(null);
  const [sourceTrashItems, setSourceTrashItems] = useState<EngineeringConfigSourceSnapshot[]>([]);
  const [sourceTrashLoading, setSourceTrashLoading] = useState(false);
  const [sourceTrashClearConfirmKey, setSourceTrashClearConfirmKey] = useState<string | null>(null);
  const [controlOpen, setControlOpen] = useState(false);
  const [activePanel, setActivePanel] = useState<ProductConfigPanel>("filters");
  const [simpleAdvancedSearchOpen, setSimpleAdvancedSearchOpen] = useState(false);
  const [simpleDirectDiagnosticsOpen, setSimpleDirectDiagnosticsOpen] = useState(false);
  const [sourceContextBindingPromptOpen, setSourceContextBindingPromptOpen] = useState(false);
  const [trimsLoading, setTrimsLoading] = useState(false);
  const [competitorRecommendations, setCompetitorRecommendations] = useState<EngineeringConfigCompetitorRecommendation[]>([]);
  const [competitorRecommendationSource, setCompetitorRecommendationSource] = useState<CompetitorRecommendationSource | null>(null);
  const [competitorRecommendationsLoading, setCompetitorRecommendationsLoading] = useState(false);
  const [competitorRecommendationNote, setCompetitorRecommendationNote] = useState<string | null>(null);
  const [compareLoading, setCompareLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const externalPrefilterActive = compareIds.length === 0 && hasTrimFilterSearchParams(searchParams);
  const directConfigSearchPanelActive = controlOpen && activePanel === "filters";
  const simpleModeActive = summaryMode === "simple";
  const shouldLoadTrimLibrary = externalPrefilterActive || (
    directConfigSearchPanelActive
    && (!simpleModeActive || simpleAdvancedSearchOpen)
  );
  const shouldLoadLocalDigestSample = localDigestSampleRequested && compareIds.length === 0 && !externalPrefilterActive;
  const advancedConfigSearchActive = directConfigSearchPanelActive && (!simpleModeActive || simpleAdvancedSearchOpen);
  const shouldShowDrawerAiReadiness = !simpleModeActive || drawerAiReadinessOpen;
  const shouldLoadBusinessSummaryReadiness = controlOpen && activePanel === "display" && shouldShowDrawerAiReadiness;
  const sourceDigestFilters = sourceDigestFiltersForContext(filters, sourceContextOverride);
  const sourceDigestLookupQuery = sourceDigestLibraryLookupQuery(sourceDigestFilters, sourceDigestSearchQuery);
  const sourceDigestHasLookupQuery = sourceDigestLookupQuery.length >= 2;
  const sourceDigestLibrarySearchActive = sourceDigestHasLookupQuery || Boolean(focusedSourceDigestSourceId);

  useEffect(() => {
    rememberProductConfigSummaryMode(summaryMode);
  }, [summaryMode]);

  useEffect(() => {
    if (!shouldLoadBusinessSummaryReadiness || businessSummaryReadinessRequestedRef.current) return;
    let active = true;
    businessSummaryReadinessRequestedRef.current = true;
    api.getEngineeringConfigBusinessSummaryReadiness()
      .then((readiness) => {
        if (!active) return;
        setBusinessSummaryReadiness(readiness);
        setBusinessSummaryReadinessError(null);
      })
      .catch((err: unknown) => {
        if (!active) return;
        setBusinessSummaryReadiness(null);
        setBusinessSummaryReadinessError(err instanceof Error ? err.message : "AI 摘要状态加载失败");
      });
    return () => {
      active = false;
    };
  }, [shouldLoadBusinessSummaryReadiness]);

  const loadTrims = useCallback(async () => {
    setTrimsLoading(true);
    setError(null);
    try {
      const result = await api.listEngineeringConfigTrims({
        brand: filters.brand || undefined,
        model_name: filters.model || undefined,
        market: filters.market || undefined,
        model_year: filters.modelYear || undefined,
        trim_name: filters.trim || undefined,
        energy_type: filters.powertrain || undefined,
        source: filters.source || undefined,
        q: filters.keyword || undefined,
        limit: 200,
      });
      const items = (result.items || []) as unknown as VehicleTrimItem[];
      setTrims(items);
      setTrimLibraryTotalRows(result.rows ?? items.length);
      setTrimOptionPool((previous) => mergeTrimOptionPool(previous, items));
    } catch (err) {
      setTrimLibraryTotalRows(0);
      setError(err instanceof Error ? err.message : "车型配置加载失败");
    } finally {
      setTrimsLoading(false);
    }
  }, [filters]);

  useEffect(() => {
    if (!shouldLoadTrimLibrary) {
      setTrimsLoading(false);
      return;
    }
    void loadTrims();
  }, [loadTrims, shouldLoadTrimLibrary]);

  useEffect(() => {
    const query = directTrimSearchQuery.trim();
    if (!directConfigSearchPanelActive) {
      setDirectTrimSearchLoading(false);
      return;
    }
    if (query.length < 2) {
      setDirectTrimSearchResults([]);
      setDirectTrimSearchTotalRows(0);
      setDirectTrimSearchLoading(false);
      setDirectTrimSearchError(null);
      return;
    }
    let disposed = false;
    setDirectTrimSearchLoading(true);
    setDirectTrimSearchError(null);
    const timer = window.setTimeout(() => {
      api.listEngineeringConfigTrims({
        brand: filters.brand || undefined,
        model_name: filters.model || undefined,
        market: filters.market || undefined,
        model_year: filters.modelYear || undefined,
        trim_name: filters.trim || undefined,
        energy_type: filters.powertrain || undefined,
        source: filters.source || undefined,
        q: query,
        limit: 80,
      })
        .then((result) => {
          if (disposed) return;
          const items = (result.items || []) as unknown as VehicleTrimItem[];
          setDirectTrimSearchResults(items);
          setDirectTrimSearchTotalRows(result.rows ?? items.length);
          setTrimOptionPool((previous) => mergeTrimOptionPool(previous, items));
        })
        .catch((err) => {
          if (disposed) return;
          setDirectTrimSearchResults([]);
          setDirectTrimSearchTotalRows(0);
          setDirectTrimSearchError(err instanceof Error ? err.message : "配置列库搜索失败");
        })
        .finally(() => {
          if (!disposed) setDirectTrimSearchLoading(false);
        });
    }, 220);
    return () => {
      disposed = true;
      window.clearTimeout(timer);
    };
  }, [
    directTrimSearchQuery,
    directConfigSearchPanelActive,
    filters.brand,
    filters.market,
    filters.model,
    filters.modelYear,
    filters.powertrain,
    filters.source,
    filters.trim,
  ]);

  useEffect(() => {
    setFocusedSourceDigestSourceId((current) => {
      if (!current) return null;
      const focusedCandidates = librarySourceDigestCandidates.filter((candidate) => candidate.sourceId === current);
      if (focusedCandidates.length === 0) return null;
      if (!sourceDigestLookupQuery) return current;
      return focusedCandidates.some((candidate) => sourceDigestGroupMatchesSearch(candidate, sourceDigestLookupQuery))
        ? current
        : null;
    });
  }, [librarySourceDigestCandidates, sourceDigestLookupQuery]);

  useEffect(() => {
    if (!pendingSourceDigestFocusSourceId) return;
    const hasCandidate = librarySourceDigestCandidates.some((candidate) => candidate.sourceId === pendingSourceDigestFocusSourceId);
    const hasSnapshot = librarySourceSnapshotMatches.some((snapshot) => snapshot.sourceId === pendingSourceDigestFocusSourceId);
    if (!hasCandidate && !hasSnapshot) return;
    setFocusedSourceDigestSourceId(pendingSourceDigestFocusSourceId);
    setPendingSourceDigestFocusSourceId(null);
  }, [librarySourceDigestCandidates, librarySourceSnapshotMatches, pendingSourceDigestFocusSourceId]);

  useEffect(() => {
    if (!controlOpen || (activePanel !== "filters" && activePanel !== "source")) return;
    if (!sourceDigestLibrarySearchActive) {
      setLibrarySourceSnapshotMatches([]);
      setLibrarySourceDigestCandidates([]);
      setLibrarySourceDigestTotalRows(0);
      setLibrarySourceDigestLoading(false);
      setLibrarySourceDigestError(null);
      return;
    }
    if (!sourceDigestHasLookupQuery) {
      setLibrarySourceDigestLoading(false);
      setLibrarySourceDigestError(null);
      return;
    }
    let disposed = false;
    const timer = window.setTimeout(() => {
      setLibrarySourceDigestLoading(true);
      setLibrarySourceDigestError(null);
      api.listEngineeringConfigSourceSnapshots({
        limit: 20,
        brand: sourceDigestFilters.brand || null,
        country: sourceDigestFilters.market || null,
        modelYear: sourceDigestFilters.modelYear || null,
        powertrain: sourceDigestFilters.powertrain || null,
        segment: sourceDigestFilters.segment || null,
        q: sourceDigestLookupQuery,
      })
        .then(async (result) => {
          const sortedSnapshots = sortSourceDigestSnapshots(result.items, sourceDigestFilters, sourceDigestLookupQuery);
          const snapshots = sortedSnapshots.slice(0, SOURCE_DIGEST_SNAPSHOT_MATCH_LIMIT);
          const detailLimit = activePanel === "source"
            ? sourceDigestDetailBrowserOpen ? SOURCE_DIGEST_SOURCE_PANEL_DETAIL_LIMIT : SOURCE_DIGEST_DIRECT_DETAIL_LIMIT
            : SOURCE_DIGEST_DIRECT_DETAIL_LIMIT;
          const detailSnapshots = snapshots.filter(sourceSnapshotHasComparableDigest).slice(0, detailLimit);
          if (!disposed) {
            setLibrarySourceDigestError(null);
            setLibrarySourceSnapshotMatches(snapshots);
            setLibrarySourceDigestTotalRows(result.rows);
          }
          if (detailSnapshots.length === 0) {
            if (!disposed) setLibrarySourceDigestCandidates([]);
            return;
          }
          const details = await Promise.all(detailSnapshots.map(async (snapshot) => {
            if (snapshot.sourceDigest) return snapshot;
            const detail = await api.getEngineeringConfigSourceSnapshot(snapshot.sourceId);
            return {
              ...detail,
              sourceSearchMatches: detail.sourceSearchMatches?.length
                ? detail.sourceSearchMatches
                : snapshot.sourceSearchMatches,
            };
          }));
          if (disposed) return;
          const byKey = new Map<string, SourceDigestGroupCandidate>();
          details.flatMap(sourceDigestCandidatesFromSnapshot).forEach((candidate) => {
            byKey.set(sourceDigestCandidateKey(candidate), candidate);
          });
          setLibrarySourceDigestCandidates(Array.from(byKey.values()).slice(0, SOURCE_DIGEST_GROUP_CANDIDATE_LIMIT));
          setLibrarySourceDigestTotalRows(result.rows);
        })
        .catch((err) => {
          if (disposed) return;
          setLibrarySourceSnapshotMatches([]);
          setLibrarySourceDigestCandidates([]);
          setLibrarySourceDigestTotalRows(0);
          setLibrarySourceDigestError(err instanceof Error ? err.message : "来源库 digest 组加载失败");
        })
        .finally(() => {
          if (!disposed) setLibrarySourceDigestLoading(false);
        });
    }, 220);
    return () => {
      disposed = true;
      window.clearTimeout(timer);
    };
  }, [
    activePanel,
    controlOpen,
    focusedSourceDigestSourceId,
    sourceDigestFilters.brand,
    sourceDigestFilters.keyword,
    sourceDigestFilters.market,
    sourceDigestFilters.model,
    sourceDigestFilters.modelYear,
    sourceDigestFilters.powertrain,
    sourceDigestFilters.segment,
    sourceDigestFilters.source,
    sourceDigestFilters.trim,
    sourceDigestHasLookupQuery,
    sourceDigestLibrarySearchActive,
    sourceDigestLibraryRefreshKey,
    sourceDigestDetailBrowserOpen,
    sourceDigestLookupQuery,
  ]);

  useEffect(() => {
    if (!controlOpen || activePanel !== "filters" || directPickerFocusRequest <= 0) return;
    const focusDirectPicker = () => {
      const input = document.querySelector<HTMLInputElement>(`input[aria-label="${DIRECT_CONFIG_COLUMN_PICKER_LABEL}"]`);
      input?.focus();
      input?.click();
    };
    if (typeof window.requestAnimationFrame === "function") {
      const handle = window.requestAnimationFrame(focusDirectPicker);
      return () => window.cancelAnimationFrame(handle);
    }
    const handle = window.setTimeout(focusDirectPicker, 0);
    return () => window.clearTimeout(handle);
  }, [activePanel, controlOpen, directPickerFocusRequest]);

  useEffect(() => {
    if (!shouldLoadLocalDigestSample) {
      setLocalDigest(null);
      setLocalDigestLoading(false);
      setLocalDigestError(null);
      setActiveDigestGroupId(null);
      return;
    }
    let disposed = false;
    setLocalDigestLoading(true);
    const cancelDeferredLoad = scheduleLocalDigestSampleLoad(() => {
      api.getEngineeringConfigLocalWorkbookDigest(LOCAL_CONFIG_WORKBOOK_FILE)
        .then((digest) => {
          if (disposed) return;
          setLocalDigest(digest);
          setLocalDigestError(null);
          const firstComparableGroups = digest.compareGroups.filter((group) => group.trimCount >= 2);
          const firstGroup = firstComparableGroups[0];
          if (firstGroup) {
            setActiveDigestGroupId((current) => current ?? localDigestGroupKey(firstGroup, 0));
            setBaseTrimId((current) => current ?? firstGroup.trims[0]?.trimId ?? null);
          }
        })
        .catch((err) => {
          if (disposed) return;
          setLocalDigestError(err instanceof Error ? err.message : "本地配置表 digest 加载失败");
        })
        .finally(() => {
          if (!disposed) setLocalDigestLoading(false);
        });
    });
    return () => {
      disposed = true;
      cancelDeferredLoad();
    };
  }, [shouldLoadLocalDigestSample]);

  useEffect(() => {
    const urlIds = parseTrimIdsParam(searchParams.get("trimIds") || searchParams.get("trim_ids"));
    if (urlIds.join(",") !== compareIds.join(",")) setCompareIds(urlIds);
  }, [searchParams]);

  useEffect(() => {
    const currentIds = parseTrimIdsParam(searchParams.get("trimIds") || searchParams.get("trim_ids")).join(",");
    const nextIds = compareIds.join(",");
    const next = new URLSearchParams(searchParams);
    let changed = false;
    if (currentIds !== nextIds) {
      next.delete("trim_ids");
      if (nextIds) next.set("trimIds", nextIds);
      else next.delete("trimIds");
      changed = true;
    }
    const currentBase = searchParams.get("baseTrimId") || "";
    const nextBase = compareIds.length > 0 ? baseTrimId || "" : "";
    if (currentBase !== nextBase) {
      if (nextBase) next.set("baseTrimId", nextBase);
      else next.delete("baseTrimId");
      changed = true;
    }
    const currentVersionScope = searchParams.get("versionScope") === "latest" ? "latest" : "published";
    if (currentVersionScope !== compareVersionScope) {
      if (compareVersionScope === "latest") next.set("versionScope", "latest");
      else next.delete("versionScope");
      changed = true;
    }
    if (changed) setSearchParams(next, { replace: true });
  }, [baseTrimId, compareIds, compareVersionScope, searchParams, setSearchParams]);

  useEffect(() => {
    if (compareIds.length === 0) {
      if (!activeDigestGroupId) setBaseTrimId(null);
      return;
    }
    if (!baseTrimId || !compareIds.includes(baseTrimId)) {
      setBaseTrimId(compareIds[0]);
    }
  }, [activeDigestGroupId, baseTrimId, compareIds]);

  const doCompare = useCallback(async () => {
    const requestId = compareRequestIdRef.current + 1;
    compareRequestIdRef.current = requestId;
    if (compareIds.length < 2) {
      setCompareData(null);
      setCompareLoading(false);
      return;
    }
    setCompareLoading(true);
    setError(null);
    try {
      const result = compareVersionScope === "latest"
        ? await api.compareEngineeringConfigTrims(compareIds, false, "latest")
        : await api.compareEngineeringConfigTrims(compareIds, false);
      if (compareRequestIdRef.current !== requestId) return;
      setCompareData(result as unknown as CompareResponse);
    } catch (err) {
      if (compareRequestIdRef.current !== requestId) return;
      setError(err instanceof Error ? err.message : "配置对比失败");
    } finally {
      if (compareRequestIdRef.current === requestId) setCompareLoading(false);
    }
  }, [compareIds, compareVersionScope]);

  useEffect(() => {
    void doCompare();
  }, [doCompare]);

  useEffect(() => {
    if (!compareData || !pendingDraftReviewFocus) return;
    const focusedRow = compareData.rows.find((row) => row.featureCode === pendingDraftReviewFocus.featureCode);
    setPendingDraftReviewFocus(null);
    if (!focusedRow) return;
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
    setActiveDeltaFilter("ALL");
    setActiveCategoryFilter(focusedRow.category);
    setFocusedFeatureCode(focusedRow.featureCode);
    setFocusedFeatureRequestKey((value) => value + 1);
    scrollToCompareTable();
  }, [compareData, pendingDraftReviewFocus]);

  const saveCompareCellValue = useCallback(async (
    payload: ConfigComparisonCellSavePayload,
  ): Promise<ConfigComparisonCellSaveResult> => {
    const response = payload.valueId
      ? await api.updateEngineeringConfigFeatureValue(payload.valueId, {
          raw_value: payload.rawValue,
          expected_version: payload.expectedVersion ?? 1,
          comment: editAuditReason.trim() || "配置核对更正",
        })
      : await api.createEngineeringConfigFeatureValue({
          trim_id: payload.trim.trimId,
          feature_id: payload.featureId ?? "",
          raw_value: payload.rawValue,
        });
    const result = saveResultFromApiResponse(response, payload);
    setCompareData((current) => (current ? applySavedCompareCell(current, payload, result) : current));
    return result;
  }, [editAuditReason]);

  const localComparableGroups = useMemo(
    () => localDigest?.compareGroups.filter((group) => group.trimCount >= 2) ?? [],
    [localDigest],
  );
  const localComparableGroupEntries = useMemo<LocalDigestGroupEntry[]>(
    () => localComparableGroups.map((group, index) => ({
      group,
      index,
      key: localDigestGroupKey(group, index),
    })),
    [localComparableGroups],
  );
  const activeDigestGroupEntry = useMemo(
    () => localComparableGroupEntries.find((entry) => entry.key === activeDigestGroupId) ?? null,
    [activeDigestGroupId, localComparableGroupEntries],
  );
  const activeDigestGroup = useMemo(
    () => activeDigestGroupEntry?.group ?? null,
    [activeDigestGroupEntry],
  );
  const activeDigestSelectedTrimIds = useMemo(() => {
    if (!activeDigestGroup || !activeDigestGroupEntry) return [];
    return normaliseSourceDigestTrimSelection(
      activeDigestGroup,
      sourceDigestTrimSelections[activeDigestGroupEntry.key],
    );
  }, [activeDigestGroup, activeDigestGroupEntry, sourceDigestTrimSelections]);
  const activeDigestSelectedGroup = useMemo(
    () => (activeDigestGroup ? sourceDigestGroupWithSelectedTrims(activeDigestGroup, activeDigestSelectedTrimIds) : null),
    [activeDigestGroup, activeDigestSelectedTrimIds],
  );
  const digestCompareData = useMemo(
    () => (activeDigestSelectedGroup ? compareDataFromDigestGroup(activeDigestSelectedGroup, localDigest?.fileName || LOCAL_CONFIG_WORKBOOK_FILE) : null),
    [activeDigestSelectedGroup, localDigest?.fileName],
  );
  const displayCompareData = compareData ?? digestCompareData;
  const businessSummaryDataKey = useMemo(
    () => compareDataBusinessSummaryExportKey(displayCompareData),
    [displayCompareData],
  );
  const businessSummaryExportScopeKey = [
    businessSummaryDataKey,
    baseTrimId ?? "",
    activeCategoryFilter ?? "",
    activeDeltaFilter,
    activeTableSearch,
    activeTargetTrimId ?? "",
  ].join("::");
  const digestModeActive = Boolean(!compareData && digestCompareData);
  const compareFactSource = useMemo<EngineeringConfigCompareFactRequest["factSource"]>(() => (
    digestModeActive && activeDigestSelectedGroup
      ? {
          kind: "local_workbook_digest",
          fileName: localDigest?.fileName || LOCAL_CONFIG_WORKBOOK_FILE,
          groupId: activeDigestSelectedGroup.groupId,
        }
      : undefined
  ), [activeDigestSelectedGroup, digestModeActive, localDigest?.fileName]);
  const configTableReadyForBusinessSummary = (
    summaryMode === "expert"
    || (
      configTableExportStatusKey === businessSummaryDataKey
      && configTableExportStatus !== null
    )
  );
  const businessSummaryCompareStable = Boolean(displayCompareData && !compareLoading);

  const handleConfigTableExportStatusChange = useCallback((status: ConfigComparisonTableExportStatus | null): void => {
    setConfigTableExportStatus((current) => (
      exportStatusEquals(current, status) ? current : status
    ));
    setConfigTableExportStatusKey(status ? businessSummaryDataKey : null);
  }, [businessSummaryDataKey]);

  useEffect(() => {
    setConfigTableExportStatus(null);
    setConfigTableExportStatusKey(null);
  }, [businessSummaryDataKey]);

  useEffect(() => {
    setBusinessSummaryExportItems([]);
    setBusinessSummaryExportUsage(null);
    setDeckExportFeedback(null);
  }, [businessSummaryExportScopeKey]);

  useEffect(() => {
    if (!businessSummaryCompareStable) {
      setBusinessSummaryPanelReady(false);
      return;
    }
    if (summaryMode === "expert") {
      setBusinessSummaryPanelReady(true);
      return;
    }
    if (!configTableReadyForBusinessSummary) {
      setBusinessSummaryPanelReady(false);
      return;
    }
    setBusinessSummaryPanelReady(false);
    return scheduleBusinessSummaryPanelLoad(() => setBusinessSummaryPanelReady(true));
  }, [businessSummaryCompareStable, businessSummaryDataKey, configTableReadyForBusinessSummary, summaryMode]);

  useEffect(() => {
    if (
      activePanel === "source"
      && (focusedSourceDigestSourceId || sourceDigestQualityFilter !== "all")
    ) {
      setSourceDigestDetailBrowserOpen(true);
    }
  }, [activePanel, focusedSourceDigestSourceId, sourceDigestQualityFilter]);
  const recommendedConfigTrims = competitorRecommendations.flatMap((recommendation) => recommendation.trims);
  const compareTrimLookup = new Map<string, VehicleTrimItem | CompareTrimItem>();
  trimOptionPool.forEach((trim) => compareTrimLookup.set(trim.trimId, trim));
  trims.forEach((trim) => compareTrimLookup.set(trim.trimId, trim));
  recommendedConfigTrims.forEach((trim) => compareTrimLookup.set(trim.trimId, trim));
  displayCompareData?.trims.forEach((trim) => compareTrimLookup.set(trim.trimId, trim));
  const selectedTrimItems = compareIds.map((id) => compareTrimLookup.get(id));
  const selectedResolvedTrims = selectedTrimItems.filter((trim): trim is ComparableTrim => Boolean(trim));
  const selectedDisplayTrims = selectedResolvedTrims.length > 0
    ? selectedResolvedTrims
    : displayCompareData?.trims ?? [];
  const userCanEditValues = canEditEngineeringConfigValues(auth?.user?.role);
  const compareVersionEditable = Boolean(
    compareData
    && compareData.versionScope === "latest"
    && compareData.trims.every((trim) => (
      trim.configVersionStatus === "draft"
    )),
  );
  const editModeAvailable = compareVersionEditable && userCanEditValues;
  const valuesCanBeEdited = editModeAvailable && editModeEnabled;
  const directSelectedTrims = selectedResolvedTrims;
  const directSelectedDisplayTrims = digestModeActive && selectedDisplayTrims.length > 0
    ? selectedDisplayTrims
    : directSelectedTrims;
  const directSelectedReadOnly = digestModeActive && directSelectedDisplayTrims.length > 0;
  const selectedConfigPathGroups = buildSelectedConfigPathGroups(directSelectedDisplayTrims);
  const selectedTrashCountries = uniquePresent(selectedDisplayTrims.map((trim) => trim.market || trim.country));
  const trimTrashCountry = filters.market.trim() || (selectedTrashCountries.length === 1 ? selectedTrashCountries[0] : null);
  const trimTrashClearKey = trimTrashCountry && trimTrashItems.length > 0 ? `${trimTrashCountry}::${trimTrashItems.length}` : null;
  const trimTrashClearArmed = trimTrashClearKey !== null && trimTrashClearConfirmKey === trimTrashClearKey;
  const sourceMatchCountries = uniquePresent(librarySourceSnapshotMatches.map((snapshot) => (
    snapshot.relatedContext?.country || snapshot.relatedContext?.market
  )));
  const sourceTrashCountry = filters.market.trim() || (sourceMatchCountries.length === 1 ? sourceMatchCountries[0] : null);
  const sourceTrashClearKey = sourceTrashCountry && sourceTrashItems.length > 0 ? `${sourceTrashCountry}::${sourceTrashItems.length}` : null;
  const sourceTrashClearArmed = sourceTrashClearKey !== null && sourceTrashClearConfirmKey === sourceTrashClearKey;
  const baseTrim = selectedDisplayTrims.find((trim) => trim.trimId === baseTrimId) || selectedDisplayTrims[0] || null;
  const activeTargetTrim = selectedDisplayTrims.find((trim) => trim.trimId === activeTargetTrimId) || null;
  const trimIdentityEditTrim = selectedDisplayTrims.find((trim) => trim.trimId === trimIdentityEditId)
    || activeTargetTrim
    || baseTrim
    || selectedDisplayTrims[0]
    || null;
  const trimIdentityOptions = selectedDisplayTrims.map((trim) => ({
    value: trim.trimId,
    label: compareTrimLabel(trim),
    meta: [
      trim.market || trim.country || "市场待补",
      trim.modelYear ? `MY ${trim.modelYear}` : "年款待补",
      trimIdentityAnchorLabel(trim),
      trimSourceLabel(trim),
    ].join(" · "),
  }));
  const currentTrimIdentityDraft = trimIdentityDraftFromTrim(trimIdentityEditTrim);
  const trimIdentityDirty = Boolean(trimIdentityEditTrim && (
    trimIdentityDraft.brand !== currentTrimIdentityDraft.brand
    || trimIdentityDraft.modelName !== currentTrimIdentityDraft.modelName
    || trimIdentityDraft.trimName !== currentTrimIdentityDraft.trimName
    || trimIdentityDraft.fullTrimName !== currentTrimIdentityDraft.fullTrimName
    || trimIdentityDraft.market !== currentTrimIdentityDraft.market
    || trimIdentityDraft.modelYear !== currentTrimIdentityDraft.modelYear
    || trimIdentityDraft.energyType !== currentTrimIdentityDraft.energyType
    || trimIdentityDraft.drivetrain !== currentTrimIdentityDraft.drivetrain
    || trimIdentityDraft.engine !== currentTrimIdentityDraft.engine
    || trimIdentityDraft.materialNo !== currentTrimIdentityDraft.materialNo
    || trimIdentityDraft.vehicleCode !== currentTrimIdentityDraft.vehicleCode
    || trimIdentityDraft.identityKey !== currentTrimIdentityDraft.identityKey
  ));
  const comparisonScenario = buildComparisonScenario(selectedDisplayTrims);
  const identityNotes = buildComparisonIdentityNotes(selectedDisplayTrims);
  const missingSourceTargetTrim = (activeTargetTrim && trimSourceLabel(activeTargetTrim) === "来源待补" ? activeTargetTrim : null)
    ?? selectedDisplayTrims.find((trim) => trim.trimId !== baseTrim?.trimId && trimSourceLabel(trim) === "来源待补")
    ?? selectedDisplayTrims.find((trim) => trimSourceLabel(trim) === "来源待补")
    ?? null;
  const optionPool = trimOptionPool.length > 0 ? trimOptionPool : trims;
  const filterOptionColumns = uniqueComparableTrims([...optionPool, ...recommendedConfigTrims, ...selectedDisplayTrims]);
  const directTrimCandidates = directConfigSearchPanelActive
    ? uniqueVehicleTrims([...optionPool, ...trims, ...recommendedConfigTrims, ...directTrimSearchResults])
    : [];
  const selectedDisplayTrimIdentityKeys = new Set(selectedDisplayTrims.flatMap(trimSelectionIdentityKeys));
  const directDropdownSelectedTrimIds = uniquePresent([
    ...compareIds,
    ...directTrimCandidates
      .filter((trim) => trimSelectionIdentityKeys(trim).some((key) => selectedDisplayTrimIdentityKeys.has(key)))
      .map((trim) => trim.trimId),
  ]);
  const directTrimOptions = directConfigSearchPanelActive
    ? buildDirectTrimDropdownOptions(directTrimCandidates, directDropdownSelectedTrimIds, compareIds.length >= 4)
    : [];
  const directTrimLookup = new Map(directTrimCandidates.map((trim) => [trim.trimId, trim]));
  const directTrimSearchKeyword = directTrimSearchQuery.trim();
  const directFormalSearchActive = directConfigSearchPanelActive && directTrimSearchKeyword.length >= 2;
  const directFormalOptionCount = directConfigSearchPanelActive
    ? directFormalSearchActive ? directTrimSearchResults.length : directTrimOptions.length
    : 0;
  const directTrimSearchScope = directTrimSearchScopeLabel(filters);
  const directTrimSearchScopePrefix = directTrimSearchScope ? `当前筛选范围 ${directTrimSearchScope}；` : "";
  const directTrimSearchResultHint = directTrimSearchLoading
    ? "正在搜索配置列库..."
    : directTrimSearchError
      ? directTrimSearchError
      : directTrimSearchKeyword.length >= 2
        ? directTrimSearchTotalRows > 0
          ? `${directTrimSearchScopePrefix}配置列库命中 ${directTrimSearchTotalRows} 个已建配置列，当前拉取 ${directTrimSearchResults.length} 个。${directTrimSearchTotalRows > directTrimSearchResults.length ? "继续输入品牌 / 车型 / 物料号 / 来源缩小范围。" : "可直接从下拉加入对比。"}`
          : `${directTrimSearchScopePrefix}配置列库未命中“${directTrimSearchKeyword}”；可调整关键词或清空上方筛选，也可以上传 xlsx / PDF / 图片 / CSV / HTML / 价格单后解析入库。`
        : directTrimSearchScope
          ? `当前筛选范围 ${directTrimSearchScope}；可继续搜索物料号 / sales version / 来源。`
          : "可搜索品牌 / 车型 / 市场 / 物料号 / sales version / 来源，多来源入库后用这里快速定位配置列。";
  const trimLibraryResultHint = trimsLoading
    ? "正在加载库内配置列..."
    : trimLibraryTotalRows > 0
      ? `当前筛选命中 ${trimLibraryTotalRows} 个配置列，展示 ${trims.length} 个。${trimLibraryTotalRows > trims.length ? "结果较多时可继续选择车型 / 来源 / 物料号缩小范围。" : "可按品牌 / 车型展开加入对比。"}`
      : "当前筛选没有匹配的库内配置列，可清空筛选或上传来源文件。";
  const sourceDigestPanelActive = controlOpen && (activePanel === "filters" || activePanel === "source");
  const sourceDigestHasActiveFocus = focusedSourceDigestSourceId !== null || pendingSourceDigestFocusSourceId !== null;
  const sourceDigestHasActiveDraftSelection = (
    hasSourceDigestTrimSelections(sourceDigestTrimSelections)
    || hasSourceDigestTrimSelections(directSourceDigestTrimSelections)
    || Object.keys(directSourceDigestPendingCandidates).length > 0
  );
  const sourceDigestUiActive = (
    sourceDigestPanelActive
    || summaryMode === "expert"
    || !displayCompareData
    || sourceDigestHasActiveFocus
    || sourceDigestHasActiveDraftSelection
  );
  const localSourceDigestCandidates: SourceDigestGroupCandidate[] = sourceDigestUiActive
    ? localComparableGroupEntries.map((entry) => ({
        group: entry.group,
        ocrEngine: localDigest?.ocrEngine,
        ocrEngineCandidates: localDigest?.ocrEngineCandidates,
        ocrEvaluation: localDigest?.ocrEvaluation,
        sourceDigestType: localDigest?.digestType,
        sourceFileName: localDigest?.fileName || LOCAL_CONFIG_WORKBOOK_FILE,
        sourceFormat: localDigest?.sourceFormat,
        sourceGroupIndex: entry.index,
        sourceGroupCount: localComparableGroups.length,
        sourceKind: "local",
      }))
    : [];
  const sourceDigestCandidates: SourceDigestGroupCandidate[] = sourceDigestUiActive
    ? [
        ...localSourceDigestCandidates,
        ...librarySourceDigestCandidates,
      ]
    : [];
  const directSourceDigestPendingItems = buildDirectSourceDigestPendingItems(
    directSourceDigestPendingCandidates,
    directSourceDigestTrimSelections,
    sourceDigestCandidates,
  );
  const focusedSourceDigestSnapshot = focusedSourceDigestSourceId
    ? librarySourceSnapshotMatches.find((snapshot) => snapshot.sourceId === focusedSourceDigestSourceId) ?? null
    : null;
  const focusedSourceDigestCandidate = focusedSourceDigestSourceId
    ? sourceDigestCandidates.find((candidate) => candidate.sourceId === focusedSourceDigestSourceId) ?? null
    : null;
  const focusedSourceDigestSourceLabel = focusedSourceDigestSnapshot?.sourceFileName
    ?? focusedSourceDigestCandidate?.sourceFileName
    ?? null;
  const sourceDigestSearchMatchedCandidates = sourceDigestCandidates.filter((candidate) => (
    sourceDigestGroupMatchesSearch(candidate, sourceDigestSearchQuery)
  ));
  const scopedSourceDigestCandidates = sortSourceDigestCandidates(
    sourceDigestSearchMatchedCandidates.filter((candidate) => (
      focusedSourceDigestSourceId || sourceDigestGroupMatchesFilters(candidate, sourceDigestFilters)
    )),
    sourceDigestFilters,
    sourceDigestSearchQuery,
  );
  const sourceDigestQualityFilterItemsList = sourceDigestQualityFilterItems(scopedSourceDigestCandidates);
  const sourceDigestQualityFilteredCandidates = sourceDigestQualityFilter === "all"
    ? scopedSourceDigestCandidates
    : scopedSourceDigestCandidates.filter((candidate) => (
      sourceDigestCandidateMatchesQualityFilter(candidate, sourceDigestQualityFilter)
    ));
  const filteredSourceDigestCandidates = focusedSourceDigestSourceId
    ? sourceDigestQualityFilteredCandidates.filter((candidate) => candidate.sourceId === focusedSourceDigestSourceId)
    : sourceDigestQualityFilteredCandidates;
  const sourceDigestVisibleCoverage = sourceDigestCandidateCoverage(filteredSourceDigestCandidates);
  const sourceDigestTotalCoverage = sourceDigestCandidateCoverage(sourceDigestQualityFilteredCandidates);
  const sourceDigestBrowseCondensed = (
    !sourceDigestBrowseExpanded
    && !sourceDigestLibrarySearchActive
    && !focusedSourceDigestSourceId
    && sourceDigestQualityFilter === "all"
    && filteredSourceDigestCandidates.length > SOURCE_DIGEST_BROWSE_PREVIEW_LIMIT
  );
  const sourceDigestBrowseCanCollapse = (
    sourceDigestBrowseExpanded
    && !sourceDigestLibrarySearchActive
    && !focusedSourceDigestSourceId
    && sourceDigestQualityFilter === "all"
    && filteredSourceDigestCandidates.length > SOURCE_DIGEST_BROWSE_PREVIEW_LIMIT
  );
  const prioritisedSourceDigestBrowseCandidates = prioritiseSourceDigestCandidatesBySelectedModels(
    filteredSourceDigestCandidates,
    selectedDisplayTrims,
  );
  const visibleSourceDigestBrowseCandidates = sourceDigestBrowseCondensed
    ? prioritisedSourceDigestBrowseCandidates.slice(0, SOURCE_DIGEST_BROWSE_PREVIEW_LIMIT)
    : prioritisedSourceDigestBrowseCandidates;
  const hiddenSourceDigestBrowseCount = Math.max(0, filteredSourceDigestCandidates.length - visibleSourceDigestBrowseCandidates.length);
  const sourceDigestBrowseGroups = buildSourceDigestBrowseGroups(visibleSourceDigestBrowseCandidates);
  const sourceDigestPathPreviewAllGroups = prioritiseSourceDigestBrowseGroupsBySelectedModels(
    buildSourceDigestBrowseGroups(prioritisedSourceDigestBrowseCandidates),
    selectedDisplayTrims,
  );
  const sourceDigestPathPreviewGroups = sourceDigestPathPreviewAllGroups.slice(0, SOURCE_DIGEST_PATH_PREVIEW_LIMIT);
  const sourceDigestPathPreviewHiddenCount = Math.max(0, sourceDigestPathPreviewAllGroups.length - sourceDigestPathPreviewGroups.length);
  const sourceDigestPathPreviewCompact = (
    simpleModeActive
    && !sourceDigestBrowseExpanded
    && !sourceDigestDetailBrowserOpen
    && !focusedSourceDigestSourceId
    && sourceDigestPathPreviewAllGroups.length > SOURCE_DIGEST_PATH_PREVIEW_LIMIT
  );
  const sourceDigestPathPreviewVisibleGroups = sourceDigestPathPreviewCompact ? [] : sourceDigestPathPreviewGroups;
  const sourceDigestLibraryVisibleGroupCount = filteredSourceDigestCandidates.filter((candidate) => candidate.sourceKind === "library").length;
  const sourceDigestLibraryExpandedGroupCount = librarySourceDigestCandidates.length;
  const sourceDigestLibraryEstimatedGroupCount = librarySourceSnapshotMatches.reduce(
    (total, snapshot) => total + sourceSnapshotComparableGroupCount(snapshot),
    0,
  );
  const sourceDigestDetailSummaryGroupCount = sourceDigestDetailBrowserOpen
    ? filteredSourceDigestCandidates.length
    : Math.max(filteredSourceDigestCandidates.length, sourceDigestLibraryEstimatedGroupCount);
  const sourceDigestLibraryNarrowed = sourceDigestLibraryExpandedGroupCount > sourceDigestLibraryVisibleGroupCount;
  const sourceDigestSearchOptions = mergeDropdownOptions(
    buildSourceDigestCandidateDropdownOptions(sourceDigestCandidates),
    buildSourceDigestKeywordOptions(sourceDigestCandidates),
    buildSimpleDropdownOptions(sourceDigestCandidates.flatMap(sourceDigestModelValues)),
    buildSimpleDropdownOptions(sourceDigestCandidates.flatMap((candidate) => [candidate.sourceFileName, candidate.group.sourceSheet])),
  );
  const sourceDigestLibraryResultHint = librarySourceDigestLoading
    ? "正在搜索来源库可转配置组..."
    : librarySourceDigestError
      ? librarySourceDigestError
      : !sourceDigestLibrarySearchActive
        ? "输入车型 / 来源 / 上传人 / 物料号 / sales version 后再搜索来源库；本地样例仍可直接预览。"
        : librarySourceDigestTotalRows > 0
          ? sourceDigestDetailBrowserOpen
            ? `来源库命中 ${librarySourceDigestTotalRows} 个来源，当前显示 ${sourceDigestLibraryVisibleGroupCount}/${sourceDigestLibraryExpandedGroupCount} 个可转配置列组。${
              sourceDigestLibraryNarrowed
                ? sourceDigestQualityFilter === "all"
                  ? "已按当前车型 / 市场 / 来源 / 关键词或来源聚焦收窄。"
                  : "已按来源类型、当前车型 / 市场 / 来源 / 关键词或来源聚焦收窄。"
                : librarySourceDigestTotalRows > 8
                  ? "结果较多时继续输入车型 / 市场 / 年款 / 来源 / 关键词精确定位。"
                  : ""
            }`
            : `来源库命中 ${librarySourceDigestTotalRows} 个来源，约 ${sourceDigestLibraryEstimatedGroupCount} 个可转配置列组；展开来源组详情后加载 OCR、sheet、合并来源细节。`
          : "来源库暂无匹配来源；可上传 xlsx / PDF / 图片 / CSV / HTML / 价格单后解析入库。";
  const sourceDigestLibraryScopeHintText = sourceDigestLibraryScopeHint(sourceDigestFilters, sourceDigestLookupQuery, sourceDigestLibrarySearchActive);
  const directSourceDigestOptions = buildSourceDigestDirectDropdownOptions(
    scopedSourceDigestCandidates,
    sourceDigestTrimSelections,
    directSourceDigestTrimSelections,
  );
  const sourcePanelDigestOptions = buildSourceDigestDirectDropdownOptions(
    filteredSourceDigestCandidates,
    sourceDigestTrimSelections,
    directSourceDigestTrimSelections,
  );
  const selectedSourcePanelDigestCandidateKey = sourceDigestDirectOptionKey(sourceDigestDirectPickerValue);
  const selectedSourcePanelDigestCandidate = selectedSourcePanelDigestCandidateKey
    ? sourceDigestCandidates.find((candidate) => sourceDigestCandidateKey(candidate) === selectedSourcePanelDigestCandidateKey) ?? null
    : null;
  const selectedSourcePanelDigestBusy = selectedSourcePanelDigestCandidateKey
    ? sourceDigestDraftActionKey === selectedSourcePanelDigestCandidateKey
    : false;
  function clearFocusedSourceDigestSource(): void {
    const restoreSourceQuery = !sourceDigestSearchQuery.trim() && focusedSourceDigestSourceLabel
      ? focusedSourceDigestSourceLabel
      : null;
    if (restoreSourceQuery) setSourceDigestSearchQuery(restoreSourceQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
  }

  const sourceDigestActiveScopeCandidates: Array<SourceDigestActiveScopeItem | null> = [
    sourceDigestSearchQuery.trim()
      ? {
          key: "query",
          label: sourceDigestQueryScopeLabel(sourceDigestSearchQuery, filteredSourceDigestCandidates),
          value: sourceDigestSearchQuery.trim(),
          tone: "search",
          clearLabel: "清除 Source Digest 搜索词",
          onClear: () => {
            setSourceDigestSearchQuery("");
            setSourceDigestDirectPickerValue("");
            setPendingSourceDigestFocusSourceId(null);
          },
        }
      : null,
    filters.market.trim()
      ? { key: "market", label: "国家", value: filters.market.trim(), tone: "scope" }
      : null,
    filters.powertrain.trim()
      ? { key: "powertrain", label: "动力", value: filters.powertrain.trim(), tone: "scope" }
      : null,
    filters.segment.trim()
      ? { key: "segment", label: "Segment", value: filters.segment.trim(), tone: "scope" }
      : null,
    focusedSourceDigestSourceId
      ? {
          key: "source-focus",
          label: "只看来源",
          value: focusedSourceDigestSourceLabel ?? focusedSourceDigestSourceId,
          tone: "focus",
          clearLabel: "解除来源聚焦",
          onClear: clearFocusedSourceDigestSource,
        }
      : null,
    sourceDigestQualityFilter !== "all"
      ? {
          key: "source-type",
          label: "来源类型",
          value: sourceDigestQualityFilterItemsList.find((item) => item.key === sourceDigestQualityFilter)?.label ?? sourceDigestQualityFilter,
          tone: "quality",
          clearLabel: "清除来源类型筛选",
          onClear: () => setSourceDigestQualityFilter("all"),
        }
      : null,
  ];
  const sourceDigestActiveScopeItems = sourceDigestActiveScopeCandidates.filter((item): item is SourceDigestActiveScopeItem => Boolean(item));
  const directSourceDigestSelectedOptionValues = Object.entries(directSourceDigestTrimSelections).flatMap(([candidateKey, selectedTrimIds]) => {
    const candidate = directSourceDigestPendingCandidates[candidateKey]
      ?? sourceDigestCandidates.find((item) => sourceDigestCandidateKey(item) === candidateKey);
    if (!candidate) return [];
    return selectedTrimIds.map((trimId) => sourceDigestDirectTrimOptionValue(candidate, trimId));
  });
  const directConfigSelectedOptionValues = uniquePresent([
    ...directDropdownSelectedTrimIds,
    ...selectedDisplayTrims.map((trim) => trim.trimId),
    ...directSourceDigestSelectedOptionValues,
  ]);
  const directSourceDigestCoverage = sourceDigestDirectOptionCoverage(scopedSourceDigestCandidates);
  const directSourceDigestCoverageItems = sourceDigestDirectCoverageItems(directSourceDigestCoverage);
  const directSourceDigestAmbiguities = sourceDigestDirectAmbiguityItems(scopedSourceDigestCandidates);
  const directFormalModelAmbiguities = formalLibraryModelAmbiguityItems(directTrimCandidates);
  const directModelAmbiguities = [
    ...directFormalModelAmbiguities,
    ...directSourceDigestAmbiguities.map(sourceDigestAmbiguityToDirectModelAmbiguity),
  ];
  const directModelAmbiguityContextActive = (
    directTrimSearchKeyword.length >= 2
    || sourceDigestSearchQuery.trim().length >= 2
    || Boolean(focusedSourceDigestSourceId)
    || Boolean(pendingSourceDigestFocusSourceId)
    || directSourceDigestPendingItems.length > 0
    || Boolean(filters.brand.trim())
    || Boolean(filters.model.trim())
    || Boolean(filters.source.trim())
  );
  const visibleDirectModelAmbiguities = directModelAmbiguityContextActive ? directModelAmbiguities : [];
  const directSourceDigestHint = sourceDigestDirectResultHint(directSourceDigestCoverage);
  const directSourceDigestPendingColumnCount = directSourceDigestPendingItems.reduce(
    (total, item) => total + item.selectedTrimIds.length,
    0,
  );
  const directConfigSearchSummaryItems = buildDirectConfigSearchSummaryItems({
    formalOptionCount: directFormalOptionCount,
    formalSearchActive: directFormalSearchActive,
    formalTotalRows: directTrimSearchTotalRows,
    pendingColumnCount: directSourceDigestPendingColumnCount,
    pendingGroupCount: directSourceDigestPendingItems.length,
    sourceCoverage: directSourceDigestCoverage,
  });
  const directConfigColumnOptions = mergeDropdownOptions(directTrimOptions, directSourceDigestOptions);
  const directSearchCanOpenSourceUpload = (
    directTrimSearchKeyword.length >= 2
    && !directTrimSearchLoading
    && !librarySourceDigestLoading
    && !directTrimSearchError
    && directFormalOptionCount === 0
    && directTrimSearchTotalRows === 0
    && directSourceDigestCoverage.libraryOptionCount === 0
    && directSourceDigestCoverage.localOptionCount === 0
  );
  const directSearchClearActive = (
    Boolean(directTrimPickerValue.trim())
    || Boolean(directTrimSearchKeyword)
    || Boolean(sourceDigestSearchQuery.trim())
    || Boolean(focusedSourceDigestSourceId)
    || Boolean(pendingSourceDigestFocusSourceId)
    || Boolean(directTrimSearchError)
  );
  const directConfigSearchResultHint = librarySourceDigestLoading
    ? `${directTrimSearchResultHint} 正在同时搜索来源库。`
    : directSourceDigestHint
      ? `${directTrimSearchResultHint} ${directSourceDigestHint}`
      : directTrimSearchResultHint;
  const brandOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildSimpleDropdownOptions(filterOptionColumns.map((trim) => trim.brand)),
        buildSimpleDropdownOptions(sourceDigestCandidates.flatMap(sourceDigestBrandValues)),
      )
    : [];
  const modelOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildSimpleDropdownOptions(filterOptionColumns.map((trim) => trim.modelName)),
        buildSimpleDropdownOptions(sourceDigestCandidates.flatMap(sourceDigestModelValues)),
      )
    : [];
  const trimOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildTrimDropdownOptions(filterOptionColumns),
        buildSimpleDropdownOptions(sourceDigestCandidates.flatMap((candidate) => (
          candidate.group.trims.map((trim) => trim.trimName || trim.fullTrimName)
        ))),
      )
    : [];
  const marketOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildSimpleDropdownOptions(filterOptionColumns.map((trim) => trim.market || trim.country)),
        buildSimpleDropdownOptions(sourceDigestCandidates.flatMap(sourceDigestMarketValues)),
      )
    : [];
  const modelYearOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildSimpleDropdownOptions(filterOptionColumns.map((trim) => trim.modelYear)),
        buildSimpleDropdownOptions(sourceDigestCandidates.flatMap(sourceDigestModelYearValues)),
      )
    : [];
  const powertrainOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildSimpleDropdownOptions(filterOptionColumns.map((trim) => trim.energyType || trim.drivetrain)),
        buildSimpleDropdownOptions(sourceDigestCandidates.flatMap(sourceDigestPowertrainValues)),
      )
    : [];
  const sourceOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildSourceDropdownOptions(filterOptionColumns),
        buildSourceDigestSourceDropdownOptions(sourceDigestCandidates),
      )
    : [];
  const segmentOptions = advancedConfigSearchActive
    ? buildSimpleDropdownOptions([
        filters.segment,
        ...competitorRecommendations.map((recommendation) => recommendationProfileText(recommendation, "segment")),
      ])
    : [];
  const keywordOptions = advancedConfigSearchActive
    ? mergeDropdownOptions(
        buildKeywordDropdownOptions(filterOptionColumns, displayCompareData?.rows ?? []),
        buildSourceDigestKeywordOptions(sourceDigestCandidates),
      )
    : [];
  const targetTrimOptions = buildTargetTrimDropdownOptions(
    selectedDisplayTrims,
    baseTrimId,
    displayCompareData,
    simpleModeActive ? "全部目标列" : "全部目标 trim",
  );
  const selectedStripSourcePreview = uniquePresent(selectedDisplayTrims.map((trim) => trimSourceSnapshotLabel(trim)))
    .slice(0, 2)
    .join(" · ");
  const selectedStripSourceOverflow = Math.max(0, uniquePresent(selectedDisplayTrims.map((trim) => trimSourceSnapshotLabel(trim))).length - 2);
  const selectedStripSourceHint = selectedStripSourcePreview
    ? `${selectedStripSourcePreview}${selectedStripSourceOverflow > 0 ? ` · +${selectedStripSourceOverflow} 来源` : ""}`
    : "来源待补";
  const targetAnchorItems = buildTargetAnchorItems(baseTrim, activeTargetTrim, selectedDisplayTrims);
  const sourceContextTrims = activeTargetTrim
    ? uniqueComparableTrims([baseTrim, activeTargetTrim])
    : selectedDisplayTrims;
  const recommendationModel = singleSourceContextValue(filters.model, sourceContextTrims.map((trim) => trim.modelName));
  const recommendationCountry = singleSourceContextValue(filters.market, sourceContextTrims.map((trim) => trim.market || trim.country));
  const recommendationPowertrain = singleSourceContextValue(filters.powertrain, sourceContextTrims.map((trim) => trim.energyType || trim.drivetrain));
  const recommendationSegment = filters.segment.trim() || null;
  const competitorRecommendationsVisible = advancedConfigSearchActive;
  const libraryTrimGroups = advancedConfigSearchActive ? buildLibraryTrimGroups(trims) : [];
  const libraryBrandTrimGroups = advancedConfigSearchActive ? buildLibraryBrandTrimGroups(libraryTrimGroups) : [];

  useEffect(() => {
    if (!activeTargetTrimId) return;
    const targetStillSelected = selectedDisplayTrims.some((trim) => trim.trimId === activeTargetTrimId);
    if (!targetStillSelected || activeTargetTrimId === baseTrimId) setActiveTargetTrimId(null);
  }, [activeTargetTrimId, baseTrimId, selectedDisplayTrims]);

  useEffect(() => {
    if (!editModeAvailable || selectedDisplayTrims.length === 0) {
      setTrimIdentityEditId(null);
      return;
    }
    if (trimIdentityEditId && selectedDisplayTrims.some((trim) => trim.trimId === trimIdentityEditId)) return;
    setTrimIdentityEditId((activeTargetTrim || baseTrim || selectedDisplayTrims[0])?.trimId ?? null);
  }, [activeTargetTrim, baseTrim, editModeAvailable, selectedDisplayTrims, trimIdentityEditId]);

  useEffect(() => {
    setTrimIdentityDraft(trimIdentityDraftFromTrim(trimIdentityEditTrim));
    setTrimIdentityFeedback(null);
  }, [trimIdentityEditTrim]);

  useEffect(() => {
    if (!competitorRecommendationsVisible) {
      setCompetitorRecommendationsLoading(false);
      return;
    }
    if (!recommendationModel || !recommendationCountry) {
      setCompetitorRecommendations([]);
      setCompetitorRecommendationSource(null);
      setCompetitorRecommendationNote("先选择 Model 和 Market 后再推荐竞品。");
      return;
    }
    let disposed = false;
    setCompetitorRecommendations([]);
    setCompetitorRecommendationSource(null);
    setCompetitorRecommendationsLoading(true);
    setCompetitorRecommendationNote(null);
    api.listEngineeringConfigCompetitorRecommendations({
      country: recommendationCountry,
      model_name: recommendationModel,
      powertrain: recommendationPowertrain || undefined,
      segment: recommendationSegment || undefined,
      limit: COMPETITOR_RECOMMENDATION_LIMIT,
    })
      .then((result) => {
        if (disposed) return;
        setCompetitorRecommendations(result.items || []);
        setCompetitorRecommendationSource(result.source ?? null);
        if (result.errorMessage) {
          setCompetitorRecommendationNote("高级分析暂不可用，推荐竞品稍后重试。");
        } else if (result.message && result.message !== "ok") {
          setCompetitorRecommendationNote("当前口径暂无高级分析推荐竞品。");
        } else {
          setCompetitorRecommendationNote(null);
        }
      })
      .catch((err) => {
        if (disposed) return;
        setCompetitorRecommendations([]);
        setCompetitorRecommendationSource(null);
        setCompetitorRecommendationNote(err instanceof Error ? err.message : "推荐竞品加载失败");
      })
      .finally(() => {
        if (!disposed) setCompetitorRecommendationsLoading(false);
      });
    return () => {
      disposed = true;
    };
  }, [competitorRecommendationsVisible, recommendationCountry, recommendationModel, recommendationPowertrain, recommendationSegment]);

  async function saveTrimIdentity(): Promise<void> {
    if (!trimIdentityEditTrim || trimIdentitySaving) return;
    setTrimIdentitySaving(true);
    setTrimIdentityFeedback(null);
    try {
      const response = await api.updateEngineeringConfigTrim(
        trimIdentityEditTrim.trimId,
        trimIdentityPayloadFromDraft(trimIdentityDraft, editAuditReason),
      );
      const patch = trimIdentityPatchFromApi(response, trimIdentityEditTrim);
      setCompareData((current) => (
        current ? updateCompareDataTrimIdentity(current, trimIdentityEditTrim.trimId, patch) : current
      ));
      setTrims((current) => current.map((trim) => applyTrimIdentityPatch(trim, trimIdentityEditTrim.trimId, patch)));
      setTrimOptionPool((current) => current.map((trim) => applyTrimIdentityPatch(trim, trimIdentityEditTrim.trimId, patch)));
      setDirectTrimSearchResults((current) => current.map((trim) => applyTrimIdentityPatch(trim, trimIdentityEditTrim.trimId, patch)));
      setCompetitorRecommendations((current) => current.map((recommendation) => ({
        ...recommendation,
        trims: recommendation.trims.map((trim) => applyTrimIdentityPatch(trim, trimIdentityEditTrim.trimId, patch)),
      })));
      setTrimIdentityDraft(trimIdentityDraftFromTrim({ ...trimIdentityEditTrim, ...patch }));
      setTrimIdentityFeedback("配置列身份已保存。");
    } catch (err) {
      setTrimIdentityFeedback(err instanceof Error ? err.message : "配置列身份保存失败");
    } finally {
      setTrimIdentitySaving(false);
    }
  }

  function handleFeatureCatalogMappingFileChange(event: ChangeEvent<HTMLInputElement>): void {
    const nextFile = event.target.files?.[0] ?? null;
    event.target.value = "";
    setFeatureCatalogMappingSummary(null);
    setFeatureCatalogMappingAudit(null);
    if (!nextFile) {
      setFeatureCatalogMappingFile(null);
      return;
    }
    if (!/\.(xlsx|xlsm|xls)$/i.test(nextFile.name)) {
      setFeatureCatalogMappingFile(null);
      setFeatureCatalogMappingFeedback("仅支持 .xlsx / .xlsm / .xls 字段映射表。");
      return;
    }
    setFeatureCatalogMappingFile(nextFile);
    setFeatureCatalogMappingFeedback(`${nextFile.name} 已选择，点击导入后会更新标准字段别名。`);
  }

  async function uploadFeatureCatalogMapping(): Promise<void> {
    if (!featureCatalogMappingFile || featureCatalogMappingUploading || !userCanEditValues) return;
    setFeatureCatalogMappingUploading(true);
    setFeatureCatalogMappingFeedback("正在上传字段映射表...");
    setFeatureCatalogMappingSummary(null);
    setFeatureCatalogMappingAudit(null);
    try {
      const initiated = await api.initiateEngineeringConfigFeatureCatalogUpload(
        featureCatalogMappingFile.name,
        featureCatalogMappingFile.size,
        FEATURE_CATALOG_MAPPING_CHUNK_SIZE,
      );
      const chunkSize = initiated.chunkSize || FEATURE_CATALOG_MAPPING_CHUNK_SIZE;
      for (let partIndex = 0; partIndex < initiated.totalChunks; partIndex += 1) {
        const start = partIndex * chunkSize;
        const end = Math.min(featureCatalogMappingFile.size, start + chunkSize);
        setFeatureCatalogMappingFeedback(`正在上传字段映射表 ${partIndex + 1}/${initiated.totalChunks}...`);
        await api.uploadEngineeringConfigFeatureCatalogChunk(
          initiated.uploadId,
          partIndex,
          featureCatalogMappingFile.slice(start, end),
        );
      }
      setFeatureCatalogMappingFeedback("正在解析并更新标准字段别名...");
      const completed = await api.completeEngineeringConfigFeatureCatalogUpload(initiated.uploadId);
      const completedAudit: FeatureCatalogMappingUploadAudit = completed.audit ?? {
        uploadId: initiated.uploadId,
        fileName: completed.fileName,
        status: completed.status,
        importedAtUtc: new Date().toISOString(),
        artifactRef: `eng_config_uploads/${initiated.uploadId}/session.json`,
        persistedIn: "upload_session_meta",
        summary: completed.summary,
      };
      setFeatureCatalogMappingSummary(completed.summary);
      setFeatureCatalogMappingAudit(completedAudit);
      setFeatureCatalogMappingFeedback(
        `字段映射已导入：更新 ${completed.summary.updatedFeatureCount} 项，新增 ${completed.summary.createdFeatureCount} 项。`,
      );
      setFeatureCatalogMappingFile(null);
    } catch (err) {
      setFeatureCatalogMappingFeedback(err instanceof Error ? err.message : "字段映射表导入失败。");
    } finally {
      setFeatureCatalogMappingUploading(false);
    }
  }

  async function copyFeatureCatalogMappingAudit(): Promise<void> {
    if (!featureCatalogMappingAudit) return;
    if (!navigator.clipboard?.writeText) {
      setFeatureCatalogMappingFeedback("当前浏览器不支持复制字段映射审计摘要。");
      return;
    }
    try {
      await navigator.clipboard.writeText(featureCatalogMappingAuditText(featureCatalogMappingAudit));
      setFeatureCatalogMappingFeedback("字段映射审计摘要已复制。");
    } catch (err) {
      setFeatureCatalogMappingFeedback(err instanceof Error ? err.message : "字段映射审计摘要复制失败。");
    }
  }

  useEffect(() => {
    if (summaryMode !== "simple" || SIMPLE_DISPLAY_SCOPE_KEYS.has(activeDeltaFilter)) return;
    setTableDeltaFilter("ALL");
  }, [activeDeltaFilter, summaryMode]);

  const sameModelGroups = advancedConfigSearchActive ? buildSameModelTrimGroups(trims) : [];
  const prefilterSourceUploadActive = externalPrefilterActive && sourceContextTrims.length === 0;
  const sourceUploadContext: EngineeringConfigSourceContext = {
    brand: sourceContextValue(filters.brand, sourceContextTrims.map((trim) => trim.brand)),
    model: sourceContextValue(filters.model, sourceContextTrims.map((trim) => trim.modelName)),
    market: sourceContextSingleScopeValue(filters.market, sourceContextTrims.map((trim) => trim.market || trim.country)),
    country: sourceContextSingleScopeValue(filters.market, sourceContextTrims.map((trim) => trim.country || trim.market)),
    powertrain: recommendationPowertrain,
    segment: recommendationSegment,
    modelYear: sourceContextValue(filters.modelYear, sourceContextTrims.map((trim) => trim.modelYear)),
    trimIds: sourceContextTrims.map((trim) => trim.trimId).slice(0, 4),
    salesVersionIds: uniquePresent(sourceContextTrims.map((trim) => sourceContextIdentityAnchor(trim))),
    contextType: prefilterSourceUploadActive ? "product_compare_prefilter_upload" : activeTargetTrim ? "model_trim_compare_target" : "model_trim_compare",
    scenario: prefilterSourceUploadActive ? "filtered_config_library_miss" : "product_model_trim_compare",
    identityAnchor: sourceContextIdentityAnchorType(sourceContextTrims),
  };
  const effectiveSourceUploadContext = sourceContextOverride ?? sourceUploadContext;
  const summary = displayCompareData ? displayCompareData.summary || buildFallbackSummary(displayCompareData) : null;
  const tableScopeRows = displayCompareData
    ? displayCompareData.rows.filter((row) => (
      rowMatchesConfigScope(displayCompareData, row, activeDeltaFilter, baseTrimId, activeTargetTrimId)
      && rowMatchesConfigSearch(row, activeTableSearch)
    ))
    : [];
  const scopedSummaryRows = tableScopeRows.filter((row) => !activeCategoryFilter || row.category === activeCategoryFilter);
  const scopedSummaryMetrics = displayCompareData
    ? summarizeScopedConfigRows(displayCompareData, scopedSummaryRows, baseTrimId, activeTargetTrimId)
    : emptyScopedConfigSummaryMetrics();
  const evidenceValueIndexes = displayCompareData
    ? sourceEvidenceValueIndexes(displayCompareData, baseTrim?.trimId ?? baseTrimId, activeTargetTrimId)
    : null;
  const sourceEvidenceMetrics = summarizeSourceEvidenceRows(scopedSummaryRows, evidenceValueIndexes);
  const visibleItemUnit = simpleModeActive ? simpleRowScopeUnitLabel(activeDeltaFilter) : scopeUnitLabel(activeDeltaFilter);
  const prefilterLabel = trimPrefilterLabel(filters);
  const tableSearchActive = Boolean(activeTableSearch.trim());
  const scopeParts = [
    tableSearchActive ? "当前搜索" : null,
    activeTargetTrim ? `${simpleModeActive ? "当前目标配置列" : "当前目标"} ${compareTrimLabel(activeTargetTrim)}` : null,
    activeCategoryFilter ? `当前大类 ${categoryDisplayLabel(activeCategoryFilter)}` : null,
  ].filter((part): part is string => Boolean(part));
  const simpleVisibleRowLabel = summary
    ? `当前展示 ${scopedSummaryRows.length}/${summary.totalFeatures} ${simpleRowScopeUnitLabel(activeDeltaFilter)}`
    : null;
  const controlTriggerPrimaryLabel = valuesCanBeEdited ? "编辑已开启 / 显示" : "添加配置列 / 显示";
  const controlTriggerSecondaryClosedLabel = valuesCanBeEdited ? "打开编辑控制" : "打开控制";
  const summaryShownLabel = scopeParts.length > 0
    ? `${scopeParts.join(" · ")} · ${simpleModeActive && simpleVisibleRowLabel ? simpleVisibleRowLabel : `${scopedSummaryRows.length} ${visibleItemUnit}`}`
    : simpleModeActive && simpleVisibleRowLabel
      ? simpleVisibleRowLabel
      : `当前表格 ${tableScopeRows.length} ${visibleItemUnit}`;
  const localDigestSwitcherOpen = summaryMode === "expert" || !displayCompareData;
  const showExpertContextBlocks = summaryMode === "expert" || !displayCompareData;
  const showLocalDigestSection = summaryMode === "expert" || !displayCompareData;
  const handleLlmSummaryChange = useCallback((
    items: EngineeringConfigBusinessSummaryItem[],
    usage: EngineeringConfigBusinessSummaryUsage | null,
  ) => {
    setBusinessSummaryExportItems(items);
    setBusinessSummaryExportUsage(usage);
  }, []);
  const searchUploadedSourceInFloatingDeck = useCallback((query: string): void => {
    const nextQuery = query.trim();
    if (!nextQuery) return;
    setSourceDigestSearchQuery(nextQuery);
    setDirectTrimSearchQuery(nextQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(true);
    setControlOpen(true);
    setActivePanel("source");
  }, []);

  function openSourceDigestAfterFeatureMappingImport(): void {
    const query = sourceDigestLibraryLookupQuery(
      filters,
      sourceDigestSearchQueryForTrim(activeTargetTrim ?? baseTrim ?? selectedDisplayTrims[0] ?? null),
    );
    if (query) searchUploadedSourceInFloatingDeck(query);
    else openControlPanel("source");
    setSourceDigestDraftFeedback("字段映射已更新；请重新从来源生成配置列以应用新别名。");
  }

  function openSourcePanelForDirectSearch(): void {
    const query = directTrimSearchKeyword.trim();
    if (!query) return;
    const uploadModel = query;
    const searchQuery = sourceDigestLibraryLookupQuery(filters, query);
    setFilters((current) => ({
      ...current,
      model: uploadModel,
    }));
    setSourceContextOverride({
      brand: filters.brand || null,
      model: uploadModel,
      market: filters.market || null,
      country: filters.market || null,
      powertrain: filters.powertrain || null,
      segment: filters.segment || null,
      modelYear: filters.modelYear || null,
      trimIds: [],
      salesVersionIds: [],
      contextType: "direct_search_upload",
      scenario: "config_library_search_miss",
      identityAnchor: "brand_model_market",
    });
    setSourceDigestSearchQuery(searchQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(true);
    openControlPanel("source");
  }
  const categoryNavItems = buildCategoryNavItems(tableScopeRows);
  const rowScopeActive = activeDeltaFilter !== "ALL"
    || Boolean(activeCategoryFilter)
    || tableSearchActive;
  const analysisScopeActive = rowScopeActive || Boolean(activeTargetTrimId);
  const restoreAllConfigRowsLabel = simpleModeActive ? "恢复全部配置行" : "恢复全部配置";
  const heroScopeResetLabel = rowScopeActive ? restoreAllConfigRowsLabel : "显示全部目标列";
  const heroDifferenceActionLabel = simpleModeActive ? "查看差异行" : "查看差异项";
  const resultScopeChipLabel = simpleModeActive ? simpleDisplayScopeLabel(activeDeltaFilter) : resultScopeLabel(activeDeltaFilter);
  function updateSourceDigestSearchQuery(query: string): void {
    const focusedCandidate = query.trim()
      ? sourceDigestCandidates.find((candidate) => sourceDigestCandidateDropdownValue(candidate) === query.trim()) ?? null
      : null;
    setSourceDigestSearchQuery(query);
    setPendingSourceDigestFocusSourceId(null);
    if (focusedCandidate) {
      setSourceDigestDirectPickerValue(sourceDigestDirectOptionValue(focusedCandidate));
      setSourceDigestDetailBrowserOpen(true);
      setSourceDigestBrowseExpanded(false);
      setSourceDigestDraftFeedback(`${sourceDigestCandidateScopedLabel(focusedCandidate)} 已定位；下方可直接${sourceDigestPendingActionText(focusedCandidate)}，也可继续暂选单个配置列。`);
    } else {
      setSourceDigestDirectPickerValue("");
    }
  }

  const updateSourceFilter = (value: string): void => {
    setFilters((current) => ({ ...current, source: value }));
    updateSourceDigestSearchQuery("");
  };
  const updateSourceFilterQuery = (query: string): void => {
    updateSourceDigestSearchQuery(query);
  };
  const refreshConfigLibrariesAfterSourceUpload = (uploadedSource?: EngineeringConfigSourceSnapshot): void => {
    void loadTrims();
    const uploadedCandidates = uploadedSource
      ? sourceDigestCandidatesFromSnapshot(uploadedSource)
        .filter((candidate) => (
          sourceDigestGroupMatchesFilters(candidate, sourceDigestFilters)
          && (!sourceDigestLibrarySearchActive || sourceDigestGroupMatchesSearch(candidate, sourceDigestLookupQuery))
        ))
      : [];
    if (uploadedSource && uploadedCandidates.length > 0) {
      setLibrarySourceSnapshotMatches((previous) => [
        uploadedSource,
        ...previous.filter((snapshot) => snapshot.sourceId !== uploadedSource.sourceId),
      ]);
      setLibrarySourceDigestCandidates((previous) => {
        const byKey = new Map<string, SourceDigestGroupCandidate>();
        uploadedCandidates.forEach((candidate) => {
          byKey.set(sourceDigestCandidateKey(candidate), candidate);
        });
        previous.forEach((candidate) => {
          byKey.set(sourceDigestCandidateKey(candidate), candidate);
        });
        return Array.from(byKey.values()).slice(0, SOURCE_DIGEST_GROUP_CANDIDATE_LIMIT);
      });
      setLibrarySourceDigestTotalRows((current) => Math.max(current, 1));
    }
    setSourceDigestLibraryRefreshKey((current) => current + 1);
  };
  const scopePrimaryMetric = primaryScopeMetric(
    activeDeltaFilter,
    scopedSummaryMetrics,
    sourceEvidenceMetrics,
    scopedSummaryRows.length,
    simpleModeActive,
  );
  const analysisScopeItems: AnalysisScopeItem[] = [
    baseTrim ? { key: "base", label: simpleModeActive ? "基准列" : "基准", value: compareTrimLabel(baseTrim) } : null,
    activeTargetTrim ? { key: "target", label: simpleModeActive ? "目标配置列" : "目标聚焦", value: compareTrimLabel(activeTargetTrim) } : null,
    { key: "scope", label: "范围", value: simpleModeActive ? simpleDisplayScopeLabel(activeDeltaFilter) : resultScopeLabel(activeDeltaFilter) },
    activeCategoryFilter ? { key: "category", label: "大类", value: categoryDisplayLabel(activeCategoryFilter) } : null,
    tableSearchActive ? { key: "search", label: "搜索", value: activeTableSearch.trim() } : null,
    { key: "shown", label: "当前", value: `${scopedSummaryRows.length} ${visibleItemUnit}` },
  ].filter((item): item is AnalysisScopeItem => Boolean(item));
  const deckExportRowCount = configTableExportStatus?.rowCount ?? scopedSummaryRows.length;
  const deckExportTrimCount = configTableExportStatus?.trimCount ?? (activeTargetTrim && baseTrim ? 2 : selectedDisplayTrims.length);
  const deckExportRangeLabel = configTableExportStatus?.rangeLabel ?? (simpleModeActive ? simpleDisplayScopeLabel(activeDeltaFilter) : resultScopeLabel(activeDeltaFilter));
  const deckExportReady = Boolean(configTableExportStatus);
  const deckExportAvailable = Boolean(displayCompareData && configTableExportStatus?.canExport);
  const deckCopyLabel = configTableExportStatus?.copyLabel ?? "复制当前表格";

  async function copyCurrentTableFromDeck(): Promise<void> {
    const actions = configTableExportActionsRef.current;
    if (!actions || !actions.canExport) {
      setDeckExportFeedback("当前表格没有可复制的配置行。");
      return;
    }
    setDeckExportFeedback(null);
    await actions.copyCurrentScope();
    setDeckExportFeedback(`${actions.copyLabel}：${actions.rowCount} ${simpleRowScopeUnitLabel(activeDeltaFilter)}，${actions.trimCount} 个配置列。`);
  }

  async function exportCurrentTableFromDeck(format: "xlsx" | "pdf"): Promise<void> {
    const actions = configTableExportActionsRef.current;
    if (!actions || !actions.canExport) {
      setDeckExportFeedback("当前表格没有可导出的配置行。");
      return;
    }
    setDeckExportingFormat(format);
    setDeckExportFeedback(null);
    try {
      if (format === "xlsx") {
        await actions.exportXlsx();
      } else {
        await actions.exportPdf();
      }
      setDeckExportFeedback(`已请求导出 ${format.toUpperCase()}：${actions.rowCount} ${simpleRowScopeUnitLabel(activeDeltaFilter)}，${actions.trimCount} 个配置列。`);
    } finally {
      setDeckExportingFormat(null);
    }
  }

  useEffect(() => {
    if (!editModeAvailable && editModeEnabled) setEditModeEnabled(false);
  }, [editModeAvailable, editModeEnabled]);

  useEffect(() => {
    if (!displayCompareData) {
      configTableExportActionsRef.current = null;
      setConfigTableExportStatus(null);
    }
  }, [displayCompareData]);

  function setTableDeltaFilter(filter: ConfigComparisonDeltaFilter, targetTrimOverride: string | null = activeTargetTrimId): void {
    if (displayCompareData && activeCategoryFilter) {
      const nextCategories = visibleCategoriesForDeltaFilter(displayCompareData, baseTrimId, filter, activeTableSearch, targetTrimOverride);
      if (!nextCategories.includes(activeCategoryFilter)) setActiveCategoryFilter(null);
    }
    setActiveDeltaFilter(filter);
  }

  function setComparisonBaseTrim(trimId: string): void {
    const nextTargetTrimId = activeTargetTrimId === trimId ? null : activeTargetTrimId;
    if (displayCompareData && activeCategoryFilter) {
      const nextCategories = visibleCategoriesForDeltaFilter(displayCompareData, trimId, activeDeltaFilter, activeTableSearch, nextTargetTrimId);
      if (!nextCategories.includes(activeCategoryFilter)) setActiveCategoryFilter(null);
    }
    setBaseTrimId(trimId);
    if (activeTargetTrimId === trimId) setActiveTargetTrimId(null);
  }

  function resetCompareSelectionScope(): void {
    setActiveDigestGroupId(null);
    setActiveCategoryFilter(null);
    setActiveDeltaFilter("ALL");
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
  }

  function toggleCompareId(trimId: string): void {
    resetCompareSelectionScope();
    setCompareIds((previous) => {
      if (previous.includes(trimId)) return previous.filter((id) => id !== trimId);
      if (previous.length >= 4) return previous;
      return [...previous, trimId];
    });
  }

  function toggleRecommendedTrim(trim: VehicleTrimItem): void {
    setTrimOptionPool((previous) => mergeTrimOptionPool(previous, [trim]));
    toggleCompareId(trim.trimId);
  }

  function addRecommendedTrims(trimsToAdd: VehicleTrimItem[]): void {
    const incomingTrims = uniqueVehicleTrims(trimsToAdd);
    if (incomingTrims.length === 0) return;
    resetCompareSelectionScope();
    setTrimOptionPool((previous) => mergeTrimOptionPool(previous, incomingTrims));
    setCompareIds((previous) => {
      const next = [...previous];
      const selected = new Set(next);
      incomingTrims.forEach((trim) => {
        if (next.length >= 4 || selected.has(trim.trimId)) return;
        next.push(trim.trimId);
        selected.add(trim.trimId);
      });
      return next;
    });
  }

  function selectDirectTrim(trimId: string): void {
    const trim = directTrimLookup.get(trimId);
    setDirectTrimPickerValue("");
    setDirectTrimSearchQuery("");
    if (!trim) return;
    setTrimOptionPool((previous) => mergeTrimOptionPool(previous, [trim]));
    toggleCompareId(trim.trimId);
  }

  function focusSourceDigestCandidatesFromDirectDropdown(
    focusCandidates: SourceDigestGroupCandidate[],
    focusLabel: string,
    searchQuery: string,
    sourceFocusId: string | null,
  ): void {
    const firstCandidate = focusCandidates[0];
    if (!firstCandidate) return;
    setDirectTrimSearchQuery("");
    setSourceDigestSearchQuery(searchQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(sourceFocusId);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(true);
    setSourceDigestDraftFeedback(`${focusLabel} 已聚焦；继续在当前搜索下拉里选择 2-4 个同来源配置列，或切到来源面板查看详情。`);
  }

  async function selectDirectConfigColumn(value: string): Promise<void> {
    setDirectTrimPickerValue("");
    const libraryModelTrimIds = configLibraryDirectModelAddOptionTrimIds(value);
    if (libraryModelTrimIds) {
      const modelTrims = libraryModelTrimIds
        .map((trimId) => directTrimLookup.get(trimId))
        .filter((trim): trim is VehicleTrimItem => Boolean(trim));
      setDirectTrimSearchQuery("");
      setSourceDigestSearchQuery("");
      setPendingSourceDigestFocusSourceId(null);
      addRecommendedTrims(modelTrims);
      return;
    }
    const librarySourceQuery = configLibraryDirectSourceOptionQuery(value);
    if (librarySourceQuery) {
      setDirectTrimPickerValue(value);
      setDirectTrimSearchQuery(librarySourceQuery);
      updateSourceDigestSearchQuery(librarySourceQuery);
      return;
    }
    const libraryBrandQuery = configLibraryDirectBrandOptionQuery(value);
    if (libraryBrandQuery) {
      setDirectTrimPickerValue(value);
      setDirectTrimSearchQuery(libraryBrandQuery);
      updateSourceDigestSearchQuery(libraryBrandQuery);
      return;
    }
    const libraryModelQuery = configLibraryDirectModelOptionQuery(value);
    if (libraryModelQuery) {
      setDirectTrimPickerValue(value);
      setDirectTrimSearchQuery(libraryModelQuery);
      updateSourceDigestSearchQuery(libraryModelQuery);
      return;
    }
    const sourceFocusKey = sourceDigestDirectSourceOptionKey(value);
    if (sourceFocusKey) {
      const focusCandidates = sourceDigestCandidates.filter((candidate) => (
        sourceDigestSourceFocusKey(candidate) === sourceFocusKey
      ));
      const firstCandidate = focusCandidates[0];
      if (firstCandidate) {
        focusSourceDigestCandidatesFromDirectDropdown(
          focusCandidates,
          `来源 ${firstCandidate.sourceFileName}`,
          firstCandidate.sourceId ? "" : firstCandidate.sourceFileName,
          firstCandidate.sourceId ?? null,
        );
      }
      return;
    }
    const crossModelFocusKey = sourceDigestDirectCrossModelOptionKey(value);
    if (crossModelFocusKey) {
      const focusCandidates = sourceDigestCandidates.filter((candidate) => (
        sourceDigestCrossModelFocusKey(candidate) === crossModelFocusKey
      ));
      const firstCandidate = focusCandidates[0];
      if (firstCandidate) {
        focusSourceDigestCandidatesFromDirectDropdown(
          focusCandidates,
          `同名车型 ${firstCandidate.group.modelName}`,
          firstCandidate.group.modelName,
          null,
        );
      }
      return;
    }
    const modelFocusKey = sourceDigestDirectModelOptionKey(value);
    if (modelFocusKey) {
      const focusCandidates = sourceDigestCandidates.filter((candidate) => (
        sourceDigestModelFocusKey(candidate) === modelFocusKey
      ));
      const firstCandidate = focusCandidates[0];
      if (firstCandidate) {
        focusSourceDigestCandidatesFromDirectDropdown(
          focusCandidates,
          `Model ${firstCandidate.group.modelName}`,
          firstCandidate.group.modelName,
          firstCandidate.sourceId ?? null,
        );
      }
      return;
    }
    const sourceDigestTrimSelection = sourceDigestDirectTrimOptionSelection(value);
    if (sourceDigestTrimSelection) {
      const candidate = sourceDigestCandidates.find((item) => sourceDigestCandidateKey(item) === sourceDigestTrimSelection.candidateKey);
      if (candidate) await selectDirectSourceDigestTrim(candidate, sourceDigestTrimSelection.trimId);
      return;
    }
    const sourceDigestKey = sourceDigestDirectOptionKey(value);
    if (sourceDigestKey) {
      setDirectTrimSearchQuery("");
      setSourceDigestSearchQuery("");
      setPendingSourceDigestFocusSourceId(null);
      const candidate = sourceDigestCandidates.find((item) => sourceDigestCandidateKey(item) === sourceDigestKey);
      if (candidate) await selectSourceDigestCandidate(candidate);
      return;
    }
    setSourceDigestSearchQuery("");
    setPendingSourceDigestFocusSourceId(null);
    selectDirectTrim(value);
  }

  async function selectSourcePanelDigestOption(value: string): Promise<void> {
    setSourceDigestDirectPickerValue("");
    await selectDirectConfigColumn(value);
    setActivePanel("source");
  }

  async function confirmSelectedSourcePanelDigestCandidate(): Promise<void> {
    if (!sourceDigestDirectPickerValue || selectedSourcePanelDigestBusy) return;
    await selectDirectConfigColumn(sourceDigestDirectPickerValue);
  }

  function handleSourceDigestDirectQueryChange(query: string): void {
    if (!query.trim()) return;
    updateSourceDigestSearchQuery(query);
  }

  function openSourceDigestAnchorSearch(anchor: SourceDigestSearchAnchor, event?: MouseEvent<HTMLButtonElement>): void {
    event?.preventDefault();
    event?.stopPropagation();
    if (!anchor.query) return;
    const nextSourceFocusId = anchor.key === "source" && anchor.sourceId ? anchor.sourceId : null;
    setSourceDigestSearchQuery(nextSourceFocusId ? "" : anchor.query);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(nextSourceFocusId);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(true);
    setActivePanel("source");
  }

  function renderSourceDigestSearchAnchor(anchor: SourceDigestSearchAnchor): ReactElement {
    const actionLabel = sourceDigestAnchorActionLabel(anchor);
    return (
      <button
        className="product-config-source-anchor"
        type="button"
        key={anchor.key}
        disabled={!anchor.query}
        aria-label={`${actionLabel}：${anchor.value}`}
        onClick={(event) => openSourceDigestAnchorSearch(anchor, event)}
      >
        <small>{actionLabel}</small>
        <strong>{anchor.value}</strong>
      </button>
    );
  }

  function openSourceDigestAmbiguitySearch(item: SourceDigestDirectAmbiguityItem): void {
    setSourceDigestSearchQuery(item.searchQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestDetailBrowserOpen(false);
    setActivePanel("source");
  }

  function openDirectModelAmbiguitySearch(item: DirectModelAmbiguityItem): void {
    if (item.origin === "source-digest") {
      openSourceDigestAmbiguitySearch(item);
      return;
    }
    void selectDirectConfigColumn(configLibraryDirectModelOptionValue(item.searchQuery));
    setActivePanel("filters");
    setControlOpen(true);
    setDirectPickerFocusRequest((current) => current + 1);
  }

  function openAllSourceDigestAmbiguitySearch(): void {
    const query = directTrimSearchKeyword || directModelAmbiguities[0]?.searchQuery || "";
    if (directSourceDigestAmbiguities.length === 0) {
      if (query) {
        void selectDirectConfigColumn(configLibraryDirectModelOptionValue(query));
      }
      setActivePanel("filters");
      setControlOpen(true);
      setDirectPickerFocusRequest((current) => current + 1);
      return;
    }
    if (query) setSourceDigestSearchQuery(query);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(true);
    setSourceDigestDetailBrowserOpen(true);
    setActivePanel("source");
  }

  function updateSourceDigestTrimIdentityDraft(
    candidate: SourceDigestGroupCandidate,
    trim: EngineeringConfigSourceDigestGroup["trims"][number],
    field: SourceDigestTrimIdentityFieldKey,
    value: string,
  ): void {
    const candidateKey = sourceDigestCandidateKey(candidate);
    const trimId = sourceDigestTrimId(trim);
    setSourceDigestTrimIdentityDrafts((current) => {
      const currentCandidateDrafts = current[candidateKey] ?? {};
      const currentTrimDraft = currentCandidateDrafts[trimId] ?? sourceDigestTrimIdentityDefaults(candidate, trim);
      return {
        ...current,
        [candidateKey]: {
          ...currentCandidateDrafts,
          [trimId]: {
            ...currentTrimDraft,
            trimId,
            [field]: value,
          },
        },
      };
    });
  }

  function removeSourceDigestTrimIdentityDraft(candidateKey: string, trimId?: string): void {
    setSourceDigestTrimIdentityDrafts((current) => {
      const next = { ...current };
      if (!trimId) {
        delete next[candidateKey];
        return next;
      }
      const candidateDrafts = { ...(next[candidateKey] ?? {}) };
      delete candidateDrafts[trimId];
      if (Object.keys(candidateDrafts).length > 0) {
        next[candidateKey] = candidateDrafts;
      } else {
        delete next[candidateKey];
      }
      return next;
    });
  }

  function renderSourceDigestTemporaryIdentityEditor(
    candidate: SourceDigestGroupCandidate,
    selectedTrimIds: string[],
  ): ReactElement | null {
    const temporaryTrims = sourceDigestSelectedTemporaryOcrTrims(candidate, selectedTrimIds);
    if (temporaryTrims.length === 0) return null;
    const ready = sourceDigestTemporaryIdentityReady(candidate, selectedTrimIds, sourceDigestTrimIdentityDrafts);
    return (
      <div className="product-config-source-digest-identity-editor" aria-label={`${candidate.group.modelName} OCR 临时列身份映射`}>
        <div className="product-config-source-digest-identity-editor__head">
          <strong>OCR 临时列身份映射</strong>
          <small>{ready ? "已满足建列身份要求" : "先补真实车型 / 配置列，再生成正式配置列"}</small>
        </div>
        {temporaryTrims.map((trim) => {
          const trimId = sourceDigestTrimId(trim);
          const draft = sourceDigestTrimIdentityDraftValue(candidate, trim, sourceDigestTrimIdentityDrafts);
          return (
            <div className="product-config-source-digest-identity-editor__row" key={trimId}>
              <div className="product-config-source-digest-identity-editor__origin">
                <span>临时列</span>
                <strong>{sourceDigestTrimLabel(trim)}</strong>
              </div>
              <div className="product-config-source-digest-identity-editor__fields">
                {SOURCE_DIGEST_TRIM_IDENTITY_FORM_FIELDS.map((field) => (
                  <label key={field.key}>
                    <span>{field.label}{field.required ? " *" : ""}</span>
                    <input
                      value={draft[field.key] ?? ""}
                      required={field.required}
                      placeholder={field.placeholder}
                      aria-label={`${sourceDigestTrimLabel(trim)} ${field.label}`}
                      onChange={(event) => updateSourceDigestTrimIdentityDraft(candidate, trim, field.key, event.currentTarget.value)}
                    />
                  </label>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  function focusSourceDigestReviewRow(
    candidate: SourceDigestGroupCandidate,
    row: EngineeringConfigSourceDigestGroup["rows"][number],
    rowIndex: number,
  ): void {
    const candidateKey = sourceDigestCandidateKey(candidate);
    setSourceDigestReviewFocuses((current) => ({
      ...current,
      [candidateKey]: sourceDigestReviewRowKey(row, rowIndex),
    }));
    setSourceDigestDraftFeedback(`${sourceDigestCandidateScopedLabel(candidate)} 建列后将定位到需核对行：${row.featureName}。`);
  }

  function renderSourceDigestReviewRowSelector(candidate: SourceDigestGroupCandidate): ReactElement | null {
    const reviewRows = sourceDigestReviewRows(candidate);
    if (reviewRows.length === 0) return null;
    const candidateKey = sourceDigestCandidateKey(candidate);
    const selectedRowKey = sourceDigestReviewFocuses[candidateKey];
    const selectedRow = sourceDigestSelectedReviewRow(candidate, sourceDigestReviewFocuses);
    return (
      <details className="product-config-source-digest-card__review-rows" aria-label={`${candidate.group.modelName} 需核对配置行定位`}>
        <summary>
          <span>需核对行 {reviewRows.length}</span>
          <small>建列后定位：{selectedRow?.featureName ?? reviewRows[0]?.featureName ?? "第一条需核对行"}</small>
        </summary>
        <div>
          {reviewRows.map((row, rowIndex) => {
            const rowKey = sourceDigestReviewRowKey(row, rowIndex);
            const note = row.reviewNotes?.find((item) => item.trim().length > 0)?.trim();
            const active = selectedRow ? selectedRow === row : false;
            const explicit = selectedRowKey === rowKey;
            const actionLabel = explicit ? "已设为建列后定位" : active ? "默认建列后定位" : "建列后定位此行";
            return (
              <button
                className={`product-config-source-digest-card__review-row ${active ? "is-active" : ""}`}
                type="button"
                key={rowKey}
                aria-pressed={active}
                aria-label={`${actionLabel}：${row.featureName}`}
                onClick={() => focusSourceDigestReviewRow(candidate, row, rowIndex)}
              >
                <strong>{row.featureName}</strong>
                <span>{actionLabel}</span>
                {note ? <small>{note}</small> : null}
              </button>
            );
          })}
        </div>
      </details>
    );
  }

  async function selectDirectSourceDigestTrim(candidate: SourceDigestGroupCandidate, trimId: string): Promise<void> {
    const actionKey = sourceDigestCandidateKey(candidate);
    const candidateLabel = sourceDigestCandidateScopedLabel(candidate);
    const currentSelection = directSourceDigestTrimSelections[actionKey] ?? [];
    const alreadySelected = currentSelection.includes(trimId);
    if (!alreadySelected && currentSelection.length >= 4) {
      setSourceDigestDraftFeedback(`${candidateLabel} 已暂选 4/4 个配置列；请先取消一个再选择。`);
      return;
    }
    const nextSelection = alreadySelected
      ? currentSelection.filter((value) => value !== trimId)
      : [...currentSelection, trimId];
    setDirectSourceDigestTrimSelections((current) => {
      const next = { ...current };
      if (nextSelection.length > 0) {
        next[actionKey] = nextSelection;
      } else {
        delete next[actionKey];
      }
      return next;
    });
    setDirectSourceDigestPendingCandidates((current) => {
      const next = { ...current };
      if (nextSelection.length > 0) {
        next[actionKey] = candidate;
      } else {
        delete next[actionKey];
      }
      return next;
    });
    if (alreadySelected) removeSourceDigestTrimIdentityDraft(actionKey, nextSelection.length > 0 ? trimId : undefined);
    const selectedLabels = candidate.group.trims
      .filter((trim) => nextSelection.includes(sourceDigestTrimId(trim)))
      .map(sourceDigestTrimLabel);
    const pendingActionText = sourceDigestPendingActionText(candidate);
    if (nextSelection.length < 2) {
      setSourceDigestDraftFeedback(
        nextSelection.length === 0
          ? `${candidateLabel} 的直接暂选已清空。`
          : `${candidateLabel} 已暂选 ${nextSelection.length}/4 个配置列：${selectedLabels.join(" / ")}；继续选择同组配置列后${pendingActionText}。`,
      );
      return;
    }
    setSourceDigestDraftFeedback(
      `${candidateLabel} 已暂选 ${nextSelection.length}/4 个配置列：${selectedLabels.join(" / ")}；可继续选择同组配置列，或在已选区域${pendingActionText}。`,
    );
  }

  function removeDirectSourceDigestPendingTrim(candidateKey: string, trimId: string): void {
    const candidate = directSourceDigestPendingCandidates[candidateKey] ?? sourceDigestCandidates.find((item) => sourceDigestCandidateKey(item) === candidateKey);
    if (!candidate) return;
    const currentSelection = directSourceDigestTrimSelections[candidateKey] ?? [];
    const nextSelection = currentSelection.filter((value) => value !== trimId);
    setDirectSourceDigestTrimSelections((current) => {
      const next = { ...current };
      if (nextSelection.length > 0) {
        next[candidateKey] = nextSelection;
      } else {
        delete next[candidateKey];
      }
      return next;
    });
    setDirectSourceDigestPendingCandidates((current) => {
      const next = { ...current };
      if (nextSelection.length > 0) {
        next[candidateKey] = candidate;
      } else {
        delete next[candidateKey];
      }
      return next;
    });
    removeSourceDigestTrimIdentityDraft(candidateKey, nextSelection.length > 0 ? trimId : undefined);
    const selectedLabels = candidate.group.trims
      .filter((trim) => nextSelection.includes(sourceDigestTrimId(trim)))
      .map(sourceDigestTrimLabel);
    const pendingActionText = sourceDigestPendingActionText(candidate);
    const candidateLabel = sourceDigestCandidateScopedLabel(candidate);
    setSourceDigestDraftFeedback(
      nextSelection.length === 0
        ? `${candidateLabel} 的直接暂选已清空。`
        : `${candidateLabel} 已暂选 ${nextSelection.length}/4 个配置列：${selectedLabels.join(" / ")}；${nextSelection.length >= 2 ? `可${pendingActionText}。` : `继续选择同组配置列后${pendingActionText}。`}`,
    );
  }

  function clearDirectSourceDigestPending(candidateKey: string): void {
    const candidate = directSourceDigestPendingCandidates[candidateKey] ?? sourceDigestCandidates.find((item) => sourceDigestCandidateKey(item) === candidateKey);
    setDirectSourceDigestTrimSelections((current) => {
      const next = { ...current };
      delete next[candidateKey];
      return next;
    });
    setDirectSourceDigestPendingCandidates((current) => {
      const next = { ...current };
      delete next[candidateKey];
      return next;
    });
    removeSourceDigestTrimIdentityDraft(candidateKey);
    if (candidate) setSourceDigestDraftFeedback(`${sourceDigestCandidateScopedLabel(candidate)} 的直接暂选已清空。`);
  }

  function clearAllDirectSourceDigestPending(): void {
    const pendingKeys = directSourceDigestPendingItems.map((item) => item.key);
    if (pendingKeys.length === 0) return;
    const pendingKeySet = new Set(pendingKeys);
    setDirectSourceDigestTrimSelections((current) => {
      const next = { ...current };
      pendingKeys.forEach((key) => {
        delete next[key];
      });
      return next;
    });
    setDirectSourceDigestPendingCandidates((current) => {
      const next = { ...current };
      pendingKeys.forEach((key) => {
        delete next[key];
      });
      return next;
    });
    setSourceDigestTrimIdentityDrafts((current) => {
      const next: SourceDigestTrimIdentityDraftMap = {};
      Object.entries(current).forEach(([key, value]) => {
        if (!pendingKeySet.has(key)) next[key] = value;
      });
      return next;
    });
    setSourceDigestDraftFeedback(`已清空 ${pendingKeys.length} 组 Source Digest 暂选。`);
  }

  async function createDirectSourceDigestPendingDraft(candidateKey: string): Promise<void> {
    const candidate = directSourceDigestPendingCandidates[candidateKey] ?? sourceDigestCandidates.find((item) => sourceDigestCandidateKey(item) === candidateKey);
    if (!candidate) {
      setSourceDigestDraftFeedback("待转配置列来源已失效，请重新搜索选择。");
      return;
    }
    const selectedTrimIds = directSourceDigestPendingTrimIds(candidate, directSourceDigestTrimSelections[candidateKey]);
    if (selectedTrimIds.length < 2) {
      setSourceDigestDraftFeedback(`${sourceDigestCandidateScopedLabel(candidate)} 至少需要暂选 2 个配置列后才能${candidate.sourceKind === "library" ? "生成" : "预览"}。`);
      return;
    }
    await createSourceDigestCandidateDraft(candidate, selectedTrimIds);
  }

  function handleDirectConfigColumnQueryChange(query: string): void {
    setDirectTrimSearchQuery(query);
    updateSourceDigestSearchQuery(query);
  }

  function clearDirectConfigColumnSearch(): void {
    setDirectTrimPickerValue("");
    setDirectTrimSearchQuery("");
    setDirectTrimSearchResults([]);
    setDirectTrimSearchTotalRows(0);
    setDirectTrimSearchError(null);
    setSourceDigestSearchQuery("");
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(false);
    setDirectPickerResetKey((current) => current + 1);
  }

  async function moveTrimToLibraryTrash(trim: ComparableTrim): Promise<void> {
    if (digestModeActive || !userCanEditValues || trimTrashActionId) return;
    const trimLabel = compareTrimLabel(trim);
    setTrimTrashClearConfirmKey(null);
    setTrimTrashActionId(trim.trimId);
    setTrimTrashFeedback(null);
    try {
      await api.updateEngineeringConfigTrim(trim.trimId, {
        status: "trashed",
        comment: editAuditReason.trim() || "配置列移入垃圾桶",
      });
      setCompareIds((previous) => previous.filter((id) => id !== trim.trimId));
      setTrimOptionPool((previous) => previous.filter((item) => item.trimId !== trim.trimId));
      setTrims((previous) => previous.filter((item) => item.trimId !== trim.trimId));
      setDirectTrimSearchResults((previous) => previous.filter((item) => item.trimId !== trim.trimId));
      if (baseTrimId === trim.trimId) setBaseTrimId(null);
      if (activeTargetTrimId === trim.trimId) setActiveTargetTrimId(null);
      setActiveCategoryFilter(null);
      setActiveDeltaFilter("ALL");
      setActiveTableSearch("");
      setTrimTrashFeedback(`${trimLabel} 已移入配置列库垃圾桶。`);
      void loadTrims();
    } catch (err) {
      setTrimTrashFeedback(err instanceof Error ? err.message : "配置列移入垃圾桶失败");
    } finally {
      setTrimTrashActionId(null);
    }
  }

  async function loadTrimLibraryTrash(): Promise<void> {
    if (!trimTrashCountry) {
      setTrimTrashFeedback("请先选择单一 Market，再查看配置列库垃圾桶。");
      return;
    }
    setTrimTrashClearConfirmKey(null);
    setTrimTrashLoading(true);
    setTrimTrashFeedback(null);
    try {
      const result = await api.listEngineeringConfigTrims({
        market: trimTrashCountry,
        status: "trashed",
        limit: 100,
      });
      const items = (result.items || []) as unknown as VehicleTrimItem[];
      setTrimTrashItems(items);
      setTrimTrashFeedback(`${trimTrashCountry} 配置列库垃圾桶 ${items.length} 项。`);
    } catch (err) {
      setTrimTrashFeedback(err instanceof Error ? err.message : "配置列库垃圾桶加载失败");
    } finally {
      setTrimTrashLoading(false);
    }
  }

  async function restoreTrimFromLibraryTrash(trim: VehicleTrimItem): Promise<void> {
    if (!userCanEditValues || trimTrashActionId) return;
    setTrimTrashClearConfirmKey(null);
    setTrimTrashActionId(trim.trimId);
    setTrimTrashFeedback(null);
    try {
      await api.updateEngineeringConfigTrim(trim.trimId, {
        status: "draft",
        comment: editAuditReason.trim() || "配置列从垃圾桶恢复",
      });
      setTrimTrashItems((previous) => previous.filter((item) => item.trimId !== trim.trimId));
      setTrimTrashFeedback(`${compareTrimLabel(trim)} 已恢复为 Draft 配置列。`);
      void loadTrims();
    } catch (err) {
      setTrimTrashFeedback(err instanceof Error ? err.message : "配置列恢复失败");
    } finally {
      setTrimTrashActionId(null);
    }
  }

  async function clearTrimLibraryTrash(): Promise<void> {
    if (!userCanEditValues || trimTrashActionId || !trimTrashCountry || trimTrashItems.length === 0) return;
    setTrimTrashClearConfirmKey(null);
    setTrimTrashActionId("__clear_trim_trash__");
    setTrimTrashFeedback(null);
    try {
      const result = await api.clearEngineeringConfigTrimTrash(trimTrashCountry);
      const clearedCount = result.cleared;
      setTrimTrashItems([]);
      setTrimTrashFeedback(`已清空 ${trimTrashCountry} 配置列库垃圾桶 ${clearedCount} 项。`);
      void loadTrims();
    } catch (err) {
      setTrimTrashFeedback(err instanceof Error ? err.message : "配置列库垃圾桶清空失败");
    } finally {
      setTrimTrashActionId(null);
    }
  }

  function requestClearTrimLibraryTrash(): void {
    if (!userCanEditValues || trimTrashActionId || !trimTrashCountry || trimTrashItems.length === 0 || !trimTrashClearKey) return;
    if (!trimTrashClearArmed) {
      setTrimTrashClearConfirmKey(trimTrashClearKey);
      setTrimTrashFeedback(`再次点击确认清空 ${trimTrashCountry} 配置列垃圾桶，才会永久清空 ${trimTrashItems.length} 项。`);
      return;
    }
    void clearTrimLibraryTrash();
  }

  async function moveSourceSnapshotToTrash(snapshot: EngineeringConfigSourceSnapshot): Promise<void> {
    if (!userCanEditValues || sourceTrashActionId || !sourceTrashCountry) {
      setSourceTrashFeedback("请先选择单一 Market，再移动来源到当前国家垃圾桶。");
      return;
    }
    setSourceTrashClearConfirmKey(null);
    setSourceTrashActionId(snapshot.sourceId);
    setSourceTrashFeedback(null);
    try {
      await api.trashEngineeringConfigSourceSnapshot(snapshot.sourceId, sourceTrashCountry);
      setLibrarySourceSnapshotMatches((previous) => previous.filter((item) => item.sourceId !== snapshot.sourceId));
      setLibrarySourceDigestCandidates((previous) => previous.filter((candidate) => candidate.sourceId !== snapshot.sourceId));
      if (focusedSourceDigestSourceId === snapshot.sourceId) setFocusedSourceDigestSourceId(null);
      if (pendingSourceDigestFocusSourceId === snapshot.sourceId) setPendingSourceDigestFocusSourceId(null);
      setSourceTrashFeedback(`${snapshot.sourceFileName} 已移入 ${sourceTrashCountry} 来源垃圾桶。`);
    } catch (err) {
      setSourceTrashFeedback(err instanceof Error ? err.message : "来源移入垃圾桶失败");
    } finally {
      setSourceTrashActionId(null);
    }
  }

  async function loadSourceSnapshotTrash(): Promise<void> {
    if (!sourceTrashCountry) {
      setSourceTrashFeedback("请先选择单一 Market，再查看来源垃圾桶。");
      return;
    }
    setSourceTrashClearConfirmKey(null);
    setSourceTrashLoading(true);
    setSourceTrashFeedback(null);
    try {
      const result = await api.listEngineeringConfigSourceSnapshots({
        country: sourceTrashCountry,
        trashOnly: true,
        limit: 100,
      });
      setSourceTrashItems(result.items || []);
      setSourceTrashFeedback(`${sourceTrashCountry} 来源垃圾桶 ${result.items.length} 项。`);
    } catch (err) {
      setSourceTrashFeedback(err instanceof Error ? err.message : "来源垃圾桶加载失败");
    } finally {
      setSourceTrashLoading(false);
    }
  }

  async function restoreSourceSnapshotFromTrash(snapshot: EngineeringConfigSourceSnapshot): Promise<void> {
    if (!userCanEditValues || sourceTrashActionId || !sourceTrashCountry) return;
    setSourceTrashClearConfirmKey(null);
    setSourceTrashActionId(snapshot.sourceId);
    setSourceTrashFeedback(null);
    try {
      await api.restoreEngineeringConfigSourceSnapshot(snapshot.sourceId, sourceTrashCountry);
      setSourceTrashItems((previous) => previous.filter((item) => item.sourceId !== snapshot.sourceId));
      setSourceTrashFeedback(`${snapshot.sourceFileName} 已从 ${sourceTrashCountry} 来源垃圾桶恢复。`);
    } catch (err) {
      setSourceTrashFeedback(err instanceof Error ? err.message : "来源恢复失败");
    } finally {
      setSourceTrashActionId(null);
    }
  }

  async function clearSourceSnapshotTrash(): Promise<void> {
    if (!userCanEditValues || sourceTrashActionId || !sourceTrashCountry || sourceTrashItems.length === 0) return;
    setSourceTrashClearConfirmKey(null);
    setSourceTrashActionId("__clear_source_trash__");
    setSourceTrashFeedback(null);
    try {
      const result = await api.clearEngineeringConfigSourceTrash(sourceTrashCountry);
      setSourceTrashItems([]);
      setSourceTrashFeedback(`已清空 ${sourceTrashCountry} 来源垃圾桶 ${result.cleared} 项。`);
    } catch (err) {
      setSourceTrashFeedback(err instanceof Error ? err.message : "来源垃圾桶清空失败");
    } finally {
      setSourceTrashActionId(null);
    }
  }

  function requestClearSourceSnapshotTrash(): void {
    if (!userCanEditValues || sourceTrashActionId || !sourceTrashCountry || sourceTrashItems.length === 0 || !sourceTrashClearKey) return;
    if (!sourceTrashClearArmed) {
      setSourceTrashClearConfirmKey(sourceTrashClearKey);
      setSourceTrashFeedback(`再次点击确认清空 ${sourceTrashCountry} 来源垃圾桶，才会永久清空 ${sourceTrashItems.length} 项。`);
      return;
    }
    void clearSourceSnapshotTrash();
  }

  function openSourcePanelForCompetitor(
    recommendation: EngineeringConfigCompetitorRecommendation,
    sourceMatch?: CompetitorSourceDigestMatch,
  ): void {
    const lookup = competitorSourceLookup(recommendation);
    const sourceDigestAvailable = Boolean(recommendation.sourceDigestAvailable);
    const sourceSearchQuery = sourceMatch
      ? competitorSourceDigestMatchLabel(sourceMatch)
      : recommendation.sourceDigestSearchQuery?.trim() || lookup.sourceSearchQuery;
    setSourceContextOverride({
      brand: recommendation.brand || null,
      model: recommendation.modelName || null,
      market: lookup.targetCountry,
      country: lookup.targetCountry,
      powertrain: lookup.targetPowertrain,
      segment: lookup.targetSegment,
      modelYear: null,
      trimIds: [],
      salesVersionIds: [],
      contextType: sourceDigestAvailable ? "competitor_recommendation_source_digest" : "competitor_recommendation_upload",
      scenario: sourceDigestAvailable ? "recommended_competitor_source_digest_available" : "recommended_competitor_config_gap",
      identityAnchor: "brand_model_market",
    });
    setSourceDigestSearchQuery(sourceSearchQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(sourceMatch?.sourceId ?? null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(true);
    openControlPanel("source");
  }

  function closeDigestSample(): void {
    setLocalDigestSampleRequested(false);
    setActiveDigestGroupId(null);
    setBaseTrimId(null);
    setActiveCategoryFilter(null);
    setActiveDeltaFilter("ALL");
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
    setEvidenceSelection(null);
  }

  function openLocalDigestSample(): void {
    setLocalDigestSampleRequested(true);
  }

  function selectGroup(group: SameModelTrimGroup): void {
    const ids = group.items.slice(0, 4).map((trim) => trim.trimId);
    setActiveDigestGroupId(null);
    setActiveCategoryFilter(null);
    setActiveDeltaFilter("ALL");
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
    setCompareIds(ids);
    setBaseTrimId(ids[0] ?? null);
    setActivePanel("display");
  }

  function selectDigestGroup(group: EngineeringConfigSourceDigestGroup, selectedTrimIds?: string[], groupKey = localDigestGroupKey(group)): void {
    if (selectedTrimIds) {
      setSourceDigestTrimSelections((current) => ({
        ...current,
        [groupKey]: normaliseSourceDigestTrimSelection(group, selectedTrimIds),
      }));
    }
    const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
    setCompareIds([]);
    setCompareData(null);
    setActiveDigestGroupId(groupKey);
    setBaseTrimId(selected[0] ?? group.trims[0]?.trimId ?? null);
    setActiveCategoryFilter(null);
    setActiveDeltaFilter("ALL");
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
  }

  async function createSourceDigestCandidateDraft(
    candidate: SourceDigestGroupCandidate,
    explicitTrimIds?: string[],
  ): Promise<void> {
    const actionKey = sourceDigestCandidateKey(candidate);
    const selectedTrimIds = explicitTrimIds
      ? normaliseSourceDigestTrimSelection(candidate.group, explicitTrimIds)
      : selectedSourceDigestTrimIds(candidate, sourceDigestTrimSelections);
    if (candidate.sourceKind === "local" || !candidate.sourceId) {
      selectDigestGroup(candidate.group, selectedTrimIds, actionKey);
      setSourceDigestDraftFeedback(`${sourceDigestCandidateScopedLabel(candidate)} 已加载本地预览；本地样例仅用于预览，上传或搜索来源库后可生成正式配置列。`);
      setSourceDigestDraftSuccess(null);
      setDirectSourceDigestTrimSelections((current) => {
        const next = { ...current };
        delete next[actionKey];
        return next;
      });
      setDirectSourceDigestPendingCandidates((current) => {
        const next = { ...current };
        delete next[actionKey];
        return next;
      });
      return;
    }
    if (!sourceDigestTemporaryIdentityReady(candidate, selectedTrimIds, sourceDigestTrimIdentityDrafts)) {
      setDirectSourceDigestTrimSelections((current) => ({
        ...current,
        [actionKey]: selectedTrimIds,
      }));
      setDirectSourceDigestPendingCandidates((current) => ({
        ...current,
        [actionKey]: candidate,
      }));
      setActivePanel("source");
      setSourceDigestDraftFeedback(`${sourceDigestCandidateScopedLabel(candidate)} 是 OCR 临时列；请先补真实车型 / 配置列身份后再生成正式配置列。`);
      return;
    }
    setSourceDigestDraftActionKey(actionKey);
    setSourceDigestDraftFeedback(null);
    setSourceDigestDraftSuccess(null);
    try {
      const trimPayload = explicitTrimIds
        ? sourceDigestSelectedTrimPayloadFromIds(candidate, selectedTrimIds)
        : sourceDigestSelectedTrimPayload(candidate, sourceDigestTrimSelections);
      const trimIdentityOverrides = sourceDigestTrimIdentityOverridePayload(candidate, selectedTrimIds, sourceDigestTrimIdentityDrafts);
      const draftOptions: {
        trimIds?: string[];
        trimIdentityOverrides?: EngineeringConfigDigestTrimIdentityOverride[];
      } = {
        ...(trimPayload ?? {}),
        ...(trimIdentityOverrides.length > 0 ? { trimIdentityOverrides } : {}),
      };
      const result = Object.keys(draftOptions).length > 0
        ? await api.createEngineeringConfigDraftFromSourceDigest(candidate.sourceId, candidate.group.groupId, draftOptions)
        : await api.createEngineeringConfigDraftFromSourceDigest(candidate.sourceId, candidate.group.groupId);
      const reviewRow = sourceDigestSelectedReviewRow(candidate, sourceDigestReviewFocuses);
      handleSourceDigestDraftCreated(result, candidate.group, selectedTrimIds, reviewRow ?? undefined, undefined, candidate);
      setDirectSourceDigestTrimSelections((current) => {
        const next = { ...current };
        delete next[actionKey];
        return next;
      });
      setDirectSourceDigestPendingCandidates((current) => {
        const next = { ...current };
        delete next[actionKey];
        return next;
      });
      removeSourceDigestTrimIdentityDraft(actionKey);
      void loadTrims();
    } catch (err) {
      setSourceDigestDraftFeedback(err instanceof Error ? err.message : "创建可编辑配置列失败");
      setSourceDigestDraftSuccess(null);
    } finally {
      setSourceDigestDraftActionKey(null);
    }
  }

  async function selectSourceDigestCandidate(candidate: SourceDigestGroupCandidate): Promise<void> {
    await createSourceDigestCandidateDraft(candidate);
  }

  function toggleSourceDigestCandidateTrim(candidate: SourceDigestGroupCandidate, trimId: string): void {
    const key = sourceDigestCandidateKey(candidate);
    const selected = normaliseSourceDigestTrimSelection(candidate.group, sourceDigestTrimSelections[key]);
    const checked = selected.includes(trimId);
    if (checked && selected.length > 2) removeSourceDigestTrimIdentityDraft(key, trimId);
    setSourceDigestTrimSelections((current) => {
      const selected = normaliseSourceDigestTrimSelection(candidate.group, current[key]);
      const checked = selected.includes(trimId);
      if (checked && selected.length <= 2) return current;
      if (!checked && selected.length >= 4) return current;
      const next = checked ? selected.filter((id) => id !== trimId) : [...selected, trimId];
      return {
        ...current,
        [key]: next,
      };
    });
  }

  function handleSourceDigestDraftCreated(
    result: EngineeringConfigDigestDraftResult,
    group?: EngineeringConfigSourceDigestGroup,
    selectedTrimIds: string[] = [],
    reviewRowOverride?: EngineeringConfigSourceDigestGroup["rows"][number],
    sourceSnapshot?: EngineeringConfigSourceSnapshot | null,
    candidate?: SourceDigestGroupCandidate,
  ): void {
    const createdIds = uniqueCompareTrimIds(result.compareTrimIds).slice(0, 4);
    const appendToCurrentCompare = compareIds.length > 0 || sourceDigestDraftAppendsToCurrentCompare(sourceContextOverride);
    const comparePlacement = sourceDigestDraftComparePlacement(compareIds, createdIds, appendToCurrentCompare);
    const ids = comparePlacement.ids;
    const reviewRow = reviewRowOverride ?? (group ? firstSourceDigestReviewRow(group) : null);
    setActiveDigestGroupId(null);
    setCompareData(null);
    setActiveCategoryFilter(reviewRow?.category ?? null);
    setActiveDeltaFilter("ALL");
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
    setFocusedFeatureCode(null);
    const draftReviewFocus = reviewRow
      ? {
          category: reviewRow.category,
          featureCode: reviewRow.featureCode,
          featureName: reviewRow.featureName,
        }
      : null;
    setPendingDraftReviewFocus(draftReviewFocus);
    setSourceDigestDraftReviewFocus(draftReviewFocus);
    setCompareVersionScope("latest");
    setCompareIds(ids);
    setBaseTrimId(appendToCurrentCompare && baseTrimId && ids.includes(baseTrimId) ? baseTrimId : ids[0] ?? null);
    setActivePanel("display");
    if (group) {
      const placementFeedback = comparePlacement.omittedFromCurrentCompareCount === 0 && sourceDigestDraftAppendsToCurrentCompare(sourceContextOverride)
        ? "已追加到当前推荐竞品对比。"
        : sourceDigestDraftPlacementFeedback(comparePlacement);
      const successFeedback = [
        formatEngineeringConfigDigestDraftFeedback(group.modelName, result, selectedTrimIds.length > 0 ? selectedTrimIds.length : undefined),
        placementFeedback,
        reviewRow ? `已定位到需核对行：${reviewRow.featureName}` : null,
      ].filter((part): part is string => Boolean(part)).join(" ");
      setSourceDigestDraftFeedback(successFeedback);
      setSourceDigestDraftSuccess(sourceDigestDraftSuccessSummary(successFeedback, result, group, selectedTrimIds, comparePlacement, candidate, sourceSnapshot));
    } else {
      setSourceDigestDraftFeedback("已创建可编辑配置列。");
      setSourceDigestDraftSuccess(null);
    }
  }

  function replaceOmittedSourceDigestTrimInCompare(
    omittedTrim: SourceDigestDraftCompareTrim,
    replacedTrim: ComparableTrim,
  ): void {
    if (!sourceDigestDraftSuccess) return;
    if (!compareIds.includes(replacedTrim.trimId) || compareIds.includes(omittedTrim.trimId)) return;
    const nextIds = compareIds.map((id) => id === replacedTrim.trimId ? omittedTrim.trimId : id).slice(0, 4);
    const nextOmittedCompareTrims = sourceDigestDraftSuccess.omittedCompareTrims.filter((trim) => trim.trimId !== omittedTrim.trimId);
    const replacementNote = `${omittedTrim.label} 已替换 ${compareTrimLabel(replacedTrim)} 进入当前对比。`;
    const nextFeedback = `${sourceDigestDraftSuccess.feedback} ${replacementNote}`;
    const nextVisibleCount = sourceDigestDraftSuccess.createdCompareTrimCount - nextOmittedCompareTrims.length;
    const nextMetrics = sourceDigestDraftSuccess.metrics.flatMap((metric) => {
      if (metric.key === "visible") {
        return [{
          ...metric,
          value: `${nextVisibleCount}/${sourceDigestDraftSuccess.createdCompareTrimCount}`,
        }];
      }
      if (metric.key === "omitted") {
        return nextOmittedCompareTrims.length > 0
          ? [{ ...metric, value: String(nextOmittedCompareTrims.length) }]
          : [];
      }
      return [metric];
    });
    setActiveDigestGroupId(null);
    setCompareData(null);
    setCompareIds(nextIds);
    setBaseTrimId(baseTrimId === replacedTrim.trimId ? omittedTrim.trimId : baseTrimId);
    setActiveTargetTrimId(activeTargetTrimId === replacedTrim.trimId ? omittedTrim.trimId : activeTargetTrimId);
    setActiveDeltaFilter("ALL");
    setActiveTableSearch("");
    setFocusedFeatureCode(null);
    setSourceDigestDraftFeedback(nextFeedback);
    setSourceDigestDraftSuccess({
      ...sourceDigestDraftSuccess,
      feedback: nextFeedback,
      currentCompare: {
        ...sourceDigestDraftSuccess.currentCompare,
        headline: nextOmittedCompareTrims.length > 0
          ? "新配置列已部分加入当前对比表"
          : "新配置列已加入当前对比表",
        meta: `${replacementNote} · 当前 ${nextIds.length} 个配置列`,
      },
      omittedCompareTrims: nextOmittedCompareTrims,
      metrics: nextMetrics,
    });
  }

  function focusCategory(category: string | null): void {
    setActiveCategoryFilter(category);
    window.requestAnimationFrame(() => {
      if (category) scrollToCategory(category);
      else document.getElementById("config-compare-table")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }

  function scrollToCompareTable(): void {
    window.requestAnimationFrame(() => {
      document.getElementById("config-compare-table")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }

  function focusSourceDigestDraftReviewRow(): void {
    if (!sourceDigestDraftReviewFocus) return;
    const focusedRow = compareData?.rows.find((row) => row.featureCode === sourceDigestDraftReviewFocus.featureCode);
    if (!focusedRow) {
      setPendingDraftReviewFocus(sourceDigestDraftReviewFocus);
      scrollToCompareTable();
      return;
    }
    setActiveTableSearch("");
    setActiveTargetTrimId(null);
    setActiveDeltaFilter("ALL");
    setActiveCategoryFilter(focusedRow.category);
    setFocusedFeatureCode(focusedRow.featureCode);
    setFocusedFeatureRequestKey((value) => value + 1);
    scrollToCompareTable();
  }

  function focusDifferenceRows(): void {
    setTableDeltaFilter("DIFFERENCE");
    scrollToCompareTable();
  }

  function applyTargetAndDeltaFilter(trimId: string | null, nextDeltaFilter: ConfigComparisonDeltaFilter): void {
    if (displayCompareData && activeCategoryFilter) {
      const nextCategories = visibleCategoriesForDeltaFilter(displayCompareData, baseTrimId, nextDeltaFilter, activeTableSearch, trimId);
      if (!nextCategories.includes(activeCategoryFilter)) setActiveCategoryFilter(null);
    }
    setActiveTargetTrimId(trimId);
    if (nextDeltaFilter !== activeDeltaFilter) setTableDeltaFilter(nextDeltaFilter, trimId);
  }

  function focusTargetTrim(trimId: string | null): void {
    applyTargetAndDeltaFilter(trimId, activeDeltaFilter);
    scrollToCompareTable();
  }

  function focusTargetDifference(trimId: string | null): void {
    applyTargetAndDeltaFilter(trimId, trimId ? "DIFFERENCE" : activeDeltaFilter);
    scrollToCompareTable();
  }

  function focusBusinessSummaryScope(): void {
    if (activeDeltaFilter === "ALL") setTableDeltaFilter("DIFFERENCE");
    scrollToCompareTable();
  }

  function focusBusinessDeltaType(filter: ConfigComparisonDeltaFilter, targetTrimId: string | null): void {
    setActiveTargetTrimId(targetTrimId);
    setTableDeltaFilter(filter, targetTrimId);
    scrollToCompareTable();
  }

  function focusBusinessFeatureRow(row: CompareRow, targetTrimId: string | null, filter: ConfigComparisonDeltaFilter): void {
    setActiveTableSearch("");
    setActiveTargetTrimId(targetTrimId);
    setActiveDeltaFilter(filter);
    setActiveCategoryFilter(row.category);
    setFocusedFeatureCode(row.featureCode);
    setFocusedFeatureRequestKey((value) => value + 1);
    scrollToCompareTable();
  }

  function focusVersionUpgradeStep(nextBaseTrimId: string, nextTargetTrimId: string, filter: ConfigComparisonDeltaFilter): void {
    if (displayCompareData && activeCategoryFilter) {
      const nextCategories = visibleCategoriesForDeltaFilter(displayCompareData, nextBaseTrimId, filter, activeTableSearch, nextTargetTrimId);
      if (!nextCategories.includes(activeCategoryFilter)) setActiveCategoryFilter(null);
    }
    setBaseTrimId(nextBaseTrimId);
    setActiveTargetTrimId(nextTargetTrimId);
    setActiveDeltaFilter(filter);
    scrollToCompareTable();
  }

  function showDifferenceScope(): void {
    setTableDeltaFilter("DIFFERENCE");
    scrollToCompareTable();
  }

  function restoreAllConfigRows(): void {
    setActiveCategoryFilter(null);
    setActiveTableSearch("");
    setActiveDeltaFilter("ALL");
    scrollToCompareTable();
  }

  function showAllTargetColumns(): void {
    setActiveTargetTrimId(null);
    scrollToCompareTable();
  }

  function resetHeroScope(): void {
    if (rowScopeActive) restoreAllConfigRows();
    else showAllTargetColumns();
  }

  function clearAnalysisScope(): void {
    setActiveTargetTrimId(null);
    setActiveCategoryFilter(null);
    setActiveTableSearch("");
    setActiveDeltaFilter("ALL");
    scrollToCompareTable();
  }

  function renderSummaryModeOptions(ariaLabel: string): ReactElement {
    return (
      <div className="product-config-summary-mode__options" role="group" aria-label={ariaLabel}>
        {SUMMARY_MODE_OPTIONS.map((option) => (
          <button
            className={summaryMode === option.key ? "is-active" : ""}
            type="button"
            key={option.key}
            aria-pressed={summaryMode === option.key}
            onClick={() => setSummaryMode(option.key)}
          >
            <span>{option.label}</span>
            <small>{option.description}</small>
          </button>
        ))}
      </div>
    );
  }

  function renderComparisonSummarySection(): ReactElement | null {
    if (!summary) return null;
    return (
      <section
        className={`comparison-summary ${summaryMode === "simple" && displayCompareData ? "is-compact product-config-table-status" : ""}`.trim()}
        aria-label={summaryMode === "simple" && displayCompareData ? "Excel 配置表状态" : "配置对比摘要"}
      >
        <div className="comparison-summary-card comparison-summary-card--wide">
          <span>对比对象</span>
          <strong>{selectedDisplayTrims.map((trim) => trim.trimName || trim.fullTrimName).join(" vs ")}</strong>
          <small>{baseTrim ? `${simpleModeActive ? "基准列" : "基准"}：${baseTrim.trimName || baseTrim.fullTrimName}` : `未设置${simpleModeActive ? "基准列" : "基准"}`}</small>
        </div>
        <div className="comparison-summary-card">
          <span>{simpleModeActive ? "总配置行" : "配置项"}</span>
          <strong>{summary.totalFeatures}</strong>
          <small>{summaryShownLabel}</small>
        </div>
        <div className="comparison-summary-card">
          <span>{scopePrimaryMetric.label}</span>
          <strong>{scopePrimaryMetric.value}</strong>
          <small>{scopePrimaryMetric.hint}</small>
        </div>
        <div className="comparison-summary-card">
          <span>可用性差异</span>
          <strong>{scopedSummaryMetrics.availabilityDifferenceCount}</strong>
          <small>共同配置 {scopedSummaryMetrics.commonSameCount}，值不同 {scopedSummaryMetrics.valueChangedCount}</small>
        </div>
        <div className="comparison-summary-card">
          <span>待确认</span>
          <strong>{scopedSummaryMetrics.missingUnknownCount}</strong>
          <small>
            {summaryMode === "simple"
              ? "空值 / 缺失需核对；推断 / 合并格见更多证据筛选"
              : `缺源值 ${sourceEvidenceMetrics.missingSourceValueCount}，缺值 ${sourceEvidenceMetrics.missingValueCellCount}，推断 ${sourceEvidenceMetrics.inferredCellCount}，合并格 ${sourceEvidenceMetrics.mergedCellCount}`}
          </small>
          {summaryMode === "expert" ? (
            <div className="comparison-evidence-quick-filters" aria-label="证据健康度快捷筛选">
              <button
                type="button"
                aria-label={`查看来源问题证据：缺源值 ${sourceEvidenceMetrics.missingSourceValueCount}，缺值 ${sourceEvidenceMetrics.missingValueCellCount}`}
                onClick={() => setTableDeltaFilter("MISSING_SOURCE")}
              >
                来源问题 {sourceEvidenceMetrics.sourceIssueCellCount}
              </button>
              <button
                type="button"
                aria-label={`查看规则推断差异：推断差异 ${scopedSummaryMetrics.inferredDifferenceCount}`}
                onClick={() => setTableDeltaFilter("INFERRED")}
              >
                推断差异 {scopedSummaryMetrics.inferredDifferenceCount}
              </button>
              <button
                type="button"
                aria-label={`查看合并格证据：合并格 ${sourceEvidenceMetrics.mergedCellCount}`}
                onClick={() => setTableDeltaFilter("MERGED_SOURCE")}
              >
                合并 {sourceEvidenceMetrics.mergedCellCount}
              </button>
            </div>
          ) : null}
        </div>
      </section>
    );
  }

  function renderBusinessSummaryPanel(): ReactElement | null {
    if (!displayCompareData) return null;
    if (compareLoading) return null;
    if (!businessSummaryPanelReady) return null;
    return (
      <Suspense fallback={<LoadingSurface mode="inline" label={summaryMode === "simple" ? "加载 AI 配置摘要" : "加载业务摘要"} />}>
        <LazyBusinessSummaryPanel
          data={displayCompareData}
          baseTrimId={baseTrimId}
          categoryFilter={activeCategoryFilter}
          deltaFilter={activeDeltaFilter}
          mode={summaryMode}
          searchValue={activeTableSearch}
          targetTrimFilterId={activeTargetTrimId}
          factSource={compareFactSource}
          llmSummaryEnabled
          onShowDifferenceRows={focusBusinessSummaryScope}
          onFocusCategory={focusCategory}
          onFocusDeltaType={focusBusinessDeltaType}
          onFocusFeatureRow={focusBusinessFeatureRow}
          onFocusTargetTrim={focusTargetDifference}
          onFocusVersionStep={focusVersionUpgradeStep}
          onLlmSummaryChange={handleLlmSummaryChange}
          onOpenSourceContext={openSourcePanelForTarget}
          onOpenEvidence={setEvidenceSelection}
        />
      </Suspense>
    );
  }

  function renderInlineSummaryModeSwitch(): ReactElement | null {
    if (!displayCompareData) return null;
    return (
      <section className="product-config-summary-mode is-compact product-config-summary-mode--inline" aria-label="配置摘要模式">
        <div>
          <span>Summary mode</span>
          <strong>{summaryMode === "simple" ? "简易模式" : "专家模式"}</strong>
            <small>
              {summaryMode === "simple"
                ? "AI 结论优先，高级诊断和来源口径收进专家模式。"
                : "显示高级诊断、规则推断、升级路径和来源样本。"}
            </small>
        </div>
        {renderSummaryModeOptions("摘要区切换配置对比视图模式")}
      </section>
    );
  }

  function renderHeroSummaryModeSwitch(): ReactElement | null {
    if (!displayCompareData) return null;
    return (
      <div className="product-config-hero-mode-switch" role="group" aria-label="页面配置对比模式">
        {SUMMARY_MODE_OPTIONS.map((option) => (
          <button
            className={summaryMode === option.key ? "is-active" : ""}
            type="button"
            key={option.key}
            aria-pressed={summaryMode === option.key}
            title={option.description}
            onClick={() => setSummaryMode(option.key)}
          >
            {option.key === "simple" ? "简易" : "专家"}
          </button>
        ))}
      </div>
    );
  }

  function renderIdentityNotes(): ReactElement | null {
    if (!comparisonScenario && identityNotes.length === 0) return null;
    return (
      <section className="product-config-identity-notes" aria-label="配置对比身份锚点提醒">
        {comparisonScenario ? (
          <div className="product-config-identity-note product-config-identity-note--scenario" key={comparisonScenario.key}>
            <span>{comparisonScenario.label}</span>
            <small>{comparisonScenario.detail}</small>
          </div>
        ) : null}
        {identityNotes.map((note) => (
          <div className="product-config-identity-note" key={note.key}>
            <span>{note.label}</span>
            <small>{note.detail}</small>
            {note.key === "missing-source" ? (
              <button className="btn btn-xs btn-secondary" type="button" onClick={openMissingSourcePanel}>
                补充来源 / 上传 Source Digest
              </button>
            ) : null}
          </div>
        ))}
      </section>
    );
  }

  function renderLocalDigestSection(): ReactElement | null {
    if (localComparableGroups.length > 0) {
      return (
        <section className="product-config-local-digest">
          <div className="product-config-local-digest__header">
            <div>
              <span className="market-scan-panel-eyebrow">Source Digest</span>
              <strong>{localDigest?.fileName || LOCAL_CONFIG_WORKBOOK_FILE}</strong>
              <small>{localDigest?.summary.sheetCount ?? 0} sheets · {localDigest?.summary.candidateTrimCount ?? 0} 候选配置列 · {localDigest?.summary.comparableGroupCount ?? 0} 组可比</small>
            </div>
            <button className="btn btn-sm btn-secondary" type="button" onClick={openComparisonSourcePanel}>{SOURCE_UPLOAD_CTA_LABEL}</button>
          </div>
          <details className="product-config-local-digest__switcher" open={localDigestSwitcherOpen}>
            <summary className="product-config-local-digest__summary">
              <span>来源样例</span>
              <strong>
                {activeDigestGroup
                  ? `${activeDigestGroup.modelName} · ${activeDigestGroup.trimCount} 配置列 · ${activeDigestGroup.differenceCount} 差异`
                  : `${localComparableGroups.length} 组可比`}
              </strong>
              <small>{localDigestSwitcherOpen ? "当前展开全部本地 xlsx 可比组" : "展开切换其他本地 xlsx 样例"}</small>
            </summary>
            <div className="product-config-local-digest__groups">
              {localComparableGroupEntries.map(({ group, key }) => (
                <button
                  className={`product-config-local-digest__group ${key === activeDigestGroupId ? "is-active" : ""}`}
                  type="button"
                  key={key}
                  data-testid="local-digest-group"
                  data-source-sheet={group.sourceSheet}
                  onClick={() => selectDigestGroup(group, undefined, key)}
                >
                  <strong>{group.modelName}</strong>
                  <span>{group.trimCount} 配置列 · {group.differenceCount} 差异 · {group.sourceSheet}</span>
                  <small>{group.trims.slice(0, 3).map((trim) => trim.trimName).join(" / ")}</small>
                </button>
              ))}
            </div>
          </details>
        </section>
      );
    }
    if (localDigestLoading) {
      return (
        <section className="product-config-local-digest product-config-local-digest--loading" aria-label="本地 xlsx 样例加载中">
          <div className="product-config-local-digest__header">
            <div>
              <span className="market-scan-panel-eyebrow">Source Digest</span>
              <strong>正在准备本地 xlsx 样例</strong>
              <small className="market-scan-field-hint">页面先加载配置列库；样例 digest 会在首屏稳定后拉取，不阻塞 FloatingDeck 搜索和上传。</small>
            </div>
            <button className="btn btn-sm btn-secondary" type="button" onClick={openComparisonSourcePanel}>{SOURCE_UPLOAD_CTA_LABEL}</button>
          </div>
        </section>
      );
    }
    if (!localDigestError) return null;
    return (
      <section className="product-config-local-digest product-config-local-digest--empty">
        <div className="product-config-local-digest__header">
          <div>
            <span className="market-scan-panel-eyebrow">Source Digest</span>
            <strong>来源样例暂不可用</strong>
            <small className="market-scan-field-hint">本地 xlsx 样例暂不可用：{localDigestError}</small>
          </div>
          <button className="btn btn-sm btn-secondary" type="button" onClick={openComparisonSourcePanel}>{SOURCE_UPLOAD_CTA_LABEL}</button>
        </div>
      </section>
    );
  }

  function renderCategoryNav(): ReactElement | null {
    if (categoryNavItems.length === 0) return null;
    if (simpleModeActive) return null;
    const categoryButtons = (
      <div>
        <button
          aria-label={`全部大类，当前范围 ${tableScopeRows.length} ${visibleItemUnit}`}
          className={!activeCategoryFilter ? "is-active" : ""}
          type="button"
          onClick={() => focusCategory(null)}
        >
          <span>全部大类</span>
          <small>{tableScopeRows.length} {visibleItemUnit}</small>
        </button>
        {categoryNavItems.slice(0, 14).map(({ category, count }) => (
          <button
            aria-label={`${categoryDisplayLabel(category)}，当前范围 ${count} ${visibleItemUnit}`}
            className={activeCategoryFilter === category ? "is-active" : ""}
            key={category}
            type="button"
            onClick={() => focusCategory(category)}
          >
            <span>{categoryDisplayLabel(category)}</span>
            <small>{count} {visibleItemUnit}</small>
          </button>
        ))}
      </div>
    );
    return (
      <section className="product-config-category-nav">
        <span>配置大类</span>
        {categoryButtons}
      </section>
    );
  }

  function openControlPanel(panel: ProductConfigPanel): void {
    setActivePanel(panel);
    setControlOpen(true);
    if (panel === "filters") {
      setDirectPickerFocusRequest((current) => current + 1);
    }
  }

  function openSourceContextBindingPanel(): void {
    setSourceContextOverride(null);
    setSourceContextBindingPromptOpen(true);
    setSimpleAdvancedSearchOpen(true);
    openControlPanel("filters");
  }

  function returnToSourceUploadAfterContextBinding(): void {
    setSourceContextBindingPromptOpen(false);
    openControlPanel("source");
  }

  function openComparisonSourcePanel(): void {
    setSourceContextOverride(null);
    setSourceDigestSearchQuery("");
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(false);
    openControlPanel("source");
  }

  function openSourcePanelForTarget(trimId: string | null): void {
    setSourceContextOverride(null);
    if (trimId) setActiveTargetTrimId(trimId);
    openControlPanel("source");
  }

  function openMissingSourcePanel(): void {
    const searchQuery = sourceDigestSearchQueryForTrim(missingSourceTargetTrim);
    if (searchQuery) setSourceDigestSearchQuery(searchQuery);
    setSourceDigestDirectPickerValue("");
    setFocusedSourceDigestSourceId(null);
    setPendingSourceDigestFocusSourceId(null);
    setSourceDigestQualityFilter("all");
    setSourceDigestBrowseExpanded(false);
    setSourceDigestDetailBrowserOpen(true);
    openSourcePanelForTarget(missingSourceTargetTrim?.trimId ?? null);
  }

  const competitorRecommendationsReadyCount = competitorRecommendations.filter((recommendation) => recommendation.configAvailable).length;
  const competitorRecommendationsDigestReadyCount = competitorRecommendations.filter((recommendation) => !recommendation.configAvailable && recommendation.sourceDigestAvailable).length;
  const competitorRecommendationsMissingCount = Math.max(competitorRecommendations.length - competitorRecommendationsReadyCount - competitorRecommendationsDigestReadyCount, 0);
  const competitorRecommendationDigestQueue = competitorRecommendations.filter((recommendation) => !recommendation.configAvailable && recommendation.sourceDigestAvailable);
  const competitorRecommendationMissingQueue = competitorRecommendations.filter((recommendation) => !recommendation.configAvailable && !recommendation.sourceDigestAvailable);
  const competitorRecommendationScopeText = [
    recommendationCountry || filters.market || null,
    recommendationPowertrain || filters.powertrain || null,
    recommendationSegment || filters.segment || null,
  ].filter((value): value is string => Boolean(value)).join(" · ") || "当前筛选口径";

  function competitorSourceLookup(recommendation: EngineeringConfigCompetitorRecommendation): {
    sourceSearchQuery: string;
    targetCountry: string | null;
    targetPowertrain: string | null;
    targetSegment: string | null;
  } {
    const targetCountry = recommendationCountry || filters.market || null;
    const targetPowertrain = recommendationPowertrain || recommendationProfileText(recommendation, "powertrain");
    const targetSegment = recommendationSegment || recommendationProfileText(recommendation, "segment");
    const sourceSearchQuery = uniquePresent([
      recommendation.brand,
      recommendation.modelName,
      targetCountry,
      targetPowertrain,
      targetSegment,
    ]).join(" ");
    return {
      sourceSearchQuery,
      targetCountry,
      targetPowertrain,
      targetSegment,
    };
  }

  function renderCompetitorRecommendationQueue(): ReactElement | null {
    if (competitorRecommendations.length === 0) return null;
    const firstDigestRecommendation = competitorRecommendationDigestQueue[0] ?? null;
    const firstMissingRecommendation = competitorRecommendationMissingQueue[0] ?? null;
    const firstReadyRecommendation = competitorRecommendations.find((recommendation) => (
      recommendation.configAvailable
      && recommendation.trims.some((trim) => !compareIds.includes(trim.trimId))
    )) ?? null;
    const readyRecommendationSlots = Math.max(4 - compareIds.length, 0);
    const readyRecommendationAddableTrims = firstReadyRecommendation
      ? firstReadyRecommendation.trims
        .filter((trim) => !compareIds.includes(trim.trimId))
        .slice(0, readyRecommendationSlots)
      : [];
    const nextRecommendation = firstDigestRecommendation ?? firstMissingRecommendation ?? firstReadyRecommendation;
    if (!nextRecommendation) return null;
    const nextModelLabel = nextRecommendation.modelName || "推荐竞品";
    const queueMode = firstDigestRecommendation
      ? "digest"
      : firstMissingRecommendation
        ? "missing"
        : "ready";
    const nextActionLabel = queueMode === "digest"
      ? `生成 ${nextModelLabel} 配置列`
      : queueMode === "missing"
        ? `上传 ${nextModelLabel} 来源`
        : `加入 ${nextModelLabel} 库内列`;
    const nextActionHint = queueMode === "digest"
      ? `来源库已有 ${nextRecommendation.sourceDigestGroupCount ?? 0} 组，先转成可编辑配置列。`
      : queueMode === "missing"
        ? "库内和来源库都缺资料，先上传配置表或价格单。"
        : readyRecommendationAddableTrims.length > 0
          ? `当前还有 ${readyRecommendationSlots} 个空位，可加入 ${readyRecommendationAddableTrims.length} 个配置列。`
          : "当前对比已满 4 列，先移除不需要的配置列。";
    const nextActionDisabled = queueMode === "ready" && readyRecommendationAddableTrims.length === 0;
    const handleNextRecommendationAction = (): void => {
      if (queueMode === "ready") {
        if (readyRecommendationAddableTrims.length > 0) addRecommendedTrims(readyRecommendationAddableTrims);
        return;
      }
      openSourcePanelForCompetitor(nextRecommendation);
    };
    return (
      <div className={`comparison-competitor-queue is-${queueMode}`} aria-label="推荐竞品补齐队列">
        <div className="comparison-competitor-queue__lead">
          <span>补齐队列</span>
          <strong>
            {queueMode === "digest"
              ? "优先处理 Source Digest 待生成"
              : queueMode === "missing" ? "优先补上传缺口" : "库内配置列可直接加入"}
          </strong>
          <small>{nextModelLabel} · {nextActionHint}</small>
        </div>
        <div className="comparison-competitor-queue__counts" aria-label="推荐竞品补齐状态">
          <span className="is-ready"><small>库内可用</small><strong>{competitorRecommendationsReadyCount}</strong></span>
          <span className={competitorRecommendationsDigestReadyCount > 0 ? "is-warning" : "is-ready"}><small>待生成</small><strong>{competitorRecommendationsDigestReadyCount}</strong></span>
          <span className={competitorRecommendationsMissingCount > 0 ? "is-warning" : "is-ready"}><small>待上传</small><strong>{competitorRecommendationsMissingCount}</strong></span>
        </div>
        <button
          className="btn btn-sm btn-primary"
          type="button"
          disabled={nextActionDisabled}
          onClick={handleNextRecommendationAction}
        >
          {nextActionLabel}
        </button>
      </div>
    );
  }

  function renderCompetitorRecommendationCard(recommendation: EngineeringConfigCompetitorRecommendation): ReactElement {
    const visibleTrims = recommendation.trims.slice(0, 3);
    const selectedRecommendationTrimCount = recommendation.trims.filter((trim) => compareIds.includes(trim.trimId)).length;
    const remainingCompareSlots = Math.max(4 - compareIds.length, 0);
    const addableRecommendationTrims = recommendation.trims
      .filter((trim) => !compareIds.includes(trim.trimId))
      .slice(0, remainingCompareSlots);
    const recommendationAddLabel = selectedRecommendationTrimCount === recommendation.trims.length
      ? "已全部加入"
      : remainingCompareSlots === 0
        ? "已满 4 列"
        : `加入库内配置列 ${addableRecommendationTrims.length}`;
    const recommendationAddHint = selectedRecommendationTrimCount === recommendation.trims.length
      ? "这些推荐配置列已在当前对比中。"
      : remainingCompareSlots === 0
        ? "当前对比已达 4 列上限，先移除不需要的配置列。"
        : `按当前空位加入 ${addableRecommendationTrims.length} 个库内配置列。`;
    const profilePowertrain = recommendationProfileText(recommendation, "powertrain");
    const profileSegment = recommendationProfileText(recommendation, "segment");
    const lookup = competitorSourceLookup(recommendation);
    const uploadCountry = lookup.targetCountry || "当前国家";
    const uploadPowertrain = lookup.targetPowertrain || "当前动力";
    const uploadModelLabel = recommendation.modelName || "推荐竞品";
    const sourceDigestCoverageAvailable = Boolean(recommendation.sourceDigestAvailable);
    const sourceDigestDraftNeeded = !recommendation.configAvailable && sourceDigestCoverageAvailable;
    const sourceLookupLabel = recommendation.sourceDigestSearchQuery || lookup.sourceSearchQuery || uploadModelLabel;
    const sourceDigestMatches = recommendation.sourceDigestMatches ?? [];
    const visibleSourceDigestMatches = sourceDigestMatches.slice(0, 3);
    const hiddenSourceDigestMatchCount = Math.max(sourceDigestMatches.length - visibleSourceDigestMatches.length, 0);
    const sourceDigestCoverageSummary = [
      (recommendation.sourceDigestSourceCount ?? sourceDigestMatches.length) > 0 ? `${recommendation.sourceDigestSourceCount ?? sourceDigestMatches.length} 来源` : null,
      (recommendation.sourceDigestGroupCount ?? 0) > 0 ? `${recommendation.sourceDigestGroupCount} 组` : null,
      (recommendation.sourceDigestTrimCount ?? 0) > 0 ? `${recommendation.sourceDigestTrimCount} 配置列` : null,
    ].filter((item): item is string => Boolean(item)).join(" · ");
    const recommendationEvidenceLabels = competitorRecommendationEvidenceLabels(recommendation);
    const migrationMetricItems = competitorMigrationMetricItems(recommendation);
    const cardStateClass = recommendation.configAvailable ? "is-ready" : sourceDigestDraftNeeded ? "is-digest" : "is-missing";
    const configCoverageLabel = recommendation.configAvailable
      ? `库内 ${recommendation.configTrimCount} 配置列`
      : sourceDigestDraftNeeded
        ? `来源库 ${recommendation.sourceDigestGroupCount ?? 0} 组待生成`
        : "库内缺失";
    function renderCompetitorSourceDigestMatches(): ReactElement | null {
      if (!sourceDigestCoverageAvailable || visibleSourceDigestMatches.length === 0) return null;
      return (
        <div className="comparison-competitor-source-matches" aria-label={`${uploadModelLabel} Source Digest 命中来源`}>
          <span>命中来源</span>
          {visibleSourceDigestMatches.map((match) => {
            const matchLabel = competitorSourceDigestMatchLabel(match);
            return (
              <button
                className="comparison-competitor-source-match"
                key={`${match.sourceId}-${matchLabel}`}
                type="button"
                aria-label={`按来源 ${matchLabel} 搜索 Source Digest`}
                onClick={() => openSourcePanelForCompetitor(recommendation, match)}
              >
                <strong>{matchLabel}</strong>
                <small>{competitorSourceDigestMatchMeta(match)}</small>
              </button>
            );
          })}
          {hiddenSourceDigestMatchCount > 0 ? (
            <button
              className="comparison-competitor-source-match-more"
              type="button"
              aria-label={`查看全部 ${hiddenSourceDigestMatchCount} 个 ${uploadModelLabel} 命中来源`}
              onClick={() => openSourcePanelForCompetitor(recommendation)}
            >
              +{hiddenSourceDigestMatchCount} 个来源 · 查看全部
            </button>
          ) : null}
        </div>
      );
    }
    return (
      <article className={`comparison-competitor-card ${cardStateClass}`} key={`${recommendation.rank}-${recommendation.modelName}`}>
        <header>
          <span>#{recommendation.rank} {recommendation.brand || "Brand 待补"}</span>
          <strong>{recommendation.modelName}</strong>
          <small>{competitorRecommendationRelevanceLabel(recommendation, competitorRecommendationSource)} · dV {signedCompactNumber(recommendation.deltaVolume)} · 份额 {percentLabel(recommendation.shareTarget)}</small>
        </header>
        {migrationMetricItems.length > 0 ? (
          <div className="comparison-competitor-migration" aria-label={`${uploadModelLabel} 高级分析蝴蝶图迁移指标`}>
            <span>AA 迁移指标</span>
            {migrationMetricItems.map((item) => (
              <small key={item.label}>
                {item.label}
                {" "}
                <strong>{item.value}</strong>
              </small>
            ))}
          </div>
        ) : null}
        <p>{recommendation.recommendationReason}</p>
        <div className="comparison-competitor-meta" aria-label={`${uploadModelLabel} 推荐依据`}>
          <span>推荐依据</span>
          {recommendationEvidenceLabels.map((label) => (
            <span key={label}>{label}</span>
          ))}
        </div>
        <div className="comparison-competitor-meta">
          <span>{profileSegment || "segment 待补"}</span>
          <span>{profilePowertrain || "动力待补"}</span>
          <span>{configCoverageLabel}</span>
          {recommendation.configAvailable && sourceDigestCoverageSummary ? <span>来源库 {sourceDigestCoverageSummary}</span> : null}
        </div>
        {visibleTrims.length > 0 ? (
          <div className="comparison-competitor-trims">
            {visibleTrims.map((trim) => {
              const selected = compareIds.includes(trim.trimId);
              const disabled = !selected && compareIds.length >= 4;
              return (
                <button
                  className={`comparison-competitor-trim ${selected ? "is-selected" : ""}`}
                  type="button"
                  key={trim.trimId}
                  disabled={disabled}
                  onClick={() => toggleRecommendedTrim(trim)}
                >
                  <span>{trim.materialNo || trim.salesVersion || trim.trimName || trim.fullTrimName}</span>
                  <small>{selected ? "移除" : disabled ? "最多 4 个" : "加入对比"}</small>
                </button>
              );
            })}
            {recommendation.trims.length > visibleTrims.length ? (
              <small className="comparison-competitor-more">+{recommendation.trims.length - visibleTrims.length} 个配置列可在候选列表继续筛选</small>
            ) : null}
            <div className="comparison-competitor-actions">
              <button
                className="btn btn-sm btn-primary"
                type="button"
                disabled={addableRecommendationTrims.length === 0}
                onClick={() => addRecommendedTrims(addableRecommendationTrims)}
              >
                {recommendationAddLabel}
              </button>
              <small>{recommendationAddHint}</small>
            </div>
            {sourceDigestCoverageAvailable ? (
              <>
                <div className="comparison-competitor-source-query" aria-label={`${uploadModelLabel} 来源库检索词`}>
                  <span>已入库来源</span>
                  <strong>{sourceLookupLabel}</strong>
                </div>
                {renderCompetitorSourceDigestMatches()}
                <button className="btn btn-sm btn-secondary" type="button" onClick={() => openSourcePanelForCompetitor(recommendation)}>
                  核对 {uploadModelLabel} 来源
                </button>
              </>
            ) : null}
          </div>
        ) : (
          <div className={`comparison-competitor-missing-source ${sourceDigestDraftNeeded ? "is-digest-ready" : ""}`.trim()}>
            <span>{sourceDigestDraftNeeded ? "来源库已有" : "配置资料缺口"}</span>
            <strong>
              {sourceDigestDraftNeeded
                ? `${uploadModelLabel} 有 ${recommendation.sourceDigestGroupCount ?? 0} 个 Source Digest 可比组`
                : `${uploadModelLabel} 暂无库内配置列`}
            </strong>
            <small>
              {sourceDigestDraftNeeded
                ? `先打开来源库检索，创建 ${recommendation.sourceDigestTrimCount ?? 0} 个候选配置列后再加入对比。`
                : `先搜索来源库，未命中再上传 ${uploadCountry} / ${uploadPowertrain} 的配置表或价格单；Digest 后可转成可编辑配置列。`}
            </small>
            <div className="comparison-competitor-source-query" aria-label={`${uploadModelLabel} 来源库检索词`}>
              <span>自动检索</span>
              <strong>{sourceLookupLabel}</strong>
            </div>
            {renderCompetitorSourceDigestMatches()}
            <button className="btn btn-sm btn-secondary" type="button" onClick={() => openSourcePanelForCompetitor(recommendation)}>
              {sourceDigestDraftNeeded ? `打开 ${uploadModelLabel} Source Digest` : `搜索 / 上传 ${uploadModelLabel} 来源`}
            </button>
          </div>
        )}
      </article>
    );
  }

  function countRowsForDisplayScope(filter: ConfigComparisonDeltaFilter): number {
    if (!displayCompareData) return 0;
    return displayCompareData.rows.filter((row) => (
      rowMatchesConfigScope(displayCompareData, row, filter, baseTrimId, activeTargetTrimId)
      && rowMatchesConfigSearch(row, activeTableSearch)
      && (!activeCategoryFilter || row.category === activeCategoryFilter)
    )).length;
  }

  function selectDisplayScope(filter: ConfigComparisonDeltaFilter): void {
    setTableDeltaFilter(filter);
    scrollToCompareTable();
  }

  function selectTargetTrimScope(value: string): void {
    focusTargetTrim(value && value !== ALL_TARGET_TRIMS_VALUE ? value : null);
  }

  function renderSourceDigestActiveScopeStrip(): ReactElement | null {
    if (sourceDigestActiveScopeItems.length === 0) return null;
    const clearableItems = sourceDigestActiveScopeItems.filter((item) => Boolean(item.onClear));
    const coverageText = sourceDigestLibrarySearchActive
      ? `${sourceDigestVisibleCoverage.sourceCount} 来源 · ${sourceDigestVisibleCoverage.modelCount} 车型 · ${sourceDigestVisibleCoverage.trimCount} 可比配置列`
      : "本地样例可预览，来源库需先搜索";
    const sourceScopeLabel = simpleModeActive ? "当前来源范围" : "当前 Source 范围";
    return (
      <div className="product-config-source-scope-strip" aria-label={simpleModeActive ? "来源当前搜索范围" : "Source Digest 当前搜索范围"}>
        <div className="product-config-source-scope-strip__head">
          <span>{sourceScopeLabel}</span>
          <small>{coverageText}</small>
        </div>
        <div className="product-config-source-scope-strip__items">
          {sourceDigestActiveScopeItems.map((item) => {
            const className = `product-config-source-scope-chip is-${item.tone}`;
            if (!item.onClear) {
              return (
                <span className={className} key={item.key}>
                  <small>{item.label}</small>
                  <strong>{item.value}</strong>
                </span>
              );
            }
            return (
              <button
                className={className}
                type="button"
                key={item.key}
                aria-label={`${simpleModeActive ? "清除来源搜索条件" : "清除 Source Digest 搜索条件"}：${item.label} ${item.value}`}
                title={item.clearLabel}
                onClick={item.onClear}
              >
                <small>{item.label}</small>
                <strong>{item.value}</strong>
                <em>清除</em>
              </button>
            );
          })}
        </div>
        {clearableItems.length > 1 ? (
          <button
            className="btn btn-sm btn-secondary"
            type="button"
            onClick={() => {
              setSourceDigestSearchQuery("");
              setSourceDigestDirectPickerValue("");
              setFocusedSourceDigestSourceId(null);
              setPendingSourceDigestFocusSourceId(null);
              setSourceDigestQualityFilter("all");
              setSourceDigestBrowseExpanded(false);
            }}
          >
            {simpleModeActive ? "清空来源搜索" : "清空 Source 搜索"}
          </button>
        ) : null}
      </div>
    );
  }

  function renderSourceDigestPathPreview(): ReactElement | null {
    if (sourceDigestPathPreviewAllGroups.length === 0) return null;
    const leadingPathGroup = sourceDigestPathPreviewAllGroups[0];
    return (
      <div className={`product-config-source-path-preview ${sourceDigestPathPreviewCompact ? "is-compact" : ""}`.trim()} aria-label="Source Digest 命中路径预览">
        <div className="product-config-source-path-preview__head">
          <div>
            <span>命中路径</span>
            <small>
              {sourceDigestPathPreviewAllGroups.length} 个来源路径 · {sourceDigestVisibleCoverage.modelCount} 车型 · {sourceDigestVisibleCoverage.trimCount} 可比配置列
              {sourceDigestPathPreviewCompact
                ? " · 路径卡已收起"
                : sourceDigestPathPreviewHiddenCount > 0 ? ` · 还有 ${sourceDigestPathPreviewHiddenCount} 个路径可展开查看` : ""}
            </small>
          </div>
          <div className="product-config-source-path-preview__actions">
            {sourceDigestPathPreviewCompact || sourceDigestPathPreviewHiddenCount > 0 ? (
              <button
                className="btn btn-sm btn-secondary"
                type="button"
                onClick={() => {
                  setSourceDigestBrowseExpanded(true);
                  setSourceDigestDetailBrowserOpen(true);
                }}
              >
                展开全部路径
              </button>
            ) : null}
            {focusedSourceDigestSourceId ? (
              <button
                className="btn btn-sm btn-secondary"
                type="button"
                onClick={clearFocusedSourceDigestSource}
              >
                解除来源聚焦
              </button>
            ) : null}
          </div>
        </div>
        {sourceDigestPathPreviewCompact ? (
          <div className="product-config-source-path-preview__compact" aria-label="Source Digest 路径摘要">
            <span>优先命中</span>
            <strong>{leadingPathGroup?.modelLabel ?? "车型待选"}</strong>
            <small>
              {leadingPathGroup
                ? `${leadingPathGroup.sourceFileName} · ${leadingPathGroup.coverage.trimCount} 可比配置列 · ${leadingPathGroup.coverage.differenceCount} 差异`
                : "先搜索来源或车型，再选择配置列。"}
            </small>
          </div>
        ) : (
        <div className="product-config-source-path-preview__items">
          {sourceDigestPathPreviewVisibleGroups.map((group) => (
            <article
              className="product-config-source-path-preview__item"
              key={group.key}
            >
              <div
                className="product-config-source-path-preview__flow"
                aria-label={`${group.sourceFileName} 来源车型配置列路径`}
              >
                {sourceDigestBrowsePathStages(group).map((stage) => (
                  <span className="product-config-source-path-preview__stage" key={stage.key}>
                    <small>{stage.label}</small>
                    <strong>{stage.value}</strong>
                    <em>{stage.meta}</em>
                  </span>
                ))}
              </div>
              <button
                className="product-config-source-path-preview__focus"
                disabled={!group.sourceId}
                type="button"
                aria-label={`聚焦 Source Digest 来源 ${group.sourceFileName}`}
                onClick={() => {
                  if (group.sourceId) {
                    setPendingSourceDigestFocusSourceId(null);
                    setFocusedSourceDigestSourceId(group.sourceId);
                  }
                }}
              >
                <span>{group.sourceScopeLabel}</span>
                <strong>{group.sourceFileName}</strong>
                <small>{group.ownerLabel ?? "上传人待补"}</small>
              </button>
              <div className="product-config-source-path-preview__anchors" aria-label={`${group.sourceFileName} Source Digest 路径筛选`}>
                {group.pathAnchors.map(renderSourceDigestSearchAnchor)}
              </div>
              <small>{group.coverage.modelCount} 车型 · {group.coverage.trimCount} 可比配置列 · {group.coverage.differenceCount} 差异</small>
            </article>
          ))}
        </div>
        )}
      </div>
    );
  }

  function renderSourceDigestCandidateSearch(): ReactElement {
    const sourceSearchLabel = simpleModeActive ? "搜索来源 / 车型 / 配置列" : "搜索 Source Digest 可比组";
    const sourcePickerLabel = simpleModeActive ? "选择来源 / 车型 / 配置列" : "选择 Source / Model / 配置列";
    const sourcePickerOptionLabel = simpleModeActive ? "来源 / 车型 / 配置列" : "Source / Model / 配置列";
    const sourceSearchHint = simpleModeActive
      ? "一个来源文件里可能有多个车型 / 配置列；多来源入库后，先搜索来源或车型，再选择 2-4 个配置列生成在线可编辑表格。"
      : "一个 source 里可能有多个 model / 配置列；多 source 入库后，先用这里定位可转配置组，再创建在线可编辑配置列。";
    const sourcePanelPickerHint = sourcePanelDigestOptions.length > 0
      ? `当前可选 ${sourcePanelDigestOptions.length} 个 ${sourcePickerOptionLabel}选项；选单个配置列会先暂存，选整组会直接${sourcePanelDigestOptions.some((option) => option.badge === "生成") ? "生成" : "预览"}。`
      : sourceDigestLibrarySearchActive || librarySourceDigestLoading
        ? simpleModeActive
          ? "当前搜索还没有可选配置列；可调整车型 / 来源 / 市场 / 物料号，或上传来源文件。"
          : "当前搜索还没有可选配置列；可调整 Model / Source / Market / 物料号，或上传来源文件。"
        : simpleModeActive
          ? "先输入车型 / 来源 / 上传人 / 物料号，来源库命中后可在这里直接选择车型或配置列。"
          : "先输入 Model / Source / 上传人 / 物料号，来源库命中后可在这里直接选择 model 或配置列。";
    return (
      <div className="deck-panel-grid__wide product-config-source-direct-search">
        {featureCatalogMappingSummary ? (
          <div className="comparison-drawer-view-status" aria-label="Source Digest 字段映射状态">
            <strong>字段映射已更新</strong>
            <small>
              更新 {featureCatalogMappingSummary.updatedFeatureCount} 项 · 新增 {featureCatalogMappingSummary.createdFeatureCount} 项；重新从来源生成配置列后，新别名才会进入跨来源匹配。
            </small>
            <div className="product-config-export-control__actions">
              <button className="btn btn-sm btn-secondary" type="button" onClick={() => setActivePanel("display")}>
                查看字段映射审计
              </button>
            </div>
          </div>
        ) : null}
        <SearchDropdownFilter
          allowCustomValue
          closeMenuOnCustomInput
          label={sourceSearchLabel}
          loading={librarySourceDigestLoading}
          value={sourceDigestSearchQuery}
          options={sourceDigestSearchOptions}
          placeholder={simpleModeActive ? "搜索来源文件 / 车型 / 市场 / 年款 / 上传人 / 物料号..." : "搜索 Model / Market / MY / Source / 上传人 / 物料号..."}
          emptyLabel={simpleModeActive ? "继续输入关键词搜索来源库" : "继续输入关键词搜索来源库 digest"}
          onChange={updateSourceDigestSearchQuery}
          onQueryChange={updateSourceDigestSearchQuery}
        />
        <small className="market-scan-field-hint">
          {sourceSearchHint}
        </small>
        <small className="market-scan-field-hint">{sourceDigestLibraryScopeHintText}</small>
        {renderSourceDigestActiveScopeStrip()}
        {!sourceDigestDetailBrowserOpen ? renderSourceSnapshotMatchHints() : null}
        <div className="product-config-source-direct-picker" aria-label={simpleModeActive ? "来源下拉选择" : "Source Digest 下拉选择"}>
          <SearchDropdownFilter
            label={sourcePickerLabel}
            loading={librarySourceDigestLoading}
            value={sourceDigestDirectPickerValue}
            selectedValues={directSourceDigestSelectedOptionValues}
            options={sourcePanelDigestOptions}
            initialVisibleCount={28}
            visibleCountStep={28}
            placeholder={simpleModeActive ? "从命中的来源、车型或配置列中选择..." : "从命中的 source、model 或配置列中选择..."}
            emptyLabel={simpleModeActive ? "暂无可选来源 / 车型 / 配置列；先搜索或上传配置表" : "暂无可选 Source / Model / 配置列；先搜索或上传来源"}
            onChange={(value) => { void selectSourcePanelDigestOption(value); }}
            onQueryClear={() => updateSourceDigestSearchQuery("")}
            onQueryChange={handleSourceDigestDirectQueryChange}
          />
          <small className="market-scan-field-hint">{sourcePanelPickerHint}</small>
          {selectedSourcePanelDigestCandidate ? (
            <div className="market-scan-toolbar-meta" aria-label="当前来源整组确认">
              <small>
                已定位 {sourceDigestCandidateScopedLabel(selectedSourcePanelDigestCandidate)}；可以直接
                {sourceDigestPendingActionText(selectedSourcePanelDigestCandidate)}，或继续从下拉暂选单个配置列。
              </small>
              <button
                className="btn btn-sm btn-primary"
                type="button"
                disabled={selectedSourcePanelDigestBusy}
                aria-busy={selectedSourcePanelDigestBusy}
                onClick={() => {
                  if (selectedSourcePanelDigestBusy) return;
                  void confirmSelectedSourcePanelDigestCandidate();
                }}
              >
                {selectedSourcePanelDigestCandidate.sourceKind === "library"
                  ? selectedSourcePanelDigestBusy ? "正在生成当前整组" : "生成当前整组配置列"
                  : "预览当前整组配置列"}
              </button>
            </div>
          ) : null}
          {renderSourceDigestPathPreview()}
        </div>
      </div>
    );
  }

  function renderSourceDigestBrowserPanel(): ReactElement {
    if (!simpleModeActive) return renderSourceDigestCandidatePicker();
    return (
      <details
        className="product-config-source-detail-browser deck-panel-grid__wide"
        aria-label="来源组详情浏览"
        open={sourceDigestDetailBrowserOpen}
        onToggle={(event) => setSourceDigestDetailBrowserOpen(event.currentTarget.open)}
      >
        <summary>
          <span>来源组详情浏览</span>
          <small>
            {sourceDigestDetailSummaryGroupCount} 组 · {sourceDigestDetailBrowserOpen ? "已展开 OCR、sheet、合并来源细节" : "展开后加载 OCR、sheet、合并来源细节"}
          </small>
        </summary>
        {sourceDigestDetailBrowserOpen ? renderSourceDigestCandidatePicker() : null}
      </details>
    );
  }

  function renderDirectSourceDigestPendingPanel(): ReactElement | null {
    if (directSourceDigestPendingItems.length === 0) return null;
    const pendingSourceLabels = uniquePresent(directSourceDigestPendingItems.map((item) => item.candidate.sourceFileName));
    return (
      <div className="product-config-direct-pending" aria-label="待生成来源配置列">
        <span>待生成来源配置列</span>
        {directSourceDigestPendingItems.length > 1 ? (
          <div className="product-config-direct-pending__bulk-actions">
            <em className="product-config-direct-pending__action-hint">
              已按来源拆成 {directSourceDigestPendingItems.length} 组暂选{pendingSourceLabels.length > 0 ? `：${compactList(pendingSourceLabels)}` : ""}；每组至少 2 个同来源配置列才能生成，生成后再跨国家 / 车型 / 网站来源对比。
            </em>
            <button className="btn btn-sm btn-secondary" type="button" onClick={clearAllDirectSourceDigestPending}>
              清空全部暂选
            </button>
          </div>
        ) : null}
        {directSourceDigestPendingItems.map((item) => {
          const pendingScopeLabel = sourceDigestDropdownContextLabel(item.candidate);
          const pendingCandidateLabel = sourceDigestCandidateScopedLabel(item.candidate);
          const pendingSourceTypeLabel = sourceDigestSourceTypeLabel(item.candidate);
          const pendingPathItems = sourceDigestLibraryPathItems(item.candidate, { includeSourceSheetInSourceValue: true });
          const pendingTemporaryOcrIdentityMeta = sourceDigestTemporaryOcrIdentityMeta(item.candidate);
          const pendingIdentityReady = sourceDigestTemporaryIdentityReady(item.candidate, item.selectedTrimIds, sourceDigestTrimIdentityDrafts);
          const pendingActionHint = item.selectedTrimIds.length < 2
            ? `还需再选择 ${2 - item.selectedTrimIds.length} 个同来源配置列，才能生成可编辑配置列。`
            : pendingIdentityReady
              ? "已满足生成条件，点击生成后会进入正式配置列库并加入当前对比。"
              : "请先补齐临时 OCR 列的真实车型 / 配置列身份。";
          return (
            <div className="product-config-direct-pending__group" key={item.key}>
              <div className="product-config-direct-pending__header">
                <strong>{item.candidate.group.modelName}</strong>
                <small>{item.selectedTrimIds.length}/4 · {pendingSourceTypeLabel} · {pendingScopeLabel}</small>
                <em>当前暂选按同一个来源 / 车型生成；生成后进入正式配置列库，可在线编辑、导出，并继续和其他国家 / 车型 / 网站来源一起对比。</em>
                {pendingTemporaryOcrIdentityMeta ? (
                  <em className="product-config-source-digest-card__identity-warning">
                    {pendingTemporaryOcrIdentityMeta}
                  </em>
                ) : null}
              </div>
              <div className="product-config-direct-pending__path" aria-label={`${item.candidate.group.modelName} 待生成来源路径`}>
                {pendingPathItems.map((pathItem) => (
                  <span key={pathItem.key}>
                    <small>{pathItem.label}</small>
                    <strong>{pathItem.value}</strong>
                  </span>
                ))}
              </div>
              <div className="product-config-direct-pending__chips">
                {item.selectedTrims.map((trim) => {
                  const trimLabel = sourceDigestTrimLabel(trim);
                  const trimMeta = trim.materialNo
                    || trim.profile?.materialNo
                    || trim.salesVersion
                    || trim.profile?.configurationVersion
                    || sourceDigestDirectTrimAnchorMeta(trim)
                    || item.candidate.group.sourceSheet;
                  return (
                    <button
                      type="button"
                      key={sourceDigestTrimId(trim)}
                      aria-label={`移除待处理配置列 ${trimLabel}`}
                      onClick={() => removeDirectSourceDigestPendingTrim(item.key, sourceDigestTrimId(trim))}
                    >
                      <strong>{trimLabel}</strong>
                      <small>{trimMeta}</small>
                    </button>
                  );
                })}
              </div>
              {renderSourceDigestReviewRowSelector(item.candidate)}
              {renderSourceDigestTemporaryIdentityEditor(item.candidate, item.selectedTrimIds)}
              <em className="product-config-direct-pending__action-hint">{pendingActionHint}</em>
              <div className="product-config-direct-pending__actions">
                <button
                  className="btn btn-sm btn-primary"
                  type="button"
                  disabled={item.selectedTrimIds.length < 2 || !pendingIdentityReady || sourceDigestDraftActionKey === item.key}
                  onClick={() => { void createDirectSourceDigestPendingDraft(item.key); }}
                >
                  {item.candidate.sourceKind === "library" ? "生成" : "预览"} {pendingCandidateLabel} {item.candidate.sourceKind === "library" ? "可编辑配置列" : "配置列"}
                </button>
                <button className="btn btn-sm btn-secondary" type="button" onClick={() => clearDirectSourceDigestPending(item.key)}>
                  清空暂选
                </button>
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  function renderSourceSnapshotMatchHints(): ReactElement | null {
    if (!sourceDigestLibrarySearchActive || librarySourceSnapshotMatches.length === 0) return null;
    const comparableSourceCount = librarySourceSnapshotMatches.filter(sourceSnapshotHasComparableDigest).length;
    return (
      <div className="product-config-source-snapshot-hints" aria-label="来源库轻量命中">
        <div className="product-config-source-snapshot-hints__head">
          <strong>来源命中</strong>
          <small>
            {librarySourceDigestLoading
              ? "正在展开可转配置列..."
              : `已匹配 ${librarySourceSnapshotMatches.length} 个来源，${comparableSourceCount} 个可转配置列来源`}
          </small>
        </div>
        <div className="product-config-source-snapshot-hints__items">
          {librarySourceSnapshotMatches.map((snapshot, index) => {
            const matchPreview = sourceSnapshotMatchPreview(snapshot);
            const readinessLabel = sourceSnapshotDigestReadinessLabel(snapshot);
            const issuePreview = sourceSnapshotDigestIssuePreview(snapshot);
            const focused = snapshot.sourceId === focusedSourceDigestSourceId;
            return (
              <article className="product-config-source-snapshot-hint-row" key={`source-hit-${snapshot.sourceId}-${index}`}>
                <button
                  className={`product-config-source-snapshot-hint ${focused ? "is-active" : ""}`}
                  type="button"
                  aria-label={`${focused ? "已聚焦来源" : "聚焦来源"} ${snapshot.sourceFileName}`}
                  aria-pressed={focused}
                  onClick={() => {
                    setPendingSourceDigestFocusSourceId(null);
                    setFocusedSourceDigestSourceId(snapshot.sourceId);
                  }}
                >
                  <strong>{snapshot.sourceFileName}</strong>
                  <small>{readinessLabel}</small>
                  <small>{sourceSnapshotDigestStatusPreview(snapshot)}</small>
                  <small>{sourceSnapshotContextPreview(snapshot)}</small>
                  <small>{sourceSnapshotOwnerPreview(snapshot)}</small>
                  {matchPreview ? <small>{matchPreview}</small> : null}
                  {issuePreview ? <small>{issuePreview}</small> : null}
                </button>
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  aria-label={`移入来源垃圾桶 ${snapshot.sourceFileName}`}
                  disabled={!userCanEditValues || !sourceTrashCountry || sourceTrashActionId !== null}
                  onClick={() => void moveSourceSnapshotToTrash(snapshot)}
                >
                  {sourceTrashActionId === snapshot.sourceId ? "移入中" : "移入垃圾桶"}
                </button>
              </article>
            );
          })}
        </div>
      </div>
    );
  }

  function renderSourceSnapshotTrashPanel(): ReactElement {
    const sourceTrashLoadLabel = sourceTrashCountry ? `查看 ${sourceTrashCountry} 来源垃圾桶` : "查看来源垃圾桶";
    const sourceTrashClearBaseLabel = sourceTrashCountry
      ? `清空 ${sourceTrashCountry} 来源垃圾桶（${sourceTrashItems.length} 项）`
      : "清空来源垃圾桶";
    const sourceTrashClearLabel = sourceTrashClearArmed && sourceTrashCountry
      ? `确认清空 ${sourceTrashCountry} 来源垃圾桶`
      : sourceTrashClearBaseLabel;
    return (
      <div className="market-scan-field product-config-trim-trash product-config-source-trash" aria-label="来源库垃圾桶">
        <span>来源库垃圾桶</span>
        <small className="market-scan-field-hint">
          {sourceTrashCountry
            ? `当前国家 ${sourceTrashCountry}；只移动 / 恢复这个国家的来源关联，清空当前国家垃圾桶不影响其他国家仍在用的同一来源文件。`
            : "先选择单一 Market，避免跨国家误清空共享来源。"}
        </small>
        <div className="product-config-drawer-scope__actions" aria-label="来源库垃圾桶操作">
          <button
            className="btn btn-sm btn-secondary"
            type="button"
            disabled={!userCanEditValues || !sourceTrashCountry || sourceTrashLoading}
            onClick={() => void loadSourceSnapshotTrash()}
          >
            {sourceTrashLoading ? "加载中..." : sourceTrashLoadLabel}
          </button>
          <button
            className={`btn btn-sm btn-secondary ${sourceTrashClearArmed ? "product-config-trash-clear-confirm" : ""}`.trim()}
            type="button"
            aria-pressed={sourceTrashClearArmed}
            disabled={!userCanEditValues || !sourceTrashCountry || sourceTrashItems.length === 0 || sourceTrashActionId !== null}
            onClick={requestClearSourceSnapshotTrash}
          >
            {sourceTrashActionId === "__clear_source_trash__" ? "清空中..." : sourceTrashClearLabel}
          </button>
        </div>
        {sourceTrashFeedback ? <small className="market-scan-field-hint">{sourceTrashFeedback}</small> : null}
        {sourceTrashItems.length > 0 ? (
          <div className="product-config-library-groups">
            {sourceTrashItems.map((snapshot, index) => (
              <div className="product-config-library-trim" key={`source-trash-${snapshot.sourceId}-${index}`}>
                <span>{snapshot.sourceFileName}</span>
                <strong>{sourceSnapshotContextPreview(snapshot)}</strong>
                <small>{sourceSnapshotDigestStatusPreview(snapshot)} · {sourceSnapshotOwnerPreview(snapshot)}</small>
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  aria-label={`恢复来源 ${snapshot.sourceFileName}`}
                  disabled={sourceTrashActionId !== null}
                  onClick={() => void restoreSourceSnapshotFromTrash(snapshot)}
                >
                  {sourceTrashActionId === snapshot.sourceId ? "恢复中" : "恢复来源"}
                </button>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    );
  }

  function renderSourceDigestQualityFilters(): ReactElement | null {
    if (sourceDigestQualityFilterItemsList.length === 0 || scopedSourceDigestCandidates.length === 0) return null;
    return (
      <div className="product-config-source-digest-quality-filter" aria-label="Source Digest 来源类型筛选">
        {sourceDigestQualityFilterItemsList.map((item) => {
          const active = sourceDigestQualityFilter === item.key;
          return (
            <button
              className={`comparison-filter-chip ${active ? "is-active" : ""}`}
              type="button"
              key={item.key}
              aria-label={`筛选 Source Digest：${item.label} ${item.count} 个`}
              aria-pressed={active}
              disabled={item.count === 0 && !active}
              title={item.description}
              onClick={() => setSourceDigestQualityFilter(item.key)}
            >
              <span>{item.label}</span>
              <strong>{item.count}</strong>
            </button>
          );
        })}
      </div>
    );
  }

  function renderSourceDigestCandidatePicker(): ReactElement {
    return (
      <div className="market-scan-field deck-panel-grid__wide">
        <span>Source Digest 可比组</span>
        <small className="market-scan-field-hint">{sourceDigestLibraryResultHint}</small>
        {sourceDigestCandidates.length > 0 || sourceDigestLibrarySearchActive ? (
          <div className="product-config-source-digest-coverage" aria-label="Source Digest 检索覆盖">
            <div className="product-config-source-digest-coverage__metrics">
              <span>
                <small>来源</small>
                <strong>{coverageCountLabel(sourceDigestVisibleCoverage.sourceCount, sourceDigestTotalCoverage.sourceCount)}</strong>
              </span>
              <span>
                <small>Model</small>
                <strong>{coverageCountLabel(sourceDigestVisibleCoverage.modelCount, sourceDigestTotalCoverage.modelCount)}</strong>
              </span>
              <span>
              <small>可比配置列</small>
                <strong>{coverageCountLabel(sourceDigestVisibleCoverage.trimCount, sourceDigestTotalCoverage.trimCount)}</strong>
              </span>
              <span>
                <small>配置行</small>
                <strong>{coverageCountLabel(sourceDigestVisibleCoverage.rowCount, sourceDigestTotalCoverage.rowCount)}</strong>
              </span>
              <span>
                <small>差异</small>
                <strong>{coverageCountLabel(sourceDigestVisibleCoverage.differenceCount, sourceDigestTotalCoverage.differenceCount)}</strong>
              </span>
            </div>
            <small>
              {focusedSourceDigestSourceId
                ? `已按来源 ${focusedSourceDigestSourceLabel ?? focusedSourceDigestSourceId} 收窄；解除后回到未锁定来源的搜索范围。`
                : sourceDigestQualityFilter === "all"
                  ? "覆盖当前搜索与筛选后的来源 / 车型 / 配置列；可继续输入来源、车型、配置列、上传人或物料号缩小范围。"
                  : "已叠加来源类型筛选；可切回全部或继续输入来源、车型、配置列、上传人或物料号缩小范围。"}
            </small>
          </div>
        ) : null}
        {renderSourceDigestQualityFilters()}
        {renderSourceSnapshotMatchHints()}
        {focusedSourceDigestSourceId ? (
          <div className="product-config-source-focus" aria-label="当前 Source Digest 来源聚焦">
            <span>
              当前只看来源 <strong>{focusedSourceDigestSourceLabel ?? focusedSourceDigestSourceId}</strong>
            </span>
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              onClick={clearFocusedSourceDigestSource}
            >
              解除来源聚焦
            </button>
          </div>
        ) : null}
        {sourceDigestBrowseCondensed ? (
          <div className="product-config-source-digest-browse-gate" aria-label="Source Digest 默认浏览预览">
            <div>
              <span>默认先预览 {visibleSourceDigestBrowseCandidates.length}/{filteredSourceDigestCandidates.length} 个可比组</span>
              <small>多来源入库后建议直接搜索车型 / 来源 / 上传人；需要浏览全量时再展开。</small>
            </div>
            <button className="btn btn-sm btn-secondary" type="button" onClick={() => setSourceDigestBrowseExpanded(true)}>
              展开全部 {filteredSourceDigestCandidates.length} 个
            </button>
          </div>
        ) : sourceDigestBrowseCanCollapse ? (
          <div className="product-config-source-digest-browse-gate" aria-label="Source Digest 浏览范围">
            <div>
              <span>已展示全部匹配组</span>
              <small>当前筛选命中 {filteredSourceDigestCandidates.length} 个可比组。</small>
            </div>
            <button className="btn btn-sm btn-secondary" type="button" onClick={() => setSourceDigestBrowseExpanded(false)}>
              收起为预览
            </button>
          </div>
        ) : null}
        {filteredSourceDigestCandidates.length > 0 ? (
          <div className="product-config-source-digest-browser" aria-label="Source Digest 按来源和品牌浏览">
            {sourceDigestBrowseGroups.map((browseGroup) => (
              <details className="product-config-source-digest-browser-group" key={browseGroup.key} open aria-label={`${browseGroup.sourceFileName} / ${browseGroup.modelLabel} Source Digest 分组`}>
                <summary className="product-config-source-digest-browser-group__summary">
                  <div>
                    <span>{browseGroup.sourceScopeLabel}</span>
                    <strong>{browseGroup.sourceFileName}</strong>
                    <small>{browseGroup.ownerLabel ?? "上传人待补"}</small>
                  </div>
                  <div className="product-config-source-digest-browser-group__path">
                    {browseGroup.pathAnchors.slice(1).map(renderSourceDigestSearchAnchor)}
                  </div>
                  <div className="product-config-source-digest-browser-group__metrics">
                    <span>{browseGroup.coverage.modelCount} 车型</span>
                    <span>{browseGroup.coverage.trimCount} 可比配置列</span>
                    <span>{browseGroup.coverage.differenceCount} 差异</span>
                  </div>
                </summary>
                <div className="comparison-same-model-groups">
                  {browseGroup.candidates.map((candidate) => {
                    const candidateKey = sourceDigestCandidateKey(candidate);
                    const creating = sourceDigestDraftActionKey === candidateKey;
                    const selected = candidate.sourceKind === "local" && candidateKey === activeDigestGroupId;
                    const selectedTrimIds = selectedSourceDigestTrimIds(candidate, sourceDigestTrimSelections);
                    const matchMeta = sourceDigestGroupMatchMeta(candidate);
                    const qualityMeta = sourceDigestQualityMeta(candidate);
                    const ocrQualityText = sourceDigestOcrQualityText(candidate);
                    const ocrScoreMetrics = sourceDigestOcrScoreMetrics(candidate);
                    const ocrReasonDetails = sourceDigestOcrReasonDetails(candidate);
                    const ocrComparisonText = sourceDigestOcrComparisonText(candidate);
                    const ocrSelectedPreview = sourceDigestSelectedOcrPreview(candidate);
                    const identityMeta = sourceDigestIdentityMeta(candidate);
                    const temporaryOcrIdentityMeta = sourceDigestTemporaryOcrIdentityMeta(candidate);
                    const reviewMeta = sourceDigestReviewMeta(candidate);
                    const libraryPathItems = sourceDigestLibraryPathItems(candidate, { includeSourceSheetInSourceValue: true });
                    const librarySearchAnchorItems = sourceDigestLibraryPathItems(candidate);
                    const sourceTypeLabel = sourceDigestSourceTypeLabel(candidate);
                    const cardIdentityReady = sourceDigestTemporaryIdentityReady(candidate, selectedTrimIds, sourceDigestTrimIdentityDrafts);
                    return (
                      <article
                        className={`comparison-same-model-group product-config-source-digest-card ${selected ? "is-selected" : ""}`}
                        key={candidateKey}
                      >
                        <button
                          className="product-config-source-digest-card__main"
                          type="button"
                          aria-label={`选择 Source Digest 可比组：${candidate.group.modelName}，${sourceDigestGroupMeta(candidate)}`}
                          disabled={creating || selectedTrimIds.length < 2 || (candidate.sourceKind === "library" && !cardIdentityReady)}
                          onClick={() => {
                            void selectSourceDigestCandidate(candidate);
                          }}
                        >
                          <div className="product-config-source-digest-card__head">
                            <strong>{candidate.group.modelName}</strong>
                            <span>{sourceDigestSourceScopeLabel(candidate)}</span>
                            <span>{sourceTypeLabel}</span>
                          </div>
                          <div className="product-config-source-digest-card__path" aria-label={`${candidate.group.modelName} 来源库路径`}>
                            {libraryPathItems.map((item) => (
                              <span key={item.key}>
                                <small>{item.label}</small>
                                <strong>{item.value}</strong>
                              </span>
                            ))}
                          </div>
                          <div className="product-config-source-digest-card__metrics" aria-label={`${candidate.group.modelName} digest 指标`}>
                            <span>
                              <small>配置列</small>
                              <strong>{candidate.group.trimCount}</strong>
                            </span>
                            <span>
                              <small>差异</small>
                              <strong>{candidate.group.differenceCount}</strong>
                            </span>
                            <span>
                              <small>配置行</small>
                              <strong>{candidate.group.rows.length}</strong>
                            </span>
                          </div>
                          <span className="product-config-source-digest-card__source">{sourceDigestSourceLine(candidate)}</span>
                          <small className="comparison-same-model-group-match product-config-source-digest-card__identity">{identityMeta}</small>
                          {temporaryOcrIdentityMeta ? (
                            <small className="product-config-source-digest-card__identity-warning">
                              {temporaryOcrIdentityMeta}
                            </small>
                          ) : null}
                          {reviewMeta ? (
                            <small className="product-config-source-digest-card__identity-warning">
                              {reviewMeta}
                            </small>
                          ) : null}
                          {featureCatalogMappingSummary ? (
                            <small className="product-config-source-digest-card__identity-warning">
                              字段映射待应用 · 建列时按 FeatureCatalog 别名归并（更新 {featureCatalogMappingSummary.updatedFeatureCount} / 新增 {featureCatalogMappingSummary.createdFeatureCount}）
                            </small>
                          ) : null}
                          <small>
                            {candidate.sourceKind === "library"
                              ? creating ? "正在创建可编辑配置列..." : "来源库 · 点击创建可编辑配置列"
                              : "本地样例 · 点击预览"}
                            {" · 已选 "}
                            {selectedTrimIds.length}
                            /4 · {sourceDigestTrimPreview(candidate, selectedTrimIds)}
                          </small>
                          {ocrQualityText ? (
                            <div className="product-config-source-digest-card__quality" aria-label={`${candidate.group.modelName} OCR 来源质量`}>
                              <span>{ocrQualityText}</span>
                              {ocrScoreMetrics.length > 0 ? (
                                <div>
                                  {ocrScoreMetrics.map((item) => (
                                    <small key={item.label}>
                                      {item.label}
                                      <strong>{item.value}</strong>
                                    </small>
                                  ))}
                                </div>
                              ) : null}
                              {ocrReasonDetails.length > 0 ? (
                                <ul className="product-config-source-digest-card__ocr-reasons" aria-label={`${candidate.group.modelName} OCR 选择依据`}>
                                  {ocrReasonDetails.map((detail) => (
                                    <li key={detail}>{detail}</li>
                                  ))}
                                </ul>
                              ) : null}
                              {ocrComparisonText ? <small className="product-config-source-digest-card__ocr-preview">{ocrComparisonText}</small> : null}
                              {ocrSelectedPreview ? <small className="product-config-source-digest-card__ocr-preview">{ocrSelectedPreview}</small> : null}
                            </div>
                          ) : null}
                          {qualityMeta ? <small className="comparison-same-model-group-match">{qualityMeta}</small> : null}
                          {matchMeta ? <small className="comparison-same-model-group-match">{matchMeta}</small> : null}
                        </button>
                        {renderSourceDigestReviewRowSelector(candidate)}
                        <div className="product-config-source-digest-card__quick-search" aria-label={`${candidate.group.modelName} Source Digest 路径快速搜索`}>
                          {librarySearchAnchorItems.map(renderSourceDigestSearchAnchor)}
                        </div>
                        <div className="product-config-source-digest-card__trim-picker" aria-label={`${candidate.group.modelName} 可创建配置列选择`}>
                          <span>选择 2-4 个配置列</span>
                          <div>
                            {candidate.group.trims.map((trim) => {
                              const trimId = sourceDigestTrimId(trim);
                              const checked = selectedTrimIds.includes(trimId);
                              const disabled = creating || (checked ? selectedTrimIds.length <= 2 : selectedTrimIds.length >= 4);
                              return (
                                <label className={`product-config-source-digest-card__trim ${checked ? "is-selected" : ""}`} key={trimId}>
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    disabled={disabled}
                                    onChange={() => toggleSourceDigestCandidateTrim(candidate, trimId)}
                                  />
                                  <span>{sourceDigestTrimLabel(trim)}</span>
                                  <small>{trim.materialNo || trim.salesVersion || trim.profile?.configurationVersion || trim.profile?.materialNo || sourceDigestDirectTrimAnchorMeta(trim) || "来源锚点待补"}</small>
                                </label>
                              );
                            })}
                          </div>
                        </div>
                        {renderSourceDigestTemporaryIdentityEditor(candidate, selectedTrimIds)}
                      </article>
                    );
                  })}
                </div>
              </details>
            ))}
          </div>
        ) : (
          <small className="market-scan-field-hint">
            {librarySourceDigestLoading
              ? "正在搜索来源库可转配置组..."
              : sourceDigestLibrarySearchActive
              ? "当前筛选没有可展示的来源车型；可清空车型 / 来源 / 关键词后重新搜索。"
                : "输入车型 / 来源 / 上传人 / 物料号 / sales version 后搜索来源库。"}
          </small>
        )}
        {sourceDigestDraftFeedback ? <small className="market-scan-field-hint">{sourceDigestDraftFeedback}</small> : null}
        {renderSourceSnapshotTrashPanel()}
      </div>
    );
  }

  function directSearchDiagnosticSummaryLabel(): string {
    return directConfigSearchSummaryItems
      .map((item) => `${item.label} ${item.value}`)
      .join(" · ");
  }

  function renderDirectSearchDiagnostics(): ReactElement {
    return (
      <>
        <div className="product-config-direct-summary" aria-label="直接搜索配置列结果拆解">
          {directConfigSearchSummaryItems.map((item) => (
            <span className={`product-config-direct-summary__item is-${item.tone}`} key={item.key}>
              <small>{item.label}</small>
              <strong>{item.value}</strong>
              <em>{item.description}</em>
            </span>
          ))}
        </div>
        {directSourceDigestCoverageItems.length > 0 ? (
          <div className="product-config-direct-coverage" aria-label="直接搜索 Source Digest 覆盖">
            {directSourceDigestCoverageItems.map((item) => (
              <span className="product-config-direct-coverage__item" key={item.key}>
                <small>{item.label}</small>
                <strong>{item.sourceCount} 来源 · {item.modelCount} 车型</strong>
                <em>{item.groupCount} 组 / {item.optionCount} 选项 · {item.status}</em>
              </span>
            ))}
          </div>
        ) : null}
      </>
    );
  }

  function renderDirectSearchQuickStatus(): ReactElement {
    const formalItem = directConfigSearchSummaryItems.find((item) => item.key === "formal");
    const sourceDigestItem = directConfigSearchSummaryItems.find((item) => item.key === "source-digest");
    const pendingItem = directConfigSearchSummaryItems.find((item) => item.key === "pending");
    const quickItems = [
      formalItem ? { ...formalItem, label: "正式库" } : null,
      sourceDigestItem ? { ...sourceDigestItem, label: "来源库" } : null,
      pendingItem ? { ...pendingItem, label: "待生成" } : null,
      {
        key: "ambiguity",
        label: "同名范围",
        value: visibleDirectModelAmbiguities.length > 0 ? `${visibleDirectModelAmbiguities.length} 组` : "0",
        description: visibleDirectModelAmbiguities.length > 0 ? "需按来源核对" : "暂无当前搜索冲突",
        tone: visibleDirectModelAmbiguities.length > 0 ? "pending" as const : "muted" as const,
      },
    ].filter((item): item is DirectConfigSearchSummaryItem => Boolean(item));
    return (
      <div className="product-config-direct-quick-status" aria-label="统一搜索覆盖状态">
        <span className="product-config-direct-quick-status__lead">
          <strong>下拉统一搜索</strong>
          <small>正式库和来源文件一起查；库按来源 → 车型 → 配置列组织，直接搜来源文件、车型、物料号、sales version 或上传人。</small>
        </span>
        <span className="product-config-direct-quick-status__items">
          {quickItems.map((item) => (
            <span className={`product-config-direct-quick-status__item is-${item.tone}`} key={item.key}>
              <small>{item.label}</small>
              <strong>{item.value}</strong>
              <em>{item.description}</em>
            </span>
          ))}
        </span>
      </div>
    );
  }

  function renderSelectedConfigPathGroups(): ReactElement | null {
    if (selectedConfigPathGroups.length === 0) return null;
    return (
      <div className="product-config-direct-selected-paths" aria-label="已选配置列来源路径">
        <span>对比路径分组</span>
        <div className="product-config-direct-selected-paths__items">
          {selectedConfigPathGroups.map((group) => (
            <article className="product-config-direct-selected-path" key={group.key}>
              <div className="product-config-direct-selected-path__source">
                <small>Source</small>
                <strong>{group.sourceLabel}</strong>
                <em>{[group.ownerLabel, group.sourceCreatedAtLabel].filter((value): value is string => Boolean(value)).join(" · ") || "上传信息待补"}</em>
              </div>
              <div className="product-config-direct-selected-path__scope" aria-label={`${group.sourceLabel} 配置路径`}>
                <span>{group.brandLabel}</span>
                <span>{group.marketLabel}</span>
                <span>{group.modelYearLabel}</span>
                <span>{group.modelLabel}</span>
              </div>
              <div className="product-config-direct-selected-path__trims" aria-label={`${group.sourceLabel} 已选配置列`}>
                <strong>{group.trimCount} 配置列</strong>
                <small>{compactList(group.trimLabels)}</small>
              </div>
              <small className="product-config-direct-selected-path__identity">
                {group.originLabel} · {group.anchorLabel}
              </small>
            </article>
          ))}
        </div>
      </div>
    );
  }

  function directAdvancedSearchSummaryLabel(): string {
    const activeFilterCount = [
      filters.model,
      filters.brand,
      filters.market,
      filters.modelYear,
      filters.trim,
      filters.powertrain,
      filters.segment,
      filters.source,
      filters.keyword,
    ].filter((value) => Boolean(value)).length;
    const filterLabel = activeFilterCount > 0 ? `${activeFilterCount} 个筛选已启用` : "未启用筛选";
    return `${filterLabel} · 库内 ${trimLibraryTotalRows} 列 · Source Digest ${librarySourceDigestCandidates.length} 组`;
  }

  function renderAdvancedConfigSearchControls(): ReactElement {
    return (
      <>
        <SearchDropdownFilter label="Model / 车型" value={filters.model} options={modelOptions} placeholder="先选择车型..." emptyLabel="无匹配车型" onChange={(value) => setFilters((current) => ({ ...current, model: value }))} onQueryChange={updateSourceDigestSearchQuery} />
        <SearchDropdownFilter label="Brand" value={filters.brand} options={brandOptions} placeholder="选择品牌..." emptyLabel="无匹配品牌" onChange={(value) => setFilters((current) => ({ ...current, brand: value }))} />
        <SearchDropdownFilter label="Market" value={filters.market} options={marketOptions} placeholder="选择市场..." emptyLabel="无匹配市场" onChange={(value) => setFilters((current) => ({ ...current, market: value }))} onQueryChange={updateSourceDigestSearchQuery} />
        <SearchDropdownFilter label="Model Year" value={filters.modelYear} options={modelYearOptions} placeholder="选择年款..." emptyLabel="无匹配年款" onChange={(value) => setFilters((current) => ({ ...current, modelYear: value }))} onQueryChange={updateSourceDigestSearchQuery} />
        <SearchDropdownFilter
          label="配置列 / Configuration"
          value={filters.trim}
          options={trimOptions}
          placeholder="选择配置列..."
          emptyLabel="无匹配配置列"
          onChange={(value) => setFilters((current) => ({ ...current, trim: value }))}
          onQueryChange={updateSourceDigestSearchQuery}
        />
        <SearchDropdownFilter label="Powertrain" value={filters.powertrain} options={powertrainOptions} placeholder="选择动力..." emptyLabel="无匹配动力" onChange={(value) => setFilters((current) => ({ ...current, powertrain: value }))} />
        <SearchDropdownFilter label="Segment" value={filters.segment} options={segmentOptions} placeholder="选择级别..." emptyLabel="暂无高级分析 segment；先选市场 / 车型 / 动力后加载推荐" onChange={(value) => setFilters((current) => ({ ...current, segment: value }))} />
        <SearchDropdownFilter label="Source / File" value={filters.source} options={sourceOptions} placeholder="选择来源..." emptyLabel="无匹配来源" onChange={updateSourceFilter} onQueryChange={updateSourceFilterQuery} />
        <div className="deck-panel-grid__wide">
          <SearchDropdownFilter
            label="BOM / Keyword"
            value={filters.keyword}
            options={keywordOptions}
            placeholder="选择物料号、配置项或 sales version..."
            emptyLabel="无匹配关键字"
            onChange={(value) => setFilters((current) => ({ ...current, keyword: value }))}
            onQueryChange={updateSourceDigestSearchQuery}
          />
        </div>
        <div className="market-scan-field deck-panel-grid__wide">
          <span>库内浏览</span>
          <small className="market-scan-field-hint">{trimLibraryResultHint}</small>
          {libraryBrandTrimGroups.length > 0 ? (
            <div className="product-config-library-browser" aria-label="配置列库按品牌浏览">
              {libraryBrandTrimGroups.map((brandGroup) => (
                <details className="product-config-library-brand" key={brandGroup.key} open>
                  <summary>
                    <span className="product-config-library-brand__identity">
                      <strong>{brandGroup.brandLabel}</strong>
                      <small>{brandGroup.marketLabel} · {brandGroup.modelYearLabel}</small>
                    </span>
                    <span className="product-config-library-brand__metrics">
                      <em>{brandGroup.modelCount} 车型</em>
                      <em>{brandGroup.trimCount} 配置列</em>
                      <em>{brandGroup.sourceCount > 0 ? `${brandGroup.sourceCount} 来源` : "来源待补"}</em>
                    </span>
                  </summary>
                  <div className="product-config-library-groups">
                    {brandGroup.groups.map((group) => (
                      <details className="product-config-library-group" key={group.key}>
                        <summary>
                          <span>{group.label}</span>
                          <small>{group.meta}</small>
                        </summary>
                        <div className="product-config-library-group__items">
                          {group.items.map((trim) => {
                            const selected = compareIds.includes(trim.trimId);
                            const disabled = !selected && compareIds.length >= 4;
                            const anchor = trim.materialNo || trim.vehicleCode || trim.salesVersion || trim.identityKey || trim.fullTrimName;
                            return (
                              <button
                                className={`product-config-library-trim ${selected ? "is-selected" : ""}`}
                                type="button"
                                key={trim.trimId}
                                disabled={disabled}
                                onClick={() => toggleCompareId(trim.trimId)}
                              >
                                <span>{anchor}</span>
                                <strong>{trim.trimName || trim.fullTrimName}</strong>
                                <small>{trimOriginLabel(trim)} · {trimMaterialAnchorLabel(trim)} · {trimSourceLabel(trim)}</small>
                                <em>{selected ? "移除" : disabled ? "最多 4 个" : "加入"}</em>
                              </button>
                            );
                          })}
                        </div>
                      </details>
                    ))}
                  </div>
                </details>
              ))}
            </div>
          ) : null}
        </div>
        <div className="market-scan-field deck-panel-grid__wide">
          <span>同车型配置列组</span>
          {sameModelGroups.length > 0 ? (
            <div className="comparison-same-model-groups">
              {sameModelGroups.map((group) => (
                <button className="comparison-same-model-group" type="button" key={group.key} onClick={() => selectGroup(group)}>
                  <strong>{group.label}</strong>
                  <span>{group.meta}</span>
                  <small>{group.items.slice(0, 4).map((trim) => trim.trimName || trim.fullTrimName).join(" / ")}</small>
                </button>
              ))}
            </div>
          ) : (
            <small className="market-scan-field-hint">当前筛选结果里还没有 2 个以上同车型配置列。</small>
          )}
        </div>
        <div className="market-scan-field deck-panel-grid__wide">
          <span>高级分析推荐竞品</span>
          <small className="market-scan-field-hint">
            按当前国家、车型和动力从高级分析推荐；库内有配置列可直接加入，有 Source Digest 则先生成配置列，完全缺失再上传来源。
          </small>
          {competitorRecommendations.length > 0 ? (
            <>
              <div className="comparison-competitor-summary" aria-label="推荐竞品配置覆盖">
                <span>
                  <small>推荐范围</small>
                  <strong>Top {competitorRecommendations.length}/{COMPETITOR_RECOMMENDATION_LIMIT}</strong>
                </span>
                <span>
                  <small>库内可用</small>
                  <strong>{competitorRecommendationsReadyCount}</strong>
                </span>
                <span className={competitorRecommendationsMissingCount > 0 ? "is-warning" : "is-ready"}>
                  <small>待生成</small>
                  <strong>{competitorRecommendationsDigestReadyCount}</strong>
                </span>
                <span className={competitorRecommendationsMissingCount > 0 ? "is-warning" : "is-ready"}>
                  <small>待上传</small>
                  <strong>{competitorRecommendationsMissingCount}</strong>
                </span>
                <span>
                  <small>推荐口径</small>
                  <strong>{competitorRecommendationScopeText}</strong>
                </span>
                <span>
                  <small>高级分析</small>
                  <strong>{competitorRecommendationSourceLabel(competitorRecommendationSource)}</strong>
                </span>
              </div>
              {renderCompetitorRecommendationQueue()}
            </>
          ) : null}
          {competitorRecommendationsLoading ? (
            <small className="market-scan-field-hint">正在加载推荐竞品...</small>
          ) : competitorRecommendations.length > 0 ? (
            <div className="comparison-competitor-grid">
              {competitorRecommendations.map((recommendation) => renderCompetitorRecommendationCard(recommendation))}
            </div>
          ) : (
            <small className="market-scan-field-hint">{competitorRecommendationNote || "当前口径暂无推荐竞品。"}</small>
          )}
        </div>
        {renderSourceDigestCandidatePicker()}
        <div className="market-scan-field deck-panel-grid__wide">
          <span>候选配置列 / 物料号</span>
          <div className="comparison-trim-list comparison-trim-list--drawer">
            {trims.map((trim) => {
              const selected = compareIds.includes(trim.trimId);
              const disabled = !selected && compareIds.length >= 4;
              const anchor = trim.materialNo || trim.vehicleCode || trim.salesVersion || trim.identityKey || trim.fullTrimName;
              return (
                <button className={`comparison-trim-option ${selected ? "is-selected" : ""}`} type="button" key={trim.trimId} disabled={disabled} onClick={() => toggleCompareId(trim.trimId)}>
                  <span className="comparison-trim-option-title">{anchor}</span>
                  <span className="comparison-trim-option-meta">{trim.modelName || "Model 待补"} · {trim.trimName || trim.fullTrimName}</span>
                  <span className="comparison-trim-option-meta">{trimMeta(trim)}</span>
                  <span className="comparison-trim-option-meta">
                    {[trimOriginLabel(trim), trimMaterialAnchorLabel(trim), trimSourceSnapshotLabel(trim), trimSourceCreatedAtLabel(trim)]
                      .filter((value): value is string => Boolean(value))
                      .join(" · ")}
                  </span>
                  <span className="comparison-trim-option-action">{selected ? "移除" : disabled ? "最多 4 个" : "加入"}</span>
                </button>
              );
            })}
            {trims.length === 0 ? (
              <span className="version-comparison-empty">
                {trimsLoading
                  ? "加载候选配置列..."
                  : "没有匹配配置列"}
              </span>
            ) : null}
          </div>
        </div>
        <button className="btn btn-secondary deck-panel-grid__wide" type="button" onClick={() => void loadTrims()} disabled={trimsLoading}>
          {trimsLoading ? "刷新中..." : "刷新候选"}
        </button>
      </>
    );
  }

  const renderFilters = () => (
    <div className="deck-panel-grid">
      {!simpleModeActive ? (
        <div className="market-scan-field deck-panel-grid__wide product-config-model-first-hint">
          <span>选择顺序</span>
          <strong>先选 Model，再多选物料号配置列</strong>
          <small>本品优先用物料号作为颗粒度；缺物料号的竞品 / 抓取配置列用 sales version、市场和来源快照辅助锚定。</small>
        </div>
      ) : null}
      <div className="deck-panel-grid__wide product-config-direct-picker">
        <div className="product-config-direct-picker__search-row">
          <SearchDropdownFilter
            key={`direct-config-column-picker-${directPickerResetKey}`}
            label={DIRECT_CONFIG_COLUMN_PICKER_LABEL}
            loading={directTrimSearchLoading || librarySourceDigestLoading}
            value={directTrimPickerValue}
            selectedValues={directConfigSelectedOptionValues}
            options={directConfigColumnOptions}
            initialVisibleCount={24}
            visibleCountStep={24}
            placeholder="搜索品牌 / 车型 / 市场 / 物料号 / sales version / 来源..."
            emptyLabel="当前库内没有匹配配置列；可先调整筛选或上传来源文件"
            onChange={(value) => { void selectDirectConfigColumn(value); }}
            onQueryChange={handleDirectConfigColumnQueryChange}
          />
          {directSearchClearActive ? (
            <button
              className="btn btn-sm btn-secondary product-config-direct-picker__clear"
              type="button"
              aria-label="清除直接配置列搜索"
              onClick={clearDirectConfigColumnSearch}
            >
              清除搜索
            </button>
          ) : null}
        </div>
        <small className="market-scan-field-hint product-config-direct-picker__hint">{directConfigSearchResultHint}</small>
        {simpleModeActive ? renderDirectSearchQuickStatus() : null}
        {directSearchCanOpenSourceUpload ? (
          <div className="product-config-direct-miss" aria-label="直接搜索未命中来源入口">
            <div>
              <span>库内暂未命中</span>
              <strong>{directTrimSearchKeyword}</strong>
              <small>去来源库继续搜索；仍未命中时可上传 xlsx / PDF / 图片 / CSV / HTML / 价格单并转成在线配置列。</small>
            </div>
            <button className="btn btn-sm btn-secondary" type="button" onClick={openSourcePanelForDirectSearch}>
              搜索 / 上传这个资料
            </button>
          </div>
        ) : null}
        {sourceContextBindingPromptOpen ? (
          <div className={`product-config-source-binding-prompt ${filters.market.trim() ? "is-bound" : ""}`} aria-label="来源上传国家绑定提示">
            <div>
              <span>来源归档上下文</span>
              <strong>{filters.market.trim() ? `已选择 ${filters.market.trim()}` : "先选择 Market / Country"}</strong>
              <small>
                {filters.market.trim()
                  ? "回到上传后，新来源会按当前国家上下文登记，后续垃圾桶和复用也按这个国家筛选。"
                  : "上传来源可以先入团队共享库；建议先在下方高级筛选选择 Market，避免后续多国家来源混在一起。"}
              </small>
            </div>
            <div className="product-config-source-binding-prompt__actions">
              <button className="btn btn-sm btn-primary" type="button" onClick={returnToSourceUploadAfterContextBinding}>
                回到上传 / 关联来源
              </button>
              <button className="btn btn-sm btn-secondary" type="button" onClick={() => setSourceContextBindingPromptOpen(false)}>
                知道了
              </button>
            </div>
          </div>
        ) : null}
        {simpleModeActive ? (
          <details className="product-config-model-first-hint product-config-model-first-hint--compact" aria-label="配置列选择提示">
            <summary>
              <span>选择提示</span>
              <strong>车型优先，配置列可多选</strong>
            </summary>
            <small>本品优先用物料号作为颗粒度；缺物料号的竞品 / 抓取配置列用 sales version、市场和来源快照辅助锚定。</small>
          </details>
        ) : null}
        {simpleModeActive ? (
          <details
            className="product-config-direct-diagnostics"
            aria-label="直接搜索配置列诊断"
            open={simpleDirectDiagnosticsOpen}
          >
            <summary
              onClick={(event) => {
                event.preventDefault();
                setSimpleDirectDiagnosticsOpen((currentOpen) => !currentOpen);
              }}
            >
              <span>搜索诊断</span>
              <small>{directSearchDiagnosticSummaryLabel()}</small>
            </summary>
            {simpleDirectDiagnosticsOpen ? renderDirectSearchDiagnostics() : null}
          </details>
        ) : renderDirectSearchDiagnostics()}
        {visibleDirectModelAmbiguities.length > 0 ? (
          <div className="product-config-direct-ambiguity" aria-label="同名车型多来源提示">
            <strong>同名车型多来源</strong>
            <span>
              {visibleDirectModelAmbiguities.slice(0, 2).map((item) => (
                `${item.label}：${item.candidateCount} ${item.itemUnitLabel} / ${item.sourceCount} 来源${item.sheetCount > 0 ? ` / ${item.sheetCount} 表格页` : ""}${item.ownerCount > 1 ? ` / ${item.ownerCount} 上传人` : ""}`
              )).join("；")}
              {visibleDirectModelAmbiguities.length > 2 ? `；+${visibleDirectModelAmbiguities.length - 2} 个同名范围` : ""}
            </span>
            <small>同国家同年款但来源、表格页或上传人不同，添加前按来源核对；正式库跨来源车型不会一键加入。</small>
            <div className="product-config-direct-ambiguity__actions">
              {visibleDirectModelAmbiguities.slice(0, 2).map((item) => (
                <button
                  className="btn btn-sm btn-secondary"
                  key={item.key}
                  type="button"
                  aria-label={`按此范围核对配置来源：${item.label}`}
                  onClick={() => openDirectModelAmbiguitySearch(item)}
                >
                  核对 {item.label}
                </button>
              ))}
              <button
                className="btn btn-sm btn-secondary"
                type="button"
                onClick={openAllSourceDigestAmbiguitySearch}
              >
                查看全部同名范围
              </button>
            </div>
          </div>
        ) : null}
        <div className="product-config-direct-selected" aria-label="当前已选配置列">
          <span>已选配置列 {directSelectedDisplayTrims.length}/4</span>
          {directSelectedDisplayTrims.length > 0 ? (
            <div className="product-config-direct-selected__items">
              {directSelectedDisplayTrims.map((trim) => {
                const trimLabel = compareTrimLabel(trim);
                const trimMeta = directSelectedTrimMeta(trim);
                if (directSelectedReadOnly) {
                  return (
                    <span className="product-config-direct-selected__item is-readonly" key={trim.trimId} aria-label={`当前预览 ${trimLabel}`}>
                      <strong>{trimLabel}</strong>
                      <small>{trimMeta} · 来源预览</small>
                    </span>
                  );
                }
                return (
                  <button
                    type="button"
                    key={trim.trimId}
                    aria-label={`移除 ${trimLabel}`}
                    onClick={() => toggleCompareId(trim.trimId)}
                  >
                    <strong>{trimLabel}</strong>
                    <small>{trimMeta}</small>
                  </button>
                );
              })}
            </div>
          ) : null}
          {renderSelectedConfigPathGroups()}
          {renderDirectSourceDigestPendingPanel()}
          {directSelectedDisplayTrims.length === 0 && directSourceDigestPendingItems.length === 0 ? digestModeActive ? (
            <small>当前展示的是本地 xlsx 样例；从上方搜索库内配置列后会替换为正式对比。</small>
          ) : (
            <small>从上方搜索下拉选择 2-4 个配置列，选中后会直接加入对比。</small>
          ) : null}
        </div>
      </div>
      {simpleModeActive ? (
        <section
          className={`product-config-deck-advanced-search deck-panel-grid__wide ${simpleAdvancedSearchOpen ? "is-open" : ""}`}
          aria-label="高级筛选与库内浏览"
        >
          <button
            className="product-config-deck-advanced-search__summary"
            type="button"
            aria-expanded={simpleAdvancedSearchOpen}
            aria-controls="product-config-deck-advanced-search-body"
            onClick={() => setSimpleAdvancedSearchOpen((open) => !open)}
          >
            <span>高级筛选 / 库内浏览</span>
            <small>{directAdvancedSearchSummaryLabel()}</small>
          </button>
          {simpleAdvancedSearchOpen ? (
            <div id="product-config-deck-advanced-search-body" className="deck-panel-grid product-config-deck-advanced-search__body">
              {renderAdvancedConfigSearchControls()}
            </div>
          ) : null}
        </section>
      ) : renderAdvancedConfigSearchControls()}
    </div>
  );

  function renderSelectedDisplayCard(trim: ComparableTrim, includeSourceAction: boolean, actionsEnabled = true): ReactElement {
    const isBaseTrim = trim.trimId === baseTrimId;
    const isTargetTrim = trim.trimId === activeTargetTrimId;
    return (
      <SelectedTrimCard
        key={trim.trimId}
        trim={trim}
        isBaseTrim={isBaseTrim}
        isTargetTrim={isTargetTrim}
        actionsEnabled={actionsEnabled}
        removeLabel={digestModeActive ? "关闭样例" : "移除"}
        onFocusTarget={() => focusTargetDifference(isTargetTrim ? null : trim.trimId)}
        onMoveToTrash={!digestModeActive && userCanEditValues ? () => {
          void moveTrimToLibraryTrash(trim);
        } : undefined}
        onOpenSource={includeSourceAction ? () => setActivePanel("source") : undefined}
        onRemove={() => digestModeActive ? closeDigestSample() : toggleCompareId(trim.trimId)}
        onSetBase={() => setComparisonBaseTrim(trim.trimId)}
        trashDisabled={trimTrashActionId !== null}
        trashLoading={trimTrashActionId === trim.trimId}
      />
    );
  }

  const renderSelectedPanel = () => (
    <div className="product-config-selected-panel">
      {sourceDigestDraftFeedback ? <small className="market-scan-field-hint">{sourceDigestDraftFeedback}</small> : null}
      {selectedDisplayTrims.length > 0
        ? selectedDisplayTrims.map((trim) => renderSelectedDisplayCard(trim, true))
        : <span className="version-comparison-empty">请选择至少 2 个配置列开始配置对比。</span>}
      {renderTrimTrashPanel()}
    </div>
  );

  function renderSourceDigestDraftReplacementControls(draftSuccess: SourceDigestDraftSuccessSummary): ReactElement | null {
    if (draftSuccess.omittedCompareTrims.length === 0 || selectedDisplayTrims.length === 0) return null;
    return (
      <div className="product-config-draft-success__replacements" aria-label="暂未显示配置列替换入口">
        <div>
          <strong>暂未显示的新配置列</strong>
          <small>当前对比最多 4 列；可以直接用新列替换一个已选配置列。</small>
        </div>
        {draftSuccess.omittedCompareTrims.map((omittedTrim) => (
          <div className="product-config-draft-success__replacement-row" key={omittedTrim.trimId}>
            <span>{omittedTrim.label}</span>
            <div className="product-config-draft-success__replacement-actions">
              {selectedDisplayTrims.map((trim) => (
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  key={`${omittedTrim.trimId}-${trim.trimId}`}
                  aria-label={`用 ${omittedTrim.label} 替换 ${compareTrimLabel(trim)} 进入当前对比`}
                  onClick={() => replaceOmittedSourceDigestTrimInCompare(omittedTrim, trim)}
                >
                  替换 {compareTrimLabel(trim)}
                </button>
              ))}
            </div>
          </div>
        ))}
      </div>
    );
  }

  function renderSourceDigestDraftSuccess(): ReactElement | null {
    const draftSuccess = sourceDigestDraftFeedback && sourceDigestDraftSuccess?.feedback === sourceDigestDraftFeedback
      ? sourceDigestDraftSuccess
      : null;
    if (!sourceDigestDraftFeedback || !draftSuccess) return null;
    return (
      <section className="product-config-draft-success deck-panel-grid__wide" aria-label="来源建列成功" role="status">
        <div>
          <span>已转成正式配置列</span>
          <strong>{draftSuccess.currentCompare.headline}</strong>
          <small>{sourceDigestDraftFeedback}</small>
          <small className="product-config-draft-success__next-step" aria-label="当前对比已加入配置列">
            {draftSuccess.currentCompare.label}。{draftSuccess.currentCompare.meta}。
          </small>
          {draftSuccess.ocrTransparency ? (
            <small className="product-config-draft-success__next-step" aria-label="来源建列 OCR 透明度">
              {draftSuccess.ocrTransparency.meta}。
              {draftSuccess.ocrTransparency.comparison ? `${draftSuccess.ocrTransparency.comparison} ` : ""}
              {draftSuccess.ocrTransparency.reviewNote}
            </small>
          ) : null}
          {draftSuccess.featureCatalogMatch ? (
            <small className="product-config-draft-success__next-step" aria-label="建列字段归并摘要">
              {draftSuccess.featureCatalogMatch.meta}
              {draftSuccess.featureCatalogMatch.samples.length > 0
                ? ` 样例：${draftSuccess.featureCatalogMatch.samples.join("；")}。`
                : ""}
            </small>
          ) : null}
          <div className="product-config-draft-success__path" aria-label="建列来源路径">
            {draftSuccess.pathStages.map((stage) => (
              <span className="product-config-draft-success__stage" key={stage.key}>
                <small>{stage.label}</small>
                <strong>{stage.value}</strong>
                <em>{stage.meta}</em>
              </span>
            ))}
          </div>
          <div className="product-config-draft-success__metrics" aria-label="建列结果摘要">
            {draftSuccess.metrics.map((metric) => (
              <span key={metric.key}>
                <small>{metric.label}</small>
                <strong>{metric.value}</strong>
              </span>
            ))}
          </div>
          {renderSourceDigestDraftReplacementControls(draftSuccess)}
          <small className="product-config-draft-success__next-step">
            下一步都在 FloatingDeck 内完成：如需编辑，请在下方“在线编辑”控制里开启；导出跟当前显示范围一致。
          </small>
          <small className="product-config-draft-success__next-step" aria-label="建列后工作区">
            已切到“显示 / 编辑”工作区，可继续核对 AI 摘要、在线编辑和导出当前表格。
          </small>
          <small className="product-config-draft-success__next-step" aria-label="建列后 AI 摘要边界">
            AI 业务摘要会按当前对比表运行时生成；可随 XLSX / PDF 导出，但不写回来源解析记录。
          </small>
        </div>
        <div className="product-config-draft-success__actions">
          <button
            className="btn btn-sm btn-secondary"
            type="button"
            disabled={!deckExportAvailable || deckExportingFormat !== null}
            onClick={() => void exportCurrentTableFromDeck("xlsx")}
          >
            {deckExportingFormat === "xlsx" ? "导出 XLSX 中..." : "导出建列结果 XLSX"}
          </button>
          <button
            className="btn btn-sm btn-secondary"
            type="button"
            disabled={!deckExportAvailable || deckExportingFormat !== null}
            onClick={() => void exportCurrentTableFromDeck("pdf")}
          >
            {deckExportingFormat === "pdf" ? "导出 PDF 中..." : "导出建列结果 PDF"}
          </button>
          <button className="btn btn-sm btn-secondary" type="button" onClick={() => setActivePanel("selected")}>
            查看已选配置列
          </button>
          {sourceDigestDraftReviewFocus ? (
            <button className="btn btn-sm btn-secondary" type="button" onClick={focusSourceDigestDraftReviewRow}>
              跳到需核对行
            </button>
          ) : null}
          <button className="btn btn-sm btn-secondary" type="button" onClick={() => setActivePanel("source")}>
            继续添加来源
          </button>
        </div>
      </section>
    );
  }

  function renderTrimTrashPanel(): ReactElement {
    const trimTrashLoadLabel = trimTrashCountry ? `查看 ${trimTrashCountry} 配置列垃圾桶` : "查看配置列垃圾桶";
    const trimTrashClearBaseLabel = trimTrashCountry
      ? `清空 ${trimTrashCountry} 配置列垃圾桶（${trimTrashItems.length} 项）`
      : "清空配置列垃圾桶";
    const trimTrashClearLabel = trimTrashClearArmed && trimTrashCountry
      ? `确认清空 ${trimTrashCountry} 配置列垃圾桶`
      : trimTrashClearBaseLabel;
    return (
      <div className="market-scan-field product-config-trim-trash" aria-label="配置列库垃圾桶">
        <span>配置列库垃圾桶</span>
        <small className="market-scan-field-hint">
          {trimTrashCountry
            ? `当前国家 ${trimTrashCountry}；只处理这个国家的配置列，恢复会回到 Draft，清空只清空当前国家垃圾桶。`
            : "先选择单一 Market，避免跨国家误清空配置列。"}
        </small>
        <div className="product-config-drawer-scope__actions" aria-label="配置列库垃圾桶操作">
          <button
            className="btn btn-sm btn-secondary"
            type="button"
            disabled={!userCanEditValues || !trimTrashCountry || trimTrashLoading}
            onClick={() => void loadTrimLibraryTrash()}
          >
            {trimTrashLoading ? "加载中..." : trimTrashLoadLabel}
          </button>
          <button
            className={`btn btn-sm btn-secondary ${trimTrashClearArmed ? "product-config-trash-clear-confirm" : ""}`.trim()}
            type="button"
            aria-pressed={trimTrashClearArmed}
            disabled={!userCanEditValues || !trimTrashCountry || trimTrashItems.length === 0 || trimTrashActionId !== null}
            onClick={requestClearTrimLibraryTrash}
          >
            {trimTrashActionId === "__clear_trim_trash__" ? "清空中..." : trimTrashClearLabel}
          </button>
        </div>
        {trimTrashFeedback ? <small className="market-scan-field-hint">{trimTrashFeedback}</small> : null}
        {trimTrashItems.length > 0 ? (
          <div className="product-config-library-groups">
            {trimTrashItems.map((trim) => (
              <div className="product-config-library-trim" key={trim.trimId}>
                <span>{trim.materialNo || trim.vehicleCode || trim.salesVersion || trim.identityKey || trim.trimId}</span>
                <strong>{trim.trimName || trim.fullTrimName}</strong>
                <small>{trim.brand || "Brand"} · {trim.modelName || "Model"} · {trim.market || trim.country || "市场待补"} · {trimSourceLabel(trim)}</small>
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  aria-label={`恢复 ${compareTrimLabel(trim)} 为 Draft`}
                  disabled={trimTrashActionId !== null}
                  onClick={() => void restoreTrimFromLibraryTrash(trim)}
                >
                  {trimTrashActionId === trim.trimId ? "恢复中" : "恢复为 Draft"}
                </button>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    );
  }

  function renderDisplayScopeButton(option: DisplayScopeOption): ReactElement {
    const count = countRowsForDisplayScope(option.key);
    const active = activeDeltaFilter === option.key;
    const buttonText = displayScopeButtonText(option, count, simpleModeActive && summaryMode === "simple");
    const description = displayScopeDescription(option, simpleModeActive && summaryMode === "simple");
    return (
      <button
        className={`comparison-filter-chip comparison-drawer-scope-option ${active ? "is-active" : ""}`}
        type="button"
        key={option.key}
        aria-label={`显示范围：${buttonText}`}
        aria-pressed={active}
        onClick={() => selectDisplayScope(option.key)}
      >
        <span>{buttonText}</span>
        <small>{description}</small>
      </button>
    );
  }

  function updateTrimIdentityDraftField(field: TrimIdentityFieldKey, value: string): void {
    setTrimIdentityDraft((current) => ({ ...current, [field]: value }));
  }

  function renderTrimIdentityInput(field: TrimIdentityFieldKey, label: string, placeholder: string): ReactElement {
    return (
      <label className="product-config-trim-identity-editor__field" key={field}>
        <span>{label}</span>
        <input
          value={trimIdentityDraft[field]}
          placeholder={placeholder}
          onChange={(event) => updateTrimIdentityDraftField(field, event.target.value)}
        />
      </label>
    );
  }

  function renderFeatureCatalogMappingControl(): ReactElement {
    const canImportMapping = userCanEditValues && !featureCatalogMappingUploading && Boolean(featureCatalogMappingFile);
    const mappingSummaryText = featureCatalogMappingSummary
      ? `更新 ${featureCatalogMappingSummary.updatedFeatureCount} · 新增 ${featureCatalogMappingSummary.createdFeatureCount}`
      : "导入配置字段映射表";
    return (
      <details
        className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode product-config-feature-mapping-control"
        aria-label="字段映射表导入"
      >
        <summary>
          <span>字段映射 / 别名</span>
          <strong>{mappingSummaryText}</strong>
          <small>维护审核后的跨来源字段别名，不生成配置列。</small>
        </summary>
        <div className="comparison-drawer-view-status">
          <small>
            上传配置字段映射表后会更新 FeatureCatalog 别名；适合人工确认过的跨来源同义字段。
          </small>
          <small className="market-scan-field-hint">
            只导入已审核映射：字段中文名 / 英文名 / alias 需要业务或工程 owner 确认；导入只影响后续 Source Digest 建列和跨来源匹配，不会直接修改当前配置值。
          </small>
          <label className="product-config-feature-mapping-control__picker">
            <span>选择字段映射表</span>
            <input
              type="file"
              accept={FEATURE_CATALOG_MAPPING_ACCEPT}
              disabled={!userCanEditValues || featureCatalogMappingUploading}
              onChange={handleFeatureCatalogMappingFileChange}
            />
          </label>
          <div className="product-config-export-control__actions">
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              disabled={!canImportMapping}
              onClick={() => void uploadFeatureCatalogMapping()}
            >
              {featureCatalogMappingUploading ? "导入中..." : "导入字段映射表"}
            </button>
            {featureCatalogMappingFile ? <small className="market-scan-field-hint">{featureCatalogMappingFile.name}</small> : null}
          </div>
          {featureCatalogMappingSummary ? (
            <>
              <div className="product-config-drawer-scope__chips" aria-label="字段映射导入结果">
                <span className="product-config-drawer-scope__chip">
                  <small>总字段</small>
                  <strong>{featureCatalogMappingSummary.totalFeatures}</strong>
                </span>
                <span className="product-config-drawer-scope__chip">
                  <small>已更新</small>
                  <strong>{featureCatalogMappingSummary.updatedFeatureCount}</strong>
                </span>
                <span className="product-config-drawer-scope__chip">
                  <small>新增</small>
                  <strong>{featureCatalogMappingSummary.createdFeatureCount}</strong>
                </span>
                <span className="product-config-drawer-scope__chip">
                  <small>未变更</small>
                  <strong>{featureCatalogMappingSummary.unchangedFeatureCount}</strong>
                </span>
                <span className="product-config-drawer-scope__chip">
                  <small>提示</small>
                  <strong>{featureCatalogMappingSummary.warningCount}</strong>
                </span>
              </div>
              {featureCatalogMappingSummary.categories && featureCatalogMappingSummary.categories.length > 0 ? (
                <small className="market-scan-field-hint">
                  大类：{featureCatalogMappingSummary.categories.slice(0, 6).join(" / ")}
                  {featureCatalogMappingSummary.categories.length > 6 ? `，另 ${featureCatalogMappingSummary.categories.length - 6} 个` : ""}
                </small>
              ) : null}
              <small className="market-scan-field-hint">
                新别名会用于后续 Source Digest 建列；已建列配置如需应用新映射，请重新从来源生成配置列。
              </small>
              <div className="product-config-export-control__actions">
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  onClick={openSourceDigestAfterFeatureMappingImport}
                >
                  去 Source Digest 重新建列
                </button>
              </div>
              {featureCatalogMappingAudit ? (
                <div className="comparison-drawer-view-status" aria-label="字段映射导入审计">
                  <strong>导入审计</strong>
                  <small className="market-scan-field-hint">
                    Upload {featureCatalogMappingAudit.uploadId} · {featureCatalogMappingAudit.fileName} · {featureCatalogMappingAudit.importedBy || "unknown"} / {featureCatalogMappingAudit.importedRole || "role unknown"}
                  </small>
                  <small className="market-scan-field-hint">
                    已写入 {featureCatalogMappingAudit.artifactRef || "upload session meta"}；后续跨来源字段复用有疑问时，可按这份摘要回查本次 alias 导入。
                  </small>
                  <div className="product-config-export-control__actions">
                    <button
                      className="btn btn-sm btn-secondary"
                      type="button"
                      onClick={() => void copyFeatureCatalogMappingAudit()}
                    >
                      复制审计摘要
                    </button>
                  </div>
                </div>
              ) : null}
              {featureCatalogMappingSummary.warnings && featureCatalogMappingSummary.warnings.length > 0 ? (
                <div className="comparison-drawer-view-status" aria-label="字段映射解析提示">
                  <strong>解析提示</strong>
                  {featureCatalogMappingSummary.warnings.slice(0, 3).map((warning) => (
                    <small className="market-scan-field-hint" key={warning}>{warning}</small>
                  ))}
                  {featureCatalogMappingSummary.warnings.length > 3 ? (
                    <small className="market-scan-field-hint">另 {featureCatalogMappingSummary.warnings.length - 3} 条提示可在后端导入结果中查看。</small>
                  ) : null}
                </div>
              ) : null}
            </>
          ) : null}
          {!userCanEditValues ? (
            <small className="market-scan-field-hint">需要 editor / admin / developer 权限才能导入字段映射表。</small>
          ) : null}
          {featureCatalogMappingFeedback ? (
            <small className="market-scan-field-hint" role="status">{featureCatalogMappingFeedback}</small>
          ) : null}
        </div>
      </details>
    );
  }

  function renderAiSummaryReadinessControl(): ReactElement {
    if (!simpleModeActive) {
      return (
        <Suspense fallback={<LoadingSurface mode="inline" label="加载 AI 摘要状态" />}>
          <LazyEngineeringConfigAiSummaryReadinessCard
            readiness={businessSummaryReadiness}
            error={businessSummaryReadinessError}
            variant="drawer"
          />
        </Suspense>
      );
    }
    return (
      <details
        className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode product-config-ai-readiness-disclosure"
        aria-label="AI 摘要运行状态"
        open={drawerAiReadinessOpen}
        onToggle={(event) => setDrawerAiReadinessOpen(event.currentTarget.open)}
      >
        <summary>
          <span>AI 摘要状态</span>
          <strong>{drawerAiReadinessOpen ? "Runtime 状态" : "展开后检查 runtime"}</strong>
          <small>AI 结论按当前配置表实时生成；简易模式默认不加载 provider 诊断。</small>
        </summary>
        {drawerAiReadinessOpen ? (
          <Suspense fallback={<LoadingSurface mode="inline" label="加载 AI 摘要状态" />}>
            <LazyEngineeringConfigAiSummaryReadinessCard
              ariaLabel="AI 摘要运行状态详情"
              readiness={businessSummaryReadiness}
              error={businessSummaryReadinessError}
              variant="drawer"
            />
          </Suspense>
        ) : null}
      </details>
    );
  }

  const renderDisplayPanel = () => {
    const primaryDisplayScopeOptions = summaryMode === "simple"
      ? DISPLAY_SCOPE_OPTIONS.filter((option) => SIMPLE_DISPLAY_SCOPE_KEYS.has(option.key))
      : DISPLAY_SCOPE_OPTIONS;
    const editModeButtonLabel = editModeAvailable
      ? editModeEnabled ? "关闭在线编辑" : "开启在线编辑"
      : userCanEditValues ? "预览不可编辑" : "权限只读";
    const versionFallbackCount = compareData?.versionFallbackCount ?? 0;
    const versionStatusLabel = compareVersionScope === "latest"
      ? "最新工作版本"
      : versionFallbackCount > 0
        ? `已发布口径 · ${versionFallbackCount} 列回退草稿`
        : "已发布版本";
    return (
    <div className="deck-panel-grid">
      {renderSourceDigestDraftSuccess()}
      <div className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode">
        <span>视图模式</span>
        <div className="comparison-drawer-view-status" aria-label="显示控制中的当前视图模式">
          <strong>{summaryMode === "simple" ? "简易模式" : "专家模式"}</strong>
          <small>
            {summaryMode === "simple"
              ? "AI 结论优先，只保留完整配置行、差异行、目标列和大类导航；高级诊断在专家模式。"
              : "查看规则推断、来源问题、合并格、待确认和共同配置等诊断口径。"}
          </small>
          {renderSummaryModeOptions("显示控制中切换配置对比视图模式")}
        </div>
      </div>
      {renderAiSummaryReadinessControl()}
      {compareData ? (
        <div className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode" aria-label="配置数据版本控制">
          <span>数据版本</span>
          <div className="comparison-drawer-view-status">
            <strong>{versionStatusLabel}</strong>
            <small>
              {compareVersionScope === "latest"
                ? "显示每个配置列的最新工作版本；只有最新草稿可进入在线编辑。"
                : versionFallbackCount > 0
                  ? "部分配置列尚无已发布版本，当前仅为可识别的草稿回退，不代表已审批发布。"
                  : "默认使用已发布快照，避免未审批草稿进入业务结论和导出。"}
            </small>
            <div className="product-config-version-scope-options" role="group" aria-label="选择配置数据版本">
              <button
                type="button"
                className={compareVersionScope === "published" ? "is-active" : ""}
                aria-pressed={compareVersionScope === "published"}
                onClick={() => {
                  setEditModeEnabled(false);
                  setCompareVersionScope("published");
                }}
              >
                已发布
              </button>
              <button
                type="button"
                className={compareVersionScope === "latest" ? "is-active" : ""}
                aria-pressed={compareVersionScope === "latest"}
                onClick={() => {
                  setEditModeEnabled(false);
                  setCompareVersionScope("latest");
                }}
              >
                最新工作版
              </button>
            </div>
          </div>
        </div>
      ) : null}
      <div className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode" aria-label="在线编辑控制">
        <span>在线编辑</span>
        <div className="comparison-drawer-view-status">
          <strong>
            {editModeAvailable
              ? editModeEnabled ? "编辑已开启" : "编辑未开启"
              : userCanEditValues
                ? compareData ? "当前数据版本只读" : "来源预览只读"
                : "当前权限只读"}
          </strong>
          <small>
            {editModeAvailable
              ? editModeEnabled
                ? "点击配置值会进入编辑；完成后建议关闭，避免误触。"
                : "默认点击单元格只查看来源证据；需要修改配置值时先开启编辑。"
              : userCanEditValues
                ? compareData
                  ? "切换到最新工作版，且所有配置列都存在最新草稿后才能开启编辑。"
                  : "本地 xlsx 样例是预览数据，创建正式配置列后才能编辑。"
                : "需要 editor / admin / developer 权限才能在线修改配置值。"}
          </small>
          {editModeAvailable ? (
            <label className="product-config-trim-identity-editor__field">
              <span>本次修改说明</span>
              <input
                value={editAuditReason}
                maxLength={500}
                disabled={editModeEnabled}
                placeholder="例如：复核 2026 欧盟配置表"
                onChange={(event) => setEditAuditReason(event.target.value)}
              />
            </label>
          ) : null}
          <button
            className={`btn btn-sm ${editModeEnabled ? "btn-primary" : "btn-secondary"}`}
            type="button"
            aria-pressed={editModeEnabled}
            disabled={!editModeAvailable || (!editModeEnabled && !editAuditReason.trim())}
            onClick={() => setEditModeEnabled((current) => !current)}
          >
            {editModeButtonLabel}
          </button>
        </div>
      </div>
      <div className="market-scan-field deck-panel-grid__wide comparison-drawer-view-mode product-config-export-control" aria-label="配置对比导出控制">
        <span>导出当前表格</span>
        <div className="comparison-drawer-view-status">
          <strong>{displayCompareData && deckExportRowCount > 0 ? `${deckExportRowCount} 行 · ${deckExportTrimCount} 列` : "暂无可导出配置行"}</strong>
          <small>
            {deckExportAvailable
              ? `导出范围跟当前表格一致：${deckExportRangeLabel}。简易模式默认导出全部配置行，切到差异行后只导出差异行。`
              : displayCompareData && deckExportRowCount > 0 && !deckExportReady
                ? "表格导出动作正在准备，稍后即可导出当前范围。"
                : "先选择 2-4 个配置列，或恢复筛选范围后再导出。"}
          </small>
          <div className="product-config-export-control__actions" aria-label="配置对比导出格式">
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              disabled={!deckExportAvailable}
              onClick={() => void copyCurrentTableFromDeck()}
            >
              {deckCopyLabel}
            </button>
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              disabled={!deckExportAvailable || deckExportingFormat !== null}
              onClick={() => void exportCurrentTableFromDeck("xlsx")}
            >
              {deckExportingFormat === "xlsx" ? "导出 XLSX 中..." : "导出当前范围 XLSX"}
            </button>
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              disabled={!deckExportAvailable || deckExportingFormat !== null}
              onClick={() => void exportCurrentTableFromDeck("pdf")}
            >
              {deckExportingFormat === "pdf" ? "导出 PDF 中..." : "导出当前范围 PDF"}
            </button>
          </div>
          {businessSummaryExportItems.length > 0 ? (
            <small className="market-scan-field-hint" aria-label="AI 结论导出口径">
              {businessSummaryExportUsage
                ? `AI 结论会随当前表格一起进入导出文件；${businessSummaryExportUsage.provider} / ${businessSummaryExportUsage.model} 按当前表格运行时生成，不回写来源解析记录。`
                : "AI 结论会随当前表格一起进入导出文件；这是当前表格运行时生成，不回写来源解析记录。"}
            </small>
          ) : null}
          {deckExportFeedback ? <small className="market-scan-field-hint" role="status">{deckExportFeedback}</small> : null}
        </div>
      </div>
      {summaryMode === "expert" || userCanEditValues ? renderFeatureCatalogMappingControl() : null}
      {editModeAvailable && editModeEnabled && trimIdentityEditTrim ? (
        <div className="market-scan-field deck-panel-grid__wide product-config-trim-identity-editor" aria-label="配置列身份编辑">
          <span>配置列身份</span>
          <small className="market-scan-field-hint">
            补齐国家 / 年款 / 物料号后，FloatingDeck 搜索、竞品推荐匹配和导出证据会更可靠。
          </small>
          <SearchDropdownFilter
            label="选择配置列"
            value={trimIdentityEditTrim.trimId}
            options={trimIdentityOptions}
            placeholder="选择要编辑身份的配置列..."
            emptyLabel="暂无可编辑配置列"
            onChange={(value) => setTrimIdentityEditId(value)}
          />
          <div className="product-config-trim-identity-editor__grid">
            {renderTrimIdentityInput("brand", "Brand", "例如 OMODA")}
            {renderTrimIdentityInput("modelName", "Model", "例如 T19C")}
            {renderTrimIdentityInput("trimName", "配置列", "例如 Premium-FWD")}
            {renderTrimIdentityInput("fullTrimName", "完整名称", "例如 T19C Premium-FWD")}
            {renderTrimIdentityInput("market", "Market / Country", "例如 Germany")}
            {renderTrimIdentityInput("modelYear", "Model Year", "例如 2026")}
            {renderTrimIdentityInput("energyType", "Energy", "例如 ICE / BEV")}
            {renderTrimIdentityInput("drivetrain", "Drivetrain", "例如 FWD / AWD")}
            {renderTrimIdentityInput("engine", "Engine", "例如 1.6T")}
            {renderTrimIdentityInput("materialNo", "物料号", "本品可填，竞品可留空")}
            {renderTrimIdentityInput("vehicleCode", "Sales version", "无物料号时用于锚定")}
            {renderTrimIdentityInput("identityKey", "Identity key", "来源或规则生成的身份键")}
          </div>
          <div className="product-config-trim-identity-editor__actions">
            <button
              className="btn btn-sm btn-primary"
              type="button"
              disabled={!trimIdentityDirty || trimIdentitySaving}
              onClick={() => void saveTrimIdentity()}
            >
              {trimIdentitySaving ? "保存中..." : "保存配置列身份"}
            </button>
            <button
              className="btn btn-sm btn-secondary"
              type="button"
              disabled={!trimIdentityDirty || trimIdentitySaving}
              onClick={() => setTrimIdentityDraft(trimIdentityDraftFromTrim(trimIdentityEditTrim))}
            >
              还原
            </button>
            {trimIdentityFeedback ? <small className="market-scan-field-hint" role="status">{trimIdentityFeedback}</small> : null}
          </div>
        </div>
      ) : null}
      {displayCompareData ? (
        <div className={`market-scan-field deck-panel-grid__wide product-config-drawer-scope ${analysisScopeActive ? "is-active" : ""}`.trim()} aria-label="显示控制中的当前分析口径">
          <span>当前分析口径</span>
          <div className="product-config-drawer-scope__chips">
            {analysisScopeItems.map((item) => (
              <span className="product-config-drawer-scope__chip" key={item.key}>
                <small>{item.label}</small>
                <strong>{item.value}</strong>
              </span>
            ))}
          </div>
          <small className="market-scan-field-hint">
            {analysisScopeActive
              ? simpleModeActive
                ? "显示控制、配置大类和配置表正按同一口径联动。"
                : "显示控制、业务摘要和配置表正按同一口径联动。"
              : summaryMode === "simple"
                ? "当前展示完整 xlsx 配置行；AI 摘要优先，高级诊断已收起，可先通读原表再切换差异行。"
                : "当前展示完整配置矩阵，可先巡检全部参数再切换差异。"}
          </small>
          {analysisScopeActive ? (
            <div className="product-config-drawer-scope__actions" aria-label="显示控制口径操作">
              {rowScopeActive ? (
                <button className="btn btn-sm btn-secondary product-config-drawer-scope__reset" type="button" onClick={restoreAllConfigRows}>
                  {restoreAllConfigRowsLabel}
                </button>
              ) : null}
              {activeTargetTrim ? (
                <button className="btn btn-sm btn-secondary product-config-drawer-scope__reset" type="button" onClick={showAllTargetColumns}>
                  显示全部目标列
                </button>
              ) : null}
              {rowScopeActive && activeTargetTrim ? (
                <button className="btn btn-sm btn-secondary product-config-drawer-scope__reset" type="button" onClick={clearAnalysisScope}>
                  清空显示口径
                </button>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
      <div className="market-scan-field deck-panel-grid__wide comparison-drawer-range">
        <span>结果范围</span>
        <div className="comparison-drawer-scope-options" aria-label="显示范围">
          {primaryDisplayScopeOptions.map(renderDisplayScopeButton)}
        </div>
        <small className="market-scan-field-hint">
          {simpleModeActive
            ? "简易模式只保留“全部配置行 / 差异行”；规则推断、来源问题、合并格、待确认和共同配置请切到专家模式。"
            : "目标配置列只聚焦对比对象，不改变结果范围；需要差异视图时切换“差异项”或点击“查看差异项”。"}
        </small>
      </div>
      <div className="deck-panel-grid__wide">
        <SearchDropdownFilter
          label="目标配置列"
          value={activeTargetTrimId ?? ALL_TARGET_TRIMS_VALUE}
          options={targetTrimOptions}
          placeholder="选择目标配置列..."
          emptyLabel="无可聚焦目标配置列"
          onChange={selectTargetTrimScope}
        />
      </div>
      <div className="market-scan-field deck-panel-grid__wide product-config-target-anchor" aria-label="目标配置列身份锚点">
        <span>目标锚点</span>
        <div className="product-config-target-anchor__items">
          {targetAnchorItems.map((item) => (
            <span className={`product-config-target-anchor__item is-${item.tone}`} key={item.key}>
              <small>{item.label}</small>
              <strong>{item.value}</strong>
            </span>
          ))}
        </div>
      </div>
      <div className="market-scan-field">
        <span>{simpleModeActive ? "当前范围差异行" : "当前范围差异项"}</span>
        <strong className="comparison-drawer-metric">{displayCompareData ? scopedSummaryMetrics.confirmedDifferenceCount : "-"}</strong>
      </div>
      <div className="market-scan-field">
        <span>{simpleModeActive ? "当前范围待确认行" : "当前范围待确认"}</span>
        <strong className="comparison-drawer-metric">{displayCompareData ? scopedSummaryMetrics.missingUnknownCount : "-"}</strong>
      </div>
      <button className="btn btn-secondary deck-panel-grid__wide" type="button" onClick={() => void doCompare()} disabled={compareIds.length < 2 || compareLoading}>
        {compareLoading ? "刷新中..." : "刷新配置对比"}
      </button>
    </div>
    );
  };

  return (
    <section className="crud-shell comparison-page product-config-compare-page">
      {error ? <div className="alert alert-error">{error}<button className="btn btn-sm" type="button" onClick={() => setError(null)} style={{ marginLeft: 8 }}>×</button></div> : null}
      <DeckFloatingDrawer
        open={controlOpen}
        onOpenChange={(open) => {
          if (open && valuesCanBeEdited) setActivePanel("display");
          setControlOpen(open);
        }}
        triggerPrimary={controlTriggerPrimaryLabel}
        triggerSecondaryOpen="收起控制"
        triggerSecondaryClosed={controlTriggerSecondaryClosedLabel}
        eyebrow={simpleModeActive ? "配置列选择" : "Model / Config Columns"}
        title="配置对比控制"
        ariaLabel={simpleModeActive ? "配置列对比控制" : "Model 配置列对比控制"}
        className="engineering-config-control-drawer"
        panelClassName="engineering-config-control-panel"
        bodyScrollResetKey={activePanel}
        footer={(
          <>
            <span className="market-scan-toolbar-chip">候选配置列 {trims.length}</span>
            <span className="market-scan-toolbar-chip">已选配置列 {selectedDisplayTrims.length}/4</span>
            <span
              className="market-scan-toolbar-chip"
              aria-label={`FloatingDeck 当前表格范围：${resultScopeChipLabel}`}
            >
              范围 {resultScopeChipLabel}
            </span>
            {activeTargetTrim ? <span className="market-scan-toolbar-chip">目标配置列 {compareTrimLabel(activeTargetTrim)}</span> : null}
            {baseTrim ? <span className="market-scan-toolbar-chip">基准列 {baseTrim.trimName || baseTrim.fullTrimName}</span> : null}
            {editModeAvailable ? (
              <span
                className={`market-scan-toolbar-chip product-config-edit-status-chip ${editModeEnabled ? "is-active" : "is-locked"}`}
                aria-label={editModeEnabled ? "在线编辑状态：编辑已开启" : "在线编辑状态：编辑关闭"}
                title={editModeEnabled ? "在 FloatingDeck 显示 / 编辑面板关闭在线编辑" : "在 FloatingDeck 显示 / 编辑面板开启在线编辑"}
              >
                {editModeEnabled ? "编辑已开启" : "编辑关闭"}
              </span>
            ) : null}
            {trimsLoading || compareLoading ? <span className="market-scan-toolbar-chip">刷新中</span> : null}
            {digestModeActive ? <span className="market-scan-toolbar-chip">本地样例</span> : null}
          </>
        )}
      >
        <DeckControlTabs
          tabs={simpleModeActive ? SIMPLE_CONTROL_TABS : CONTROL_TABS}
          activeKey={activePanel}
          onChange={(panel) => {
            if (panel !== "source") setSourceContextOverride(null);
            if (panel !== "filters") setSourceContextBindingPromptOpen(false);
            if (panel === "display") void auth?.refreshUser();
            setActivePanel(panel);
          }}
          ariaLabel={simpleModeActive ? "配置列对比控制" : "Model 配置列对比控制"}
        />
        {activePanel === "filters" ? renderFilters() : null}
        {activePanel === "selected" ? renderSelectedPanel() : null}
        {activePanel === "source" ? (
          <div className="deck-panel-grid">
            {renderSourceDigestCandidateSearch()}
            {renderDirectSourceDigestPendingPanel()}
            <div className="deck-panel-grid__wide">
              <Suspense fallback={<LoadingSurface mode="inline" label="加载 Source Digest 上传入口" />}>
                <LazyEngineeringConfigSourceUploadPanel
                  compact
                  diagnosticsMode="collapsed"
                  libraryScopeMode="collapsed"
                  relatedContext={effectiveSourceUploadContext}
                  showSourceLibrary={false}
                  sourceDigestTitle="配置表 / 价格单上传（推荐）"
                  sourceDigestDescription="上传 xlsx、PDF、图片、CSV、HTML 或价格单，识别后先转成可编辑配置列。"
                  onRequestContextBinding={openSourceContextBindingPanel}
                  onRequestSourceSearch={searchUploadedSourceInFloatingDeck}
                  onUploaded={refreshConfigLibrariesAfterSourceUpload}
                  onDraftCreated={handleSourceDigestDraftCreated}
                />
              </Suspense>
            </div>
            {renderSourceDigestBrowserPanel()}
          </div>
        ) : null}
        {activePanel === "display" ? renderDisplayPanel() : null}
      </DeckFloatingDrawer>

      <section className={`comparison-hero product-config-hero ${simpleModeActive && displayCompareData ? "is-compact" : ""}`.trim()}>
        <div>
          <span className="market-scan-panel-eyebrow">Product Deck</span>
          <h1>车型配置表对比</h1>
          <p className="text-muted">
            {simpleModeActive
              ? "像看 xlsx 一样选择 2-4 个配置列，指定基准列后核对全部配置行、差异行和来源证据。"
              : "选择同一车型或不同市场下的 2-4 个配置列；专家模式会提供高级诊断、规则推断和来源路径。"}
          </p>
        </div>
        <div className="market-scan-hero-ribbon">
          {renderHeroSummaryModeSwitch()}
          {selectedDisplayTrims.length > 0 ? (
            <button className="market-scan-hero-chip product-config-hero-action" type="button" onClick={() => openControlPanel("filters")}>添加配置列</button>
          ) : null}
          {analysisScopeActive ? (
            <button
              className="market-scan-hero-chip product-config-hero-action"
              type="button"
              aria-label={rowScopeActive ? `顶部${restoreAllConfigRowsLabel}` : "顶部显示全部目标列"}
              onClick={resetHeroScope}
            >
              {heroScopeResetLabel}
            </button>
          ) : null}
          {displayCompareData && !simpleModeActive && activeDeltaFilter !== "DIFFERENCE" ? (
            <button
              className="market-scan-hero-chip product-config-hero-action"
              type="button"
              aria-label={`顶部${heroDifferenceActionLabel}`}
              onClick={showDifferenceScope}
            >
              {heroDifferenceActionLabel}
            </button>
          ) : null}
          <span className="market-scan-hero-chip">已选配置列 {selectedDisplayTrims.length}/4</span>
          {prefilterLabel ? <span className="market-scan-hero-chip">筛选 {prefilterLabel}</span> : null}
          {baseTrim ? <span className="market-scan-hero-chip">基准列 {baseTrim.trimName || baseTrim.fullTrimName}</span> : null}
          {simpleModeActive && displayCompareData ? (
            <span
              className="market-scan-hero-chip"
              aria-label={`顶部当前表格范围：${resultScopeChipLabel}`}
            >
              范围 {resultScopeChipLabel}
            </span>
          ) : null}
          {digestModeActive ? <span className="market-scan-hero-chip">本地 xlsx 样例</span> : null}
        </div>
      </section>

      <section className={`product-config-selected-strip ${simpleModeActive && displayCompareData ? "is-compact is-collapsible" : ""}`.trim()}>
        {selectedDisplayTrims.length > 0 ? (
          simpleModeActive && displayCompareData ? (
            <details
              className="product-config-selected-strip-details"
              aria-label="已选配置列抽屉"
              open={simpleSelectedStripOpen}
            >
              <summary
                className="product-config-selected-strip-summary"
                onClick={(event) => {
                  event.preventDefault();
                  setSimpleSelectedStripOpen((current) => !current);
                }}
              >
                <span>已选配置列</span>
                <strong>{selectedDisplayTrims.length}/4</strong>
                <small>{baseTrim ? `基准列 ${baseTrim.trimName || baseTrim.fullTrimName}` : "基准列待选"}</small>
                <em>{simpleSelectedStripOpen ? selectedStripSourceHint : `展开查看配置列和来源 · ${selectedStripSourceHint}`}</em>
              </summary>
              {simpleSelectedStripOpen ? (
                <div className="product-config-selected-strip-details__cards">
                  {selectedDisplayTrims.map((trim) => renderSelectedDisplayCard(trim, false, false))}
                </div>
              ) : null}
            </details>
          ) : (
            selectedDisplayTrims.map((trim) => renderSelectedDisplayCard(trim, false))
          )
        ) : (
          <div className="comparison-empty-state product-config-empty">
            <strong>{prefilterLabel ? "当前筛选还没有选择可比配置列。" : "请选择至少 2 个配置列开始配置对比。"}</strong>
            <span>
              {prefilterLabel
                ? `筛选：${prefilterLabel}。请从候选列表添加 2-4 个配置列；库内未命中时上传当前筛选范围的来源。`
                : "可以查看本地 xlsx 样例里的同车型不同配置列，或上传 xlsx / PDF / CSV / HTML / 图片作为来源文件快照。"}
            </span>
            <small className="market-scan-field-hint">
              先用下拉搜索库内品牌 / 车型 / 配置列；不要手填配置列，库内未命中再上传来源文件。
            </small>
            <div className="product-config-empty-actions" aria-label="配置对比空状态入口">
              <div className="product-config-empty-actions__primary">
                <button className="btn btn-sm btn-primary" type="button" onClick={() => openControlPanel("filters")}>搜索配置列</button>
              </div>
              <div className="product-config-empty-actions__secondary" aria-label="Source Digest 补充入口">
                <small>{externalPrefilterActive ? "库内未命中时上传当前筛选范围的来源。" : "库内未命中时再上传来源或打开样例。"}</small>
                <div>
                  <button className="btn btn-sm btn-secondary" type="button" onClick={openComparisonSourcePanel}>{SOURCE_UPLOAD_CTA_LABEL}</button>
                  {!externalPrefilterActive ? (
                    <button className="btn btn-sm btn-secondary" type="button" onClick={openLocalDigestSample}>
                      查看本地 xlsx 样例
                    </button>
                  ) : null}
                </div>
              </div>
            </div>
          </div>
        )}
      </section>

      {showExpertContextBlocks ? renderIdentityNotes() : null}

      {showLocalDigestSection ? renderLocalDigestSection() : null}

      {summaryMode === "expert" || !displayCompareData ? renderComparisonSummarySection() : null}

      {summaryMode === "expert" && displayCompareData && analysisScopeActive ? (
        <section className="product-config-analysis-scope" aria-label="当前配置分析口径">
          <div className="product-config-analysis-scope__body">
            <span className="market-scan-panel-eyebrow">Current Scope</span>
            {simpleModeActive ? <strong className="product-config-analysis-scope__summary">{summaryShownLabel}</strong> : null}
            <div className="product-config-analysis-scope__chips">
              {analysisScopeItems.map((item) => (
                <span className="product-config-analysis-scope__chip" key={item.key}>
                  <small>{item.label}</small>
                  <strong>{item.value}</strong>
                </span>
              ))}
            </div>
            <p>{simpleModeActive ? "顶部状态、配置大类和下方表格会按这个口径联动；来源证据仍可从单元格追溯。" : "业务摘要、配置大类和下方表格会按这个口径联动；来源证据仍可从单元格追溯。"}</p>
          </div>
          <div className="product-config-analysis-scope__actions">
            {rowScopeActive ? (
              <button className="btn btn-sm btn-secondary" type="button" onClick={restoreAllConfigRows}>
                {restoreAllConfigRowsLabel}
              </button>
            ) : null}
            {activeTargetTrim ? (
              <button className="btn btn-sm btn-secondary" type="button" onClick={showAllTargetColumns}>
                显示全部目标列
              </button>
            ) : null}
            {rowScopeActive && activeTargetTrim ? (
              <button className="btn btn-sm btn-secondary" type="button" onClick={clearAnalysisScope}>
                清空分析口径
              </button>
            ) : null}
          </div>
        </section>
      ) : null}

      {displayCompareData && summaryMode === "expert" ? renderInlineSummaryModeSwitch() : null}

      {displayCompareData ? renderBusinessSummaryPanel() : null}

      {renderCategoryNav()}

      {compareLoading ? <LoadingSurface mode="inline" label="刷新配置对比" /> : null}
      {displayCompareData ? (
        <Suspense fallback={<LoadingSurface mode="inline" label="加载配置表" />}>
          <LazyConfigComparisonTable
            data={displayCompareData}
            baseTrimId={baseTrimId}
            categoryFilter={activeCategoryFilter}
            categorySummaryMode={summaryMode === "simple" ? "compact" : "full"}
            cellEvidenceMode={summaryMode === "simple" ? "compact" : "full"}
            columnMode={summaryMode === "simple" ? "matrix" : "full"}
            deltaFilter={activeDeltaFilter}
            exportActionsRef={configTableExportActionsRef}
            focusedFeatureCode={focusedFeatureCode}
            focusedFeatureRequestKey={focusedFeatureRequestKey}
            legendMode={summaryMode === "simple" ? "compact" : "full"}
            searchValue={activeTableSearch}
            targetTrimId={activeTargetTrimId}
            toolbarMode={summaryMode === "simple" ? "simple" : "full"}
            businessSummaryExport={businessSummaryExportItems}
            businessSummaryUsage={businessSummaryExportUsage}
            valuesEditable={valuesCanBeEdited}
            factSource={compareFactSource}
            onCategoryFilterChange={setActiveCategoryFilter}
            onDeltaFilterChange={setTableDeltaFilter}
            onExportActionsChange={handleConfigTableExportStatusChange}
            onOpenEvidence={setEvidenceSelection}
            onSaveCell={valuesCanBeEdited ? saveCompareCellValue : undefined}
            onSearchChange={setActiveTableSearch}
            onTargetTrimChange={setActiveTargetTrimId}
          />
        </Suspense>
      ) : null}
      {evidenceSelection ? (
        <Suspense fallback={null}>
          <LazySourceEvidenceDrawer selection={evidenceSelection} onClose={() => setEvidenceSelection(null)} />
        </Suspense>
      ) : null}
    </section>
  );
}
