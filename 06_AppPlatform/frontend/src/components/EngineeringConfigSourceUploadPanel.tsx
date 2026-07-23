import { lazy, Suspense, useEffect, useId, useState, type ChangeEvent, type DragEvent, type ReactElement } from "react";
import { api } from "../api/client";
import { formatEngineeringConfigDigestDraftMetrics } from "../utils/engineeringConfigDigestDraft";
import {
  engineeringConfigOcrComparisonText,
  isOcrSemanticStrategy,
} from "../utils/engineeringConfigOcr";
import { SearchDropdownFilter, type SearchDropdownOption } from "./SearchDropdownFilter";
import type {
  EngineeringConfigBusinessSummaryReadiness,
  EngineeringConfigDigestTrimIdentityOverride,
  EngineeringConfigDigestDraftResult,
  EngineeringConfigOcrCandidate,
  EngineeringConfigOcrCandidateScore,
  EngineeringConfigOcrReadiness,
  EngineeringConfigSourceDigest,
  EngineeringConfigSourceDigestGroup,
  EngineeringConfigSourceDigestRow,
  EngineeringConfigSourceDigestStatus,
  EngineeringConfigSourceContext,
  EngineeringConfigSourceSnapshot,
  ParsePreview,
} from "../types/engineeringConfig";

const LazyEngineeringConfigAiSummaryReadinessCard = lazy(() => import("./EngineeringConfigAiSummaryReadinessCard").then((module) => ({
  default: module.EngineeringConfigAiSummaryReadinessCard,
})));

type UploadStage =
  | "select"
  | "uploading"
  | "assembling"
  | "parsing"
  | "matching"
  | "preview"
  | "storing"
  | "stored"
  | "confirming"
  | "done";

type UploadKind = "matrix" | "source" | "unsupported";
type SourceLibraryScope = "country" | "all" | "trash";
type SourceLibraryTypeFilterKey = "all" | "digest_ready" | "excel" | "tabular" | "pdf" | "ocr" | "ocr_temporary" | "price_list" | "pending";
type SourceLibraryTypeFilterItem = {
  key: SourceLibraryTypeFilterKey;
  label: string;
  count: number;
  description: string;
};
type SourceDigestStatusLike = EngineeringConfigSourceDigest | EngineeringConfigSourceDigestStatus;
type SourceDigestTrimSelectionMap = Record<string, string[]>;
type SourceDigestReviewFocusMap = Record<string, string>;
type SourceDigestTrimIdentityDraftMap = Record<string, Record<string, EngineeringConfigDigestTrimIdentityOverride>>;
type SourceDigestTrimIdentityFieldKey = Exclude<keyof EngineeringConfigDigestTrimIdentityOverride, "trimId">;
type SourceDigestExportFormat = "xlsx" | "pdf";
const SOURCE_SNAPSHOT_MATCH_CHIP_LIMIT = 5;
const SOURCE_DIGEST_PREVIEW_ROW_LIMIT = 8;
const SOURCE_LIBRARY_TYPE_FILTERS: Array<Omit<SourceLibraryTypeFilterItem, "count">> = [
  { key: "all", label: "全部", description: "当前已加载来源" },
  { key: "digest_ready", label: "可转配置列", description: "已识别可比配置组" },
  { key: "excel", label: "Excel", description: "xlsx / xls / xlsm 来源" },
  { key: "tabular", label: "表格文本", description: "CSV / TSV / HTML / 网页表格" },
  { key: "pdf", label: "PDF", description: "文本 PDF 或扫描 PDF" },
  { key: "ocr", label: "OCR", description: "图片或扫描件 OCR" },
  { key: "ocr_temporary", label: "临时 OCR 列", description: "OCR 未识别表头，需补真实配置列身份" },
  { key: "price_list", label: "价格单", description: "价格字段转配置列" },
  { key: "pending", label: "待处理", description: "等待抽取或 OCR 配置" },
];

const CHUNK_SIZE = 5 * 1024 * 1024;
const SOURCE_DIGEST_AI_BOUNDARY_HINT = "上传只保存来源文件和 Source Digest；AI 摘要回到配置对比页后，按当前已选配置列实时生成。";
const MATRIX_EXTENSIONS = [".xlsx", ".xlsm", ".xls"];
const SOURCE_EXTENSIONS = [
  ...MATRIX_EXTENSIONS,
  ".pdf",
  ".csv",
  ".tsv",
  ".html",
  ".htm",
  ".png",
  ".jpg",
  ".jpeg",
  ".webp",
];
const ACCEPTED_SOURCE_FILES = SOURCE_EXTENSIONS.join(",");
const SOURCE_EXTENSION_LABEL = SOURCE_EXTENSIONS.join(" / ");

function digestDraftActionKey(sourceId: string | null | undefined, groupId: string): string {
  return `${sourceId || "source"}::${groupId}`;
}

function sourceDigestExportActionKey(
  sourceId: string | null | undefined,
  groupId: string,
  format: SourceDigestExportFormat,
): string {
  return `${digestDraftActionKey(sourceId, groupId)}::${format}`;
}

function sourceDigestTrimId(trim: EngineeringConfigSourceDigestGroup["trims"][number]): string {
  return trim.trimId.trim();
}

function sourceDigestTrimLabel(trim: EngineeringConfigSourceDigestGroup["trims"][number]): string {
  return trim.trimName || trim.fullTrimName || trim.trimId;
}

function sourceDigestReviewRowKey(row: EngineeringConfigSourceDigestRow, rowIndex: number): string {
  return `${row.featureCode || row.featureName || "review-row"}::${rowIndex}`;
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
  { key: "trimName", label: "配置列 / Trim", required: true, placeholder: "真实配置列名称" },
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

function sourceDigestTrimHasTemporaryOcrIdentity(trim: EngineeringConfigSourceDigestGroup["trims"][number]): boolean {
  return trim.identityStatus === "temporary_ocr_column"
    || (trim.trimName || trim.fullTrimName || "").trim().toLowerCase().startsWith("ocr column");
}

function sourceDigestGroupHasTemporaryOcrIdentity(group: EngineeringConfigSourceDigestGroup): boolean {
  return group.identityStatus === "temporary_ocr_column"
    || group.sourceKind === "ocr_headerless"
    || group.trims.some(sourceDigestTrimHasTemporaryOcrIdentity);
}

function sourceDigestHasTemporaryOcrIdentity(digest: SourceDigestStatusLike | null | undefined): boolean {
  return digestCompareGroups(digest).some(sourceDigestGroupHasTemporaryOcrIdentity);
}

function sourceDigestTemporaryOcrIdentityMeta(group: EngineeringConfigSourceDigestGroup): string | null {
  if (!sourceDigestGroupHasTemporaryOcrIdentity(group)) return null;
  const temporaryCount = group.trims.filter(sourceDigestTrimHasTemporaryOcrIdentity).length;
  const countLabel = temporaryCount > 0 ? `${temporaryCount}/${group.trimCount} 列` : `${group.trimCount} 列`;
  return `OCR 临时列身份 ${countLabel}：未识别到真实配置列表头，创建前需补真实车型 / 配置列身份。`;
}

function sourceDigestTrimAnchorMeta(trim: EngineeringConfigSourceDigestGroup["trims"][number]): string {
  if (sourceDigestTrimHasTemporaryOcrIdentity(trim)) {
    return trim.identityNote?.trim() || "临时 OCR 列 · 待补真实配置列身份";
  }
  return trim.materialNo || trim.salesVersion || trim.profile?.configurationVersion || trim.profile?.materialNo || "来源锚点待补";
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

function sourceDigestTrimIdentityDefaults(
  group: EngineeringConfigSourceDigestGroup,
  trim: EngineeringConfigSourceDigestGroup["trims"][number],
  sourceContext: EngineeringConfigSourceContext | null | undefined,
): EngineeringConfigDigestTrimIdentityOverride {
  const temporaryOcrIdentity = sourceDigestTrimHasTemporaryOcrIdentity(trim);
  const modelName = trim.modelName || group.modelName || sourceContext?.model || "";
  const trimName = temporaryOcrIdentity ? "" : sourceDigestTrimLabel(trim);
  const market = trim.market || trim.country || trim.profile?.country || sourceContext?.market || sourceContext?.country || "";
  return {
    trimId: sourceDigestTrimId(trim),
    brand: trim.profile?.brand || sourceContext?.brand || "",
    modelName,
    trimName,
    fullTrimName: temporaryOcrIdentity ? "" : [modelName, trimName].filter(Boolean).join(" "),
    market,
    country: trim.country || trim.profile?.country || sourceContext?.country || sourceContext?.market || market,
    modelYear: trim.profile?.modelYear || sourceContext?.modelYear || "",
    energyType: trim.energyType || trim.energy_type || trim.powertrain || trim.profile?.energyType || trim.profile?.powertrain || sourceContext?.powertrain || "",
    drivetrain: trim.drivetrain || trim.profile?.drivetrain || "",
    engine: trim.engine || trim.profile?.engine || "",
    materialNo: trim.materialNo || trim.profile?.materialNo || "",
    salesVersion: trim.salesVersion || trim.profile?.configurationVersion || "",
  };
}

function sourceDigestTrimIdentityDraftValue(
  sourceId: string | null | undefined,
  group: EngineeringConfigSourceDigestGroup,
  trim: EngineeringConfigSourceDigestGroup["trims"][number],
  sourceContext: EngineeringConfigSourceContext | null | undefined,
  drafts: SourceDigestTrimIdentityDraftMap,
): EngineeringConfigDigestTrimIdentityOverride {
  const actionKey = digestDraftActionKey(sourceId, group.groupId);
  return {
    ...sourceDigestTrimIdentityDefaults(group, trim, sourceContext),
    ...(drafts[actionKey]?.[sourceDigestTrimId(trim)] ?? {}),
    trimId: sourceDigestTrimId(trim),
  };
}

function sourceDigestSelectedTemporaryOcrTrims(
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[],
): EngineeringConfigSourceDigestGroup["trims"] {
  const selectedIdSet = new Set(selectedTrimIds);
  return group.trims.filter((trim) => (
    selectedIdSet.has(sourceDigestTrimId(trim)) && sourceDigestTrimHasTemporaryOcrIdentity(trim)
  ));
}

function sourceDigestTemporaryIdentityReady(
  sourceId: string | null | undefined,
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[],
  sourceContext: EngineeringConfigSourceContext | null | undefined,
  drafts: SourceDigestTrimIdentityDraftMap,
): boolean {
  const temporaryTrims = sourceDigestSelectedTemporaryOcrTrims(group, selectedTrimIds);
  if (temporaryTrims.length === 0) return true;
  return temporaryTrims.every((trim) => {
    const draft = sourceDigestTrimIdentityDraftValue(sourceId, group, trim, sourceContext, drafts);
    return Boolean(draft.modelName?.trim() && draft.trimName?.trim());
  });
}

function sourceDigestTrimIdentityOverridePayload(
  sourceId: string | null | undefined,
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[],
  sourceContext: EngineeringConfigSourceContext | null | undefined,
  drafts: SourceDigestTrimIdentityDraftMap,
): EngineeringConfigDigestTrimIdentityOverride[] {
  return sourceDigestSelectedTemporaryOcrTrims(group, selectedTrimIds).map((trim) => {
    const draft = sourceDigestTrimIdentityDraftValue(sourceId, group, trim, sourceContext, drafts);
    const override: EngineeringConfigDigestTrimIdentityOverride = { trimId: sourceDigestTrimId(trim) };
    SOURCE_DIGEST_TRIM_IDENTITY_FIELDS.forEach((field) => {
      const value = cleanSourceDigestIdentityValue(draft[field]);
      if (value) override[field] = value;
    });
    return override;
  });
}

function sourceDigestSelectedTrimPayload(
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[] | undefined,
): { trimIds: string[] } | undefined {
  const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
  const all = group.trims.map(sourceDigestTrimId).filter(Boolean);
  const selectedDiffersFromAll = selected.length !== all.length || selected.some((trimId, index) => trimId !== all[index]);
  return selectedDiffersFromAll ? { trimIds: selected } : undefined;
}

function fileExtension(fileName: string): string {
  const index = fileName.lastIndexOf(".");
  return index >= 0 ? fileName.slice(index).toLowerCase() : "";
}

function sourceSnapshotDigest(snapshot: EngineeringConfigSourceSnapshot): SourceDigestStatusLike | null {
  return snapshot.sourceDigest ?? snapshot.sourceDigestStatus ?? null;
}

function sourceSnapshotIsExcel(snapshot: EngineeringConfigSourceSnapshot): boolean {
  const digest = sourceSnapshotDigest(snapshot);
  const extension = fileExtension(snapshot.sourceFileName);
  return digest?.digestType === "workbook" || MATRIX_EXTENSIONS.includes(extension);
}

function sourceSnapshotIsTabular(snapshot: EngineeringConfigSourceSnapshot): boolean {
  const digest = sourceSnapshotDigest(snapshot);
  const extension = fileExtension(snapshot.sourceFileName);
  return digest?.digestType === "tabular"
    || digest?.sourceFormat === "tabular"
    || [".csv", ".tsv", ".html", ".htm"].includes(extension);
}

function sourceSnapshotIsPdf(snapshot: EngineeringConfigSourceSnapshot): boolean {
  const digest = sourceSnapshotDigest(snapshot);
  return digest?.digestType === "pdf_text"
    || digest?.digestType === "pdf_ocr"
    || digest?.sourceFormat === "pdf_text"
    || digest?.sourceFormat === "pdf_ocr"
    || fileExtension(snapshot.sourceFileName) === ".pdf";
}

function sourceSnapshotIsOcr(snapshot: EngineeringConfigSourceSnapshot): boolean {
  const digest = sourceSnapshotDigest(snapshot);
  return digest?.digestType === "image_ocr"
    || digest?.digestType === "pdf_ocr"
    || digest?.sourceFormat === "image_ocr"
    || digest?.sourceFormat === "pdf_ocr"
    || Boolean(digest?.ocrEngine || digest?.ocrEvaluation);
}

function sourceSnapshotIsPending(snapshot: EngineeringConfigSourceSnapshot): boolean {
  const digest = sourceSnapshotDigest(snapshot);
  return snapshot.extractStatus === "pending" || digest?.status === "pending";
}

function sourceSnapshotMatchesTypeFilter(
  snapshot: EngineeringConfigSourceSnapshot,
  filter: SourceLibraryTypeFilterKey,
): boolean {
  const digest = sourceSnapshotDigest(snapshot);
  if (filter === "all") return true;
  if (filter === "digest_ready") return snapshot.extractStatus === "digest_ready" && Boolean(digest?.status === "ready");
  if (filter === "excel") return sourceSnapshotIsExcel(snapshot);
  if (filter === "tabular") return sourceSnapshotIsTabular(snapshot);
  if (filter === "pdf") return sourceSnapshotIsPdf(snapshot);
  if (filter === "ocr") return sourceSnapshotIsOcr(snapshot);
  if (filter === "ocr_temporary") return sourceDigestHasTemporaryOcrIdentity(digest);
  if (filter === "price_list") return Boolean(digest && sourceDigestHasPriceList(digest));
  if (filter === "pending") return sourceSnapshotIsPending(snapshot);
  return true;
}

function sourceLibraryTypeFilterItems(snapshots: EngineeringConfigSourceSnapshot[]): SourceLibraryTypeFilterItem[] {
  return SOURCE_LIBRARY_TYPE_FILTERS.map((filter) => ({
    ...filter,
    count: snapshots.filter((snapshot) => sourceSnapshotMatchesTypeFilter(snapshot, filter.key)).length,
  }));
}

function classifyUpload(file: File | null): UploadKind {
  if (!file) return "unsupported";
  const extension = fileExtension(file.name);
  if (MATRIX_EXTENSIONS.includes(extension)) return "matrix";
  if (SOURCE_EXTENSIONS.includes(extension)) return "source";
  return "unsupported";
}

function selectedFileKindLabel(uploadKind: UploadKind): string {
  if (uploadKind === "matrix") return "Excel 来源文件，可生成 Source Digest";
  if (uploadKind === "source") return "来源文件，可生成 Source Digest";
  return "不支持的文件";
}

function formatFileSize(size: number): string {
  if (size >= 1024 * 1024) return `${(size / 1024 / 1024).toFixed(2)} MB`;
  if (size >= 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${size} B`;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function numericValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function sourceStatusLabel(snapshot: EngineeringConfigSourceSnapshot): string {
  if (snapshot.inTrash || snapshot.uploadStatus === "trashed") return "垃圾桶";
  if (snapshot.uploadStatus === "duplicate" || snapshot.duplicate) return "重复文件";
  if (snapshot.uploadStatus === "registered") return "已登记";
  return snapshot.uploadStatus || "已登记";
}

function extractStatusLabel(snapshot: EngineeringConfigSourceSnapshot): string {
  if (snapshot.extractStatus === "digest_ready") return "Digest 就绪";
  if (snapshot.extractStatus === "pending") return "待抽取";
  if (snapshot.extractStatus === "not_applicable") return "矩阵导入";
  return snapshot.extractStatus || "待处理";
}

function fileTypeLabel(fileType: string): string {
  if (fileType === "source_document") return "source";
  return fileType.toUpperCase();
}

function formatDateTime(value: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function sourceOwnerLabel(createdBy: string | null | undefined): string {
  return createdBy?.trim() ? `上传人 ${createdBy.trim()}` : "上传人待补";
}

function hasSourceContextSignal(context: EngineeringConfigSourceContext | undefined): boolean {
  if (!context) return false;
  return Boolean(context.brand || context.model || context.market || context.powertrain || context.segment || context.modelYear || context.trimIds.length > 0 || (context.salesVersionIds?.length ?? 0) > 0);
}

function sourceIdentityAnchorPart(context: EngineeringConfigSourceContext): string | null {
  const identityAnchors = context.salesVersionIds?.map((value) => value.trim()).filter(Boolean) ?? [];
  if (identityAnchors.length > 0) {
    const visibleAnchors = identityAnchors.slice(0, 3);
    return `身份锚点 ${visibleAnchors.join(" / ")}${identityAnchors.length > visibleAnchors.length ? ` / +${identityAnchors.length - visibleAnchors.length}` : ""}`;
  }
  if (context.identityAnchor === "brand_model_market") return "身份锚点 品牌 / 车型 / 市场";
  if (context.identityAnchor) return `身份锚点 ${context.identityAnchor}`;
  return null;
}

function sourceScenarioPart(context: EngineeringConfigSourceContext): string | null {
  if (context.contextType === "product_compare_prefilter_upload") return "当前筛选来源补充";
  if (context.scenario === "filtered_config_library_miss") return "当前筛选来源补充";
  if (context.contextType === "competitor_recommendation_source_digest_supplement") return "推荐竞品补充来源";
  if (context.scenario === "recommended_competitor_source_digest_supplement") return "推荐竞品补充来源";
  if (context.contextType === "competitor_recommendation_source_digest") return "推荐竞品来源已入库";
  if (context.scenario === "recommended_competitor_source_digest_available") return "推荐竞品来源已入库";
  if (context.contextType === "competitor_recommendation_upload") return "推荐竞品缺口";
  if (context.scenario === "recommended_competitor_config_gap") return "推荐竞品缺口";
  if (context.scenario) return `场景 ${context.scenario}`;
  return null;
}

function sourceContextParts(context: EngineeringConfigSourceContext): string[] {
  const parts = [
    context.brand,
    context.model,
    context.market || context.country,
    context.powertrain,
    context.segment,
    context.modelYear,
  ].filter((value): value is string => Boolean(value));
  if (context.contextType === "model_trim_compare_target") parts.push("目标口径");
  const scenarioPart = sourceScenarioPart(context);
  if (scenarioPart) parts.push(scenarioPart);
  const identityAnchorPart = sourceIdentityAnchorPart(context);
  if (identityAnchorPart) parts.push(identityAnchorPart);
  if (context.trimIds.length > 0) parts.push(`已选 ${context.trimIds.length} 配置列`);
  return parts;
}

function sourceRegistrationContext(context: EngineeringConfigSourceContext | undefined): EngineeringConfigSourceContext | undefined {
  if (!context) return undefined;
  if (
    context.contextType === "competitor_recommendation_source_digest"
    || context.scenario === "recommended_competitor_source_digest_available"
  ) {
    return {
      ...context,
      contextType: "competitor_recommendation_source_digest_supplement",
      scenario: "recommended_competitor_source_digest_supplement",
    };
  }
  return context;
}

function sourceContextTaskHint(context: EngineeringConfigSourceContext): string {
  if (
    context.contextType === "competitor_recommendation_source_digest"
    || context.scenario === "recommended_competitor_source_digest_available"
  ) {
    return "这是从高级分析推荐竞品进入的来源任务：优先用已入库 Source Digest 创建可编辑配置列，创建后回到 FloatingDeck 加入当前对比；现有 digest 不够时再上传补充来源。";
  }
  if (
    context.contextType === "competitor_recommendation_upload"
    || context.scenario === "recommended_competitor_config_gap"
  ) {
    return "这是推荐竞品资料缺口：先上传该国家 / 动力 / segment 的配置表或价格单，Digest 成功后创建可编辑配置列并加入当前对比。";
  }
  if (context.contextType === "model_trim_compare_target") {
    return "这是当前目标配置列的来源核对任务：补传或关联来源后，可回到表格单元格查看 evidence。";
  }
  if (
    context.contextType === "product_compare_prefilter_upload"
    || context.scenario === "filtered_config_library_miss"
  ) {
    return "这是当前筛选范围的来源补充任务：上传该国家 / 车型 / 动力的配置表或价格单，Digest 成功后生成在线配置列再加入对比。";
  }
  return "来源会带着当前上下文入库，后续可按国家、车型、来源和身份锚点搜索复用。";
}

function uniquePresentStrings(values: Array<string | null | undefined>): string[] {
  return [...new Set(values.map((value) => value?.trim()).filter((value): value is string => Boolean(value)))];
}

function compactValueList(values: string[]): string {
  const visible = values.slice(0, 3);
  return `${visible.join(" / ")}${values.length > visible.length ? ` / +${values.length - visible.length}` : ""}`;
}

function sourceDigestPowertrainValues(digest: SourceDigestStatusLike | null | undefined): string[] {
  const groups = digestCompareGroups(digest);
  const trims = groups.flatMap((group) => group.trims);
  return uniquePresentStrings(trims.flatMap((trim) => [
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
}

function sourceSnapshotSearchMatchChips(snapshot: EngineeringConfigSourceSnapshot): string[] {
  const matches = uniquePresentStrings(snapshot.sourceSearchMatches ?? []);
  if (matches.length <= SOURCE_SNAPSHOT_MATCH_CHIP_LIMIT) return matches;
  return [
    ...matches.slice(0, SOURCE_SNAPSHOT_MATCH_CHIP_LIMIT),
    `+${matches.length - SOURCE_SNAPSHOT_MATCH_CHIP_LIMIT}`,
  ];
}

function digestCompareGroups(digest: SourceDigestStatusLike | null | undefined): EngineeringConfigSourceDigestGroup[] {
  if (!digest || !("compareGroups" in digest)) return [];
  return digest.compareGroups;
}

function sourceDigestIdentityParts(digest: SourceDigestStatusLike | null | undefined): string[] {
  const groups = digestCompareGroups(digest);
  if (groups.length === 0) return [];
  const trims = groups.flatMap((group) => group.trims);
  const markets = uniquePresentStrings(trims.flatMap((trim) => [trim.market, trim.country, trim.profile?.country]));
  const modelYears = uniquePresentStrings(trims.map((trim) => trim.profile?.modelYear));
  const powertrains = sourceDigestPowertrainValues(digest);
  const materialCount = trims.filter((trim) => Boolean(trim.materialNo || trim.hasMaterialNo || trim.profile?.materialNo)).length;
  const noMaterialCount = Math.max(trims.length - materialCount, 0);
  const salesVersionCount = trims.filter((trim) => Boolean(trim.salesVersion || trim.profile?.configurationVersion)).length;
  const identityParts: string[] = [];
  if (markets.length > 0) identityParts.push(compactValueList(markets));
  if (modelYears.length > 0) identityParts.push(`MY ${compactValueList(modelYears)}`);
  if (powertrains.length > 0) identityParts.push(`动力 ${compactValueList(powertrains)}`);
  if (groups.some((group) => group.sourceKind === "price_list")) identityParts.push("价格单");
  if (groups.some(sourceDigestGroupHasTemporaryOcrIdentity)) identityParts.push("临时 OCR 列身份待补");
  if (materialCount === trims.length && trims.length > 0) {
    identityParts.push("物料号齐全");
  } else if (materialCount > 0) {
    identityParts.push(`物料号 ${materialCount}/${trims.length}`);
  }
  if (noMaterialCount > 0) {
    identityParts.push(salesVersionCount > 0 ? `无物料号 ${noMaterialCount}，Sales version ${salesVersionCount}` : `无物料号 ${noMaterialCount}`);
  }
  return identityParts;
}

function sourceSnapshotSearchOption(snapshot: EngineeringConfigSourceSnapshot): SearchDropdownOption {
  const context = sourceContextParts(snapshot.relatedContext).join(" · ");
  const matches = sourceSnapshotSearchMatchChips(snapshot);
  const matchMeta = matches.length > 0
    ? `命中 ${matches.join(" / ")}`
    : "";
  const digestStatus = snapshot.sourceDigest ?? snapshot.sourceDigestStatus;
  const digestMeta = snapshot.sourceDigest
    ? `${snapshot.sourceDigest.compareGroups.length} 可比组 · ${snapshot.sourceDigest.summary.candidateTrimCount} 配置列`
    : digestStatus
      ? `${sourceDigestTypeLabel(digestStatus)} · ${sourceDigestStatusLabel(digestStatus)}`
    : extractStatusLabel(snapshot);
  const digestIdentity = sourceDigestIdentityParts(digestStatus).join(" · ");
  return {
    value: snapshot.sourceId,
    label: snapshot.sourceFileName,
    meta: [fileTypeLabel(snapshot.fileType), sourceOwnerLabel(snapshot.createdBy), matchMeta, context, digestMeta, digestIdentity].filter(Boolean).join(" · "),
  };
}

function sourceDigestSummaryParts(digest: EngineeringConfigSourceDigest): string[] {
  const summary = digest.summary;
  return [
    `${summary.sheetCount} sheets`,
    `${summary.candidateTrimCount} 候选配置列`,
    `${summary.comparableGroupCount} 组可比`,
    `${summary.differenceCount} 项差异`,
  ];
}

function sourceDigestHasPriceList(digest: SourceDigestStatusLike): boolean {
  return "compareGroups" in digest && digest.compareGroups.some((group) => group.sourceKind === "price_list");
}

function sourceDigestTypeLabel(digest: SourceDigestStatusLike): string {
  if (sourceDigestHasTemporaryOcrIdentity(digest)) return "OCR 临时列 digest";
  if (sourceDigestHasPriceList(digest)) return "价格单 digest";
  if (digest.digestType === "workbook") return "Excel digest";
  if (digest.digestType === "tabular") return "表格文本 digest";
  if (digest.digestType === "pdf_text") return "文本 PDF digest";
  if (digest.digestType === "pdf_ocr") return "扫描 PDF OCR digest";
  if (digest.digestType === "image_ocr") return "图片 OCR digest";
  if (digest.digestType === "unavailable") return "Digest 不可用";
  return `${digest.digestType || "source"} digest`;
}

function sourceDigestStatusLabel(digest: SourceDigestStatusLike): string {
  if (digest.status === "ready") return "可转可编辑配置列";
  if (digest.status === "pending" && digest.digestType === "pdf_ocr") return "OCR 待配置";
  if (digest.status === "pending" && digest.digestType === "image_ocr") return "OCR 待配置";
  if (digest.status === "pending") return "等待抽取";
  if (digest.status === "failed") return "Digest 失败";
  return digest.status;
}

function sourceDigestOcrSummaryParts(digest: SourceDigestStatusLike): string[] {
  const candidates = digest.ocrEngineCandidates ?? [];
  const evaluation = digest.ocrEvaluation ?? null;
  const selectedCandidate = candidates.find((candidate) => candidate.selected);
  const selectedScore = evaluation?.selectedScore ?? selectedCandidate?.score;
  const selectedEngine = evaluation?.selectedEngine || digest.ocrEngine || selectedCandidate?.engine;
  const candidateCount = evaluation?.candidateCount ?? candidates.length;
  const parts: string[] = [];

  if (selectedEngine) parts.push(`OCR 选择 ${selectedEngine}`);
  if (candidateCount > 0) parts.push(`候选 ${candidateCount}`);
  if (candidateCount > 0 && typeof evaluation?.comparableCandidateCount === "number") {
    parts.push(`可比候选 ${evaluation.comparableCandidateCount}/${candidateCount}`);
  }
  if (isOcrSemanticStrategy(evaluation?.strategy)) parts.push("按配置表语义选优");
  if (selectedScore?.featureCount) parts.push(`配置项 ${selectedScore.featureCount}`);
  if (selectedScore && selectedScore.rowCount > 0 && selectedScore.columnCount > 0) {
    parts.push(`选中表格 ${selectedScore.rowCount} x ${selectedScore.columnCount}`);
  }
  return parts;
}

function sourceDigestOcrHint(digest: SourceDigestStatusLike): string | null {
  const candidates = digest.ocrEngineCandidates ?? [];
  const evaluation = digest.ocrEvaluation ?? null;
  if (candidates.length === 0 && !evaluation) return null;
  const selectedCandidate = candidates.find((candidate) => candidate.selected);
  const selectedEngine = evaluation?.selectedEngine || digest.ocrEngine || selectedCandidate?.engine;
  const reasonDetails = evaluation?.selectedReasonDetails?.filter(Boolean) ?? [];
  if (reasonDetails.length > 0 && selectedEngine) {
    return `OCR 已比较 ${evaluation?.candidateCount ?? candidates.length} 个候选，最终采用 ${selectedEngine}：${reasonDetails.slice(0, 2).join(" ")}`;
  }
  if (isOcrSemanticStrategy(evaluation?.reason) && selectedEngine) {
    return `OCR 已比较 ${evaluation?.candidateCount ?? candidates.length} 个候选，优先按配置项、差异项和配置列质量采用 ${selectedEngine}。`;
  }
  if (selectedCandidate?.comparableTableDetected && selectedEngine) {
    return `OCR 候选引擎已按配置表语义和表格形态打分，最终采用 ${selectedEngine}。`;
  }
  if (candidates.some((candidate) => candidate.message)) {
    return "OCR 已返回文本，但候选结果暂未形成可横向对比的配置表。";
  }
  return null;
}

function formatOcrScoreNumber(value: number): string {
  if (!Number.isFinite(value)) return "0";
  return value.toFixed(2).replace(/\.?0+$/, "");
}

function ocrCandidateScoreParts(score: EngineeringConfigOcrCandidateScore | undefined): string[] {
  if (!score) return [];
  const parts: string[] = [];
  if ((score.featureCount ?? 0) > 0) parts.push(`配置项 ${score.featureCount}`);
  if ((score.differenceCount ?? 0) > 0) parts.push(`差异 ${score.differenceCount}`);
  if ((score.candidateTrimCount ?? 0) > 0) parts.push(`配置列 ${score.candidateTrimCount}`);
  if ((score.comparableGroupCount ?? 0) > 0) parts.push(`候选组 ${score.comparableGroupCount}`);
  if (score.rowCount > 0 || score.columnCount > 0) parts.push(`表格 ${score.rowCount} x ${score.columnCount}`);
  parts.push(`非空 ${score.nonEmptyCount}`);
  const scoreLabel = typeof score.semanticScore === "number" ? "语义分" : "表格评分";
  parts.push(`${scoreLabel} ${formatOcrScoreNumber(score.semanticScore ?? score.tableShapeScore)}`);
  return parts;
}

function ocrCandidateMetaParts(candidate: EngineeringConfigOcrCandidate): string[] {
  const parts = [
    candidate.sheetName,
    typeof candidate.pageNumber === "number" ? `第 ${candidate.pageNumber} 页` : null,
    candidate.comparableTableDetected === true ? "识别到可比表" : null,
    candidate.comparableTableDetected === false ? "未识别可比表" : null,
    ...ocrCandidateScoreParts(candidate.score),
  ];
  return parts.filter((part): part is string => Boolean(part));
}

function ocrCandidateStatusLabel(candidate: EngineeringConfigOcrCandidate, isSelected: boolean): string {
  if (isSelected) return "已采用";
  if (candidate.message && candidate.comparableTableDetected === false) return "候选失败";
  if (candidate.comparableTableDetected) return "可用候选";
  return "未采用";
}

function ocrCandidateClassName(candidate: EngineeringConfigOcrCandidate, isSelected: boolean): string {
  return [
    "config-source-ocr-candidate",
    isSelected ? "is-selected" : null,
    !isSelected && candidate.message && candidate.comparableTableDetected === false ? "is-failed" : null,
  ].filter(Boolean).join(" ");
}

function renderSourceDigestOcrCandidateComparison(digest: SourceDigestStatusLike): ReactElement | null {
  const candidates = digest.ocrEngineCandidates ?? [];
  if (candidates.length === 0) return null;
  const evaluation = digest.ocrEvaluation ?? null;
  const selectedEngine = evaluation?.selectedEngine || digest.ocrEngine || candidates.find((candidate) => candidate.selected)?.engine || null;
  const hasExplicitSelection = candidates.some((candidate) => candidate.selected);
  const strategyLabel = isOcrSemanticStrategy(evaluation?.strategy)
    ? "按配置表语义选优"
    : selectedEngine
      ? `采用 ${selectedEngine}`
      : `${candidates.length} 个候选`;
  const reasonDetails = evaluation?.selectedReasonDetails?.filter(Boolean).slice(0, 2) ?? [];
  const comparisonText = engineeringConfigOcrComparisonText(digest);

  return (
    <div className="config-source-ocr-candidates" aria-label="OCR 候选对比">
      <div className="config-source-ocr-candidates__head">
        <strong>OCR 候选对比</strong>
        <span>{strategyLabel}</span>
      </div>
      {reasonDetails.length > 0 ? (
        <div className="config-source-ocr-candidates__reason" aria-label="OCR 选优原因">
          {reasonDetails.map((detail) => <span key={detail}>{detail}</span>)}
        </div>
      ) : null}
      {comparisonText ? (
        <small className="config-source-ocr-candidates__summary">{comparisonText}</small>
      ) : null}
      {candidates.map((candidate, index) => {
        const isSelected = Boolean(candidate.selected || (!hasExplicitSelection && selectedEngine === candidate.engine));
        const metaParts = ocrCandidateMetaParts(candidate);
        return (
          <div
            className={ocrCandidateClassName(candidate, isSelected)}
            key={`${candidate.engine}-${candidate.sheetName || "sheet"}-${candidate.pageNumber ?? index}`}
          >
            <div>
              <strong>{candidate.engine}</strong>
              <span>{metaParts.length > 0 ? metaParts.join(" · ") : "未返回表格评分"}</span>
            </div>
            <span className="config-source-ocr-candidate__status">
              {ocrCandidateStatusLabel(candidate, isSelected)}
            </span>
            {candidate.textPreview ? (
              <small className="config-source-ocr-candidate__preview">
                识别原文{candidate.lineCount ? ` ${candidate.lineCount} 行` : ""}：{candidate.textPreview}
              </small>
            ) : null}
            {candidate.message ? <small>{candidate.message}</small> : null}
          </div>
        );
      })}
    </div>
  );
}

function sourceSnapshotDigestStatusParts(digest: SourceDigestStatusLike | null | undefined): string[] {
  if (!digest) return [];
  const parts = [sourceDigestTypeLabel(digest)];
  parts.push(...sourceDigestOcrSummaryParts(digest).slice(0, 5));
  return parts;
}

function sourceSnapshotDigestStatusHint(digest: SourceDigestStatusLike | null | undefined): string | null {
  if (!digest) return null;
  return sourceDigestOcrHint(digest) || digest.message || digest.errorMessage || null;
}

function draftDigestTypeLabel(result: EngineeringConfigDigestDraftResult): string {
  if (result.sourceKind === "price_list") return "价格单";
  if (result.sourceDigestType === "workbook") return "Excel digest";
  if (result.sourceDigestType === "tabular") return "表格文本 digest";
  if (result.sourceDigestType === "pdf_text") return "文本 PDF digest";
  if (result.sourceDigestType === "pdf_ocr") return "扫描 PDF OCR digest";
  if (result.sourceDigestType === "image_ocr") return "图片 OCR digest";
  return result.sourceDigestType ? `${result.sourceDigestType} digest` : "Source digest";
}

function draftResultMetaParts(result: EngineeringConfigDigestDraftResult): string[] {
  const parts = [
    result.sourceFileName ? `来源 ${result.sourceFileName}` : null,
    result.groupTitle ? `组 ${result.groupTitle}` : null,
    draftDigestTypeLabel(result),
    result.sourceKind === "price_list" ? "价格单字段已转配置列" : null,
  ].filter((value): value is string => Boolean(value));

  const selectedEngine = result.ocrEvaluation?.selectedEngine || result.ocrEngine;
  if (selectedEngine) parts.push(`OCR ${selectedEngine}`);
  if (result.ocrEvaluation?.candidateCount) parts.push(`候选 ${result.ocrEvaluation.candidateCount}`);
  if (
    result.ocrEvaluation?.candidateCount
    && typeof result.ocrEvaluation.comparableCandidateCount === "number"
  ) {
    parts.push(`可比候选 ${result.ocrEvaluation.comparableCandidateCount}/${result.ocrEvaluation.candidateCount}`);
  }
  if (isOcrSemanticStrategy(result.ocrEvaluation?.reason) || isOcrSemanticStrategy(result.ocrEvaluation?.strategy)) {
    parts.push("按配置表语义选优");
  }
  return parts;
}

function renderDigestDraftResult(
  result: EngineeringConfigDigestDraftResult,
  linkedToCompare: boolean,
): ReactElement {
  return (
    <div className="config-source-draft-result" role="status">
      <strong>已创建可编辑配置列</strong>
      <span>{formatEngineeringConfigDigestDraftMetrics(result)}</span>
      <small>{draftResultMetaParts(result).join(" · ")}</small>
      <small>
        {linkedToCompare
          ? "已同步到当前配置对比表；可在 FloatingDeck 显示面板查看、编辑或导出。"
          : "已进入配置列库；可在 FloatingDeck 搜索并加入任意国家 / 车型对比。"}
      </small>
    </div>
  );
}

type SourceReceiptNextStepTone = "ready" | "pending" | "blocked" | "stored";

type SourceReceiptNextStep = {
  tone: SourceReceiptNextStepTone;
  title: string;
  description: string;
  metrics: string[];
  ocrHint?: string | null;
};

function sourceReceiptOcrMetrics(digest: SourceDigestStatusLike | null | undefined): string[] {
  if (!digest) return [];
  return sourceDigestOcrSummaryParts(digest)
    .filter((part) => !part.startsWith("配置项 "))
    .slice(0, 4);
}

function sourceReceiptNextStep(snapshot: EngineeringConfigSourceSnapshot): SourceReceiptNextStep {
  const digest = snapshot.sourceDigest;
  if (snapshot.extractStatus === "digest_ready" && digest?.status === "ready") {
    const comparableGroups = digest.compareGroups.filter((group) => group.trimCount >= 2);
    const ocrMetrics = sourceReceiptOcrMetrics(digest);
    if (comparableGroups.length > 0) {
      return {
        tone: "ready",
        title: "可创建可编辑配置列",
        description: "在下方选择一个车型组和 2-4 个配置列；也可回 FloatingDeck 按来源 / 车型 / 配置列定位，创建后再加入当前对比。",
        metrics: [
          `可创建 ${comparableGroups.length} 组可编辑配置列`,
          `候选配置列 ${digest.summary.candidateTrimCount}`,
          `配置项 ${digest.summary.featureCount}`,
          `差异 ${digest.summary.differenceCount}`,
          ...ocrMetrics,
        ],
        ocrHint: sourceDigestOcrHint(digest),
      };
    }
    return {
      tone: "blocked",
      title: "暂未识别到可编辑配置列",
      description: "Digest 已完成，但没有 2 个以上可横向对比的配置列；需要补充表格结构或重新上传更完整来源。",
      metrics: [
        `工作表 ${digest.summary.sheetCount}`,
        `候选配置列 ${digest.summary.candidateTrimCount}`,
        `配置项 ${digest.summary.featureCount}`,
        ...ocrMetrics,
      ],
      ocrHint: sourceDigestOcrHint(digest),
    };
  }
  if (digest?.status === "pending" || snapshot.extractStatus === "pending") {
    const ocrMetrics = sourceReceiptOcrMetrics(digest);
    return {
      tone: "pending",
      title: "等待抽取后才能创建配置列",
      description: "当前还没有可创建的配置列；处理 OCR / 文本抽取后再回到来源库创建。",
      metrics: digest
        ? [`来源类型 ${sourceDigestTypeLabel(digest)}`, `抽取状态 ${sourceDigestStatusLabel(digest)}`, ...ocrMetrics]
        : [`抽取状态 ${extractStatusLabel(snapshot)}`],
      ocrHint: digest ? sourceDigestOcrHint(digest) : null,
    };
  }
  if (digest?.status === "failed") {
    return {
      tone: "blocked",
      title: "Digest 失败，暂不能创建配置列",
      description: digest.errorMessage || sourceReceiptGuide(snapshot),
      metrics: [
        `来源类型 ${sourceDigestTypeLabel(digest)}`,
        `抽取状态 ${sourceDigestStatusLabel(digest)}`,
        ...sourceReceiptOcrMetrics(digest),
      ],
      ocrHint: sourceDigestOcrHint(digest),
    };
  }
  return {
    tone: "stored",
    title: "来源已进入共享库",
    description: sourceReceiptGuide(snapshot),
    metrics: [sourceStatusLabel(snapshot), extractStatusLabel(snapshot)],
  };
}

function renderSourceReceiptNextStep(snapshot: EngineeringConfigSourceSnapshot): ReactElement {
  const nextStep = sourceReceiptNextStep(snapshot);
  return (
    <div
      aria-label="来源上传下一步"
      className={`config-source-next-step config-source-next-step--${nextStep.tone}`}
      role="status"
    >
      <div>
        <span className="market-scan-panel-eyebrow">下一步</span>
        <strong>{nextStep.title}</strong>
        <small>{nextStep.description}</small>
        {nextStep.ocrHint ? (
          <small className="config-source-next-step__ocr-hint">OCR 透明度：{nextStep.ocrHint}</small>
        ) : null}
      </div>
      {nextStep.metrics.length > 0 ? (
        <div className="config-source-next-step__metrics">
          {nextStep.metrics.map((metric) => (
            <span key={metric}>{metric}</span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function sourceReceiptGuide(snapshot: EngineeringConfigSourceSnapshot): string {
  const digest = snapshot.sourceDigest;
  if (snapshot.extractStatus === "digest_ready" && digest && sourceDigestHasTemporaryOcrIdentity(digest)) {
    return "OCR 已生成临时配置列，但未确认真实配置列表头；创建可编辑配置列后需补品牌 / 车型 / 市场 / 配置列身份。";
  }
  if (snapshot.extractStatus === "digest_ready" && digest && sourceDigestHasPriceList(digest)) {
    return "价格单已识别到可对比的车型 / 版型和价格字段，可以先创建可编辑配置列，再加入 FloatingDeck 对比。";
  }
  if (snapshot.extractStatus === "digest_ready" && digest?.compareGroups?.length) {
    return "Digest 已识别到可横向对比的配置列，可以先创建可编辑配置列，再加入 FloatingDeck 对比。";
  }
  if (digest?.digestType === "image_ocr" && digest.status === "pending") {
    return "图片已进入来源库；当前 OCR 未就绪或未识别出配置表，暂不能生成可编辑配置列。";
  }
  if (digest?.digestType === "pdf_ocr" && digest.status === "pending") {
    return "扫描 PDF 已进入来源库；当前 OCR 未就绪或未识别出配置表，暂不能生成可编辑配置列。";
  }
  if (digest?.digestType === "pdf_text" && digest.status === "pending") {
    return "PDF 已进入来源库；当前未检测到可复制文本配置表，扫描 PDF 需要 OCR 引擎后再抽取。";
  }
  if (snapshot.extractStatus === "pending") {
    return "来源已进入共享库；待抽取完成后才能生成可编辑配置列并参与逐项对比。";
  }
  return "来源已进入共享库，可在当前国家来源库中复用、恢复或清理。";
}

function sourceLibraryScopeHint(
  effectiveScope: SourceLibraryScope,
  requestedScope: SourceLibraryScope,
  country: string | null,
  segment: string | null,
): string {
  if (effectiveScope === "trash") {
    if (!country) return "来源垃圾桶按国家隔离；请先选择 Market / Country 后再查看、恢复或清空。";
    return `垃圾桶只处理当前国家 ${country} 的来源；恢复后会回到共享来源库供团队继续检索。`;
  }
  if (effectiveScope === "country" && country) {
    const countryScope = [country, segment].filter(Boolean).join(" / ");
    return `上传后的来源快照进入共享配置库，并按当前国家 ${countryScope} 关联；其他用户在相同国家上下文也可搜索复用。`;
  }
  if (requestedScope === "country" && !country) {
    return "未选择国家时先浏览全部共享来源；建议在 FloatingDeck 选择 Market / Country，让上传来源按国家归档。";
  }
  return "全部范围用于跨国家、跨用户检索共享来源；这里只浏览和复用，删除或清空仍回到当前国家范围处理。";
}

function sourceUploadLibraryScopeItems(
  country: string | null,
  segment: string | null,
): Array<{ label: string; value: string }> {
  const countryScope = [country, segment].filter(Boolean).join(" / ");
  return [
    {
      label: "团队共享",
      value: "上传后进入共享来源库，团队用户可通过 FloatingDeck 搜索复用",
    },
    {
      label: "国家归档",
      value: countryScope || "未选择 Market / Country，建议先用 FloatingDeck 绑定国家",
    },
    {
      label: "垃圾桶隔离",
      value: country ? `按 ${country} 隔离恢复 / 清空` : "按国家隔离，选择国家后才能恢复 / 清空",
    },
  ];
}

type OcrReadinessTone = "ready" | "degraded" | "blocked" | "unknown";

function ocrReadinessTone(readiness: EngineeringConfigOcrReadiness | null): OcrReadinessTone {
  if (!readiness) return "unknown";
  if (readiness.ready || readiness.status === "ready") return "ready";
  if (readiness.status === "degraded") return "degraded";
  return "blocked";
}

function ocrReadinessTitle(readiness: EngineeringConfigOcrReadiness | null, error: string | null): string {
  if (error) return "OCR 环境状态暂不可用";
  if (!readiness) return "正在检查 OCR 环境";
  if (readiness.ready || readiness.status === "ready") return "PDF / 图片 OCR 已就绪";
  if (readiness.status === "degraded") return "OCR 可用但扫描 PDF 受限";
  return "OCR 尚未配置";
}

function ocrReadinessDescription(readiness: EngineeringConfigOcrReadiness | null, error: string | null): string {
  if (error) return error;
  if (!readiness) return "正在读取后端运行环境；Excel、CSV、HTML 和文本 PDF 不依赖 OCR。";
  if (readiness.ready || readiness.status === "ready") {
    return `默认 ${readiness.defaultEngine || "OCR"}，可处理图片和扫描 PDF；Excel、CSV、HTML 仍走结构化解析。`;
  }
  if (readiness.status === "degraded") {
    return "图片 OCR 可用，但扫描 PDF 可能缺少页面渲染或 OCR 组件；上传后可能进入待处理。";
  }
  return "未检测到可用 OCR 引擎；图片和扫描 PDF 会先入库待处理，Excel、CSV、HTML 和文本 PDF 仍可解析。";
}

function ocrReadinessMetricItems(readiness: EngineeringConfigOcrReadiness | null): Array<{ label: string; value: string; active: boolean }> {
  if (!readiness) {
    return [
      { label: "默认引擎", value: "检查中", active: false },
      { label: "图片 OCR", value: "检查中", active: false },
      { label: "扫描 PDF", value: "检查中", active: false },
    ];
  }
  return [
    { label: "默认引擎", value: readiness.defaultEngine || "未配置", active: Boolean(readiness.defaultEngine) },
    { label: "图片 OCR", value: readiness.imageOcrReady ? "可用" : "不可用", active: readiness.imageOcrReady },
    { label: "扫描 PDF", value: readiness.pdfOcrReady ? "可用" : "受限", active: readiness.pdfOcrReady },
    { label: "PDF 渲染", value: readiness.pdfRenderReady ? "可用" : "缺失", active: readiness.pdfRenderReady },
  ];
}

function ocrReadinessComparisonBoundary(readiness: EngineeringConfigOcrReadiness | null): string | null {
  if (!readiness || !readiness.imageOcrReady) return null;
  if (readiness.paddleOcrReady && readiness.legacyOcrReady) {
    return "PaddleOCR 和 legacy/custom OCR 均可用；上传图片或扫描 PDF 后会按配置表候选评分选优。";
  }
  if (readiness.paddleOcrReady && !readiness.legacyOcrReady) {
    return "当前仅 PaddleOCR 可用；可解析图片和扫描 PDF，但不会形成 PaddleOCR vs legacy/custom OCR 横向对比。";
  }
  if (!readiness.paddleOcrReady && readiness.legacyOcrReady) {
    return "当前仅 legacy/custom OCR 可用；可解析图片和扫描 PDF，但不能验证 PaddleOCR 是否更好。";
  }
  return null;
}

function renderOcrReadiness(
  readiness: EngineeringConfigOcrReadiness | null,
  error: string | null,
): ReactElement {
  const tone = error ? "blocked" : ocrReadinessTone(readiness);
  const warnings = [
    ...(readiness?.warnings.slice(0, 2) ?? []),
    ocrReadinessComparisonBoundary(readiness),
  ].filter((item): item is string => Boolean(item));
  return (
    <section className={`config-source-ocr-readiness config-source-ocr-readiness--${tone}`} aria-label="OCR 环境预检">
      <div className="config-source-ocr-readiness__copy">
        <span className="market-scan-panel-eyebrow">OCR 环境预检</span>
        <strong>{ocrReadinessTitle(readiness, error)}</strong>
        <small>{ocrReadinessDescription(readiness, error)}</small>
      </div>
      <div className="config-source-ocr-readiness__metrics">
        {ocrReadinessMetricItems(readiness).map((item) => (
          <span
            className={`config-source-ocr-readiness__metric ${item.active ? "is-active" : ""}`.trim()}
            key={item.label}
          >
            {item.label}<strong>{item.value}</strong>
          </span>
        ))}
      </div>
      {warnings.length > 0 ? (
        <div className="config-source-ocr-readiness__warnings">
          {warnings.map((warning) => <small key={warning}>{warning}</small>)}
        </div>
      ) : null}
    </section>
  );
}

function compactTrimName(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function digestValueLabel(row: EngineeringConfigSourceDigestRow, index: number): string {
  const value = row.values[index];
  return value?.rawValue || "-";
}

function sourceDigestTsvCell(value: string): string {
  if (!/[	\n\r"]/.test(value)) return value;
  return `"${value.replace(/"/g, "\"\"")}"`;
}

function sourceDigestGroupTsv(group: EngineeringConfigSourceDigestGroup, selectedTrimIds: string[] | undefined): string {
  const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
  const trimIndexById = new Map(group.trims.map((trim, index) => [sourceDigestTrimId(trim), index]));
  const visibleTrimIndexes = selected
    .map((trimId) => trimIndexById.get(trimId))
    .filter((index): index is number => typeof index === "number")
    .slice(0, 4);
  const header = [
    "配置大类",
    "配置项",
    ...visibleTrimIndexes.map((index) => sourceDigestTrimLabel(group.trims[index])),
  ];
  const rows = group.rows.map((row) => [
    row.category,
    row.featureName,
    ...visibleTrimIndexes.map((index) => digestValueLabel(row, index)),
  ]);
  return [header, ...rows].map((line) => line.map(sourceDigestTsvCell).join("\t")).join("\n");
}

function sourceDigestPreviewFileName(
  group: EngineeringConfigSourceDigestGroup,
  format: SourceDigestExportFormat,
): string {
  const safe = [group.modelName, group.sourceSheet || group.title]
    .filter(Boolean)
    .join("-")
    .replace(/[\\/:*?"<>|]+/g, "-")
    .replace(/\s+/g, "-")
    .slice(0, 90);
  return `${safe || "source-digest-preview"}-${new Date().toISOString().slice(0, 10)}.${format}`;
}

function sourceDigestExportPayload(
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[] | undefined,
  format: SourceDigestExportFormat,
): Record<string, unknown> {
  const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
  const trimIndexById = new Map(group.trims.map((trim, index) => [sourceDigestTrimId(trim), index]));
  const visibleTrimIndexes = selected
    .map((trimId) => trimIndexById.get(trimId))
    .filter((index): index is number => typeof index === "number")
    .slice(0, 4);
  const trims = visibleTrimIndexes.map((index) => group.trims[index]);
  return {
    fileName: sourceDigestPreviewFileName(group, format),
    scope: {
      title: group.title || group.modelName || "Source Digest Preview",
      rangeLabel: "Source Digest Preview",
      rowCount: group.rows.length,
    },
    summary: {
      totalFeatures: group.rows.length,
      shownFeatures: group.rows.length,
      differenceCount: group.differenceCount,
      candidateTrimCount: trims.length,
    },
    trims,
    rows: group.rows.map((row) => ({
      businessNote: row.businessNote || "",
      category: row.category,
      comparisonType: row.comparisonType,
      featureCode: row.featureCode,
      featureName: row.featureName,
      values: visibleTrimIndexes.map((index) => row.values[index] ?? null),
    })),
  };
}

function downloadSourceDigestBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function renderDigestPreviewTableRows(
  group: EngineeringConfigSourceDigestGroup,
  rows: EngineeringConfigSourceDigestRow[],
  visibleTrimIndexes: number[],
  options?: {
    focusedReviewRowKey?: string;
    rowIndexOffset?: number;
    onReviewRowFocus?: (row: EngineeringConfigSourceDigestRow, rowIndex: number) => void;
  },
): ReactElement[] {
  return rows.map((row, rowIndex) => {
    const fullRowIndex = rowIndex + (options?.rowIndexOffset ?? 0);
    const rowKey = sourceDigestReviewRowKey(row, fullRowIndex);
    const hasReviewNotes = Boolean(row.reviewNotes?.length);
    const focused = hasReviewNotes && options?.focusedReviewRowKey === rowKey;
    return (
      <div className="config-source-digest-table__row" key={`${group.groupId}-${rowKey}`}>
        <span className={`${hasReviewNotes ? "config-source-digest-table__feature-cell is-review" : "config-source-digest-table__feature-cell"} ${focused ? "is-focused" : ""}`.trim()}>
          {row.featureName}
          {hasReviewNotes ? (
            <>
              <small className="config-source-digest-table__review-note">
                需核对：{row.reviewNotes?.[0]}
              </small>
              {options?.onReviewRowFocus ? (
                <button
                  className={`config-source-digest-table__review-focus ${focused ? "is-active" : ""}`}
                  type="button"
                  aria-pressed={focused}
                  onClick={(event) => {
                    event.stopPropagation();
                    options.onReviewRowFocus?.(row, fullRowIndex);
                  }}
                >
                  {focused ? "已设为建列后定位" : "建列后定位此行"}
                </button>
              ) : null}
            </>
          ) : null}
        </span>
        {visibleTrimIndexes.map((trimIndex) => (
          <span key={`${row.featureCode}-${trimIndex}`}>{digestValueLabel(row, trimIndex)}</span>
        ))}
      </div>
    );
  });
}

function renderSourceDigestTemporaryIdentityEditor(
  sourceId: string | null | undefined,
  group: EngineeringConfigSourceDigestGroup,
  selectedTrimIds: string[],
  sourceContext: EngineeringConfigSourceContext | null | undefined,
  drafts: SourceDigestTrimIdentityDraftMap | undefined,
  onIdentityDraftChange: (group: EngineeringConfigSourceDigestGroup, trim: EngineeringConfigSourceDigestGroup["trims"][number], field: SourceDigestTrimIdentityFieldKey, value: string) => void,
): ReactElement | null {
  const temporaryTrims = sourceDigestSelectedTemporaryOcrTrims(group, selectedTrimIds);
  if (temporaryTrims.length === 0) return null;
  const activeDrafts = drafts ?? {};
  const ready = sourceDigestTemporaryIdentityReady(sourceId, group, selectedTrimIds, sourceContext, activeDrafts);
  return (
    <div className="product-config-source-digest-identity-editor" aria-label={`${group.modelName || group.title} OCR 临时列身份映射`}>
      <div className="product-config-source-digest-identity-editor__head">
        <strong>OCR 临时列身份映射</strong>
        <small>{ready ? "已满足建列身份要求" : "先补真实车型 / 配置列，再生成正式配置列"}</small>
      </div>
      {temporaryTrims.map((trim) => {
        const trimId = sourceDigestTrimId(trim);
        const draft = sourceDigestTrimIdentityDraftValue(sourceId, group, trim, sourceContext, activeDrafts);
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
                    onChange={(event) => onIdentityDraftChange(group, trim, field.key, event.currentTarget.value)}
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

function renderDigestPreviewGroup(
  group: EngineeringConfigSourceDigestGroup,
  options?: {
    sourceId?: string | null;
    sourceContext?: EngineeringConfigSourceContext | null;
    creatingDraftGroupKey?: string | null;
    createdDraftGroupKey?: string | null;
    selectedTrimIds?: string[];
    focusedReviewRowKey?: string;
    groupRenderKey?: string;
    exportingGroupKey?: string | null;
    trimIdentityDrafts?: SourceDigestTrimIdentityDraftMap;
    onCreateDraft?: (group: EngineeringConfigSourceDigestGroup, selectedTrimIds?: string[]) => void;
    onCopyPreview?: (group: EngineeringConfigSourceDigestGroup, selectedTrimIds?: string[]) => void;
    onExportPreview?: (
      group: EngineeringConfigSourceDigestGroup,
      selectedTrimIds: string[] | undefined,
      format: SourceDigestExportFormat,
    ) => void;
    onToggleTrim?: (group: EngineeringConfigSourceDigestGroup, trimId: string) => void;
    onIdentityDraftChange?: (group: EngineeringConfigSourceDigestGroup, trim: EngineeringConfigSourceDigestGroup["trims"][number], field: SourceDigestTrimIdentityFieldKey, value: string) => void;
    onReviewRowFocus?: (group: EngineeringConfigSourceDigestGroup, row: EngineeringConfigSourceDigestRow, rowIndex: number) => void;
  },
) {
  const rows = group.rows.slice(0, SOURCE_DIGEST_PREVIEW_ROW_LIMIT);
  const remainingRows = group.rows.slice(SOURCE_DIGEST_PREVIEW_ROW_LIMIT);
  const selectedTrimIds = normaliseSourceDigestTrimSelection(group, options?.selectedTrimIds);
  const trimIndexById = new Map(group.trims.map((trim, index) => [sourceDigestTrimId(trim), index]));
  const selectedTrimIndexes = selectedTrimIds
    .map((trimId) => trimIndexById.get(trimId))
    .filter((index): index is number => typeof index === "number");
  const visibleTrimIndexes = selectedTrimIndexes.slice(0, 4);
  const visibleTrims = visibleTrimIndexes.map((index) => group.trims[index]);
  const actionKey = digestDraftActionKey(options?.sourceId, group.groupId);
  const creatingDraft = options?.creatingDraftGroupKey === actionKey;
  const createdDraft = options?.createdDraftGroupKey === actionKey;
  const exportingXlsx = options?.exportingGroupKey === sourceDigestExportActionKey(options?.sourceId, group.groupId, "xlsx");
  const exportingPdf = options?.exportingGroupKey === sourceDigestExportActionKey(options?.sourceId, group.groupId, "pdf");
  const showTrimPicker = Boolean(options?.onToggleTrim) && group.trims.length > 2;
  const temporaryOcrIdentityMeta = sourceDigestTemporaryOcrIdentityMeta(group);
  const identityReady = sourceDigestTemporaryIdentityReady(options?.sourceId, group, selectedTrimIds, options?.sourceContext, options?.trimIdentityDrafts ?? {});

  return (
    <div className="config-source-digest-group" key={options?.groupRenderKey ?? group.groupId}>
      <div className="config-source-digest-group__header">
        <div>
          <strong>{group.title}</strong>
          <span>{group.trimCount} 配置列 / option · {group.featureCount} 参数 · {group.differenceCount} 差异</span>
          {temporaryOcrIdentityMeta ? (
            <small className="product-config-source-digest-card__identity-warning">
              {temporaryOcrIdentityMeta}
            </small>
          ) : null}
        </div>
        {options?.onCreateDraft ? (
          <div className="config-source-digest-group__actions">
            {options?.onCopyPreview ? (
              <button
                className="btn btn-sm btn-secondary"
                type="button"
                disabled={selectedTrimIds.length < 2 || creatingDraft || exportingXlsx || exportingPdf}
                onClick={() => options.onCopyPreview?.(group, selectedTrimIds)}
              >
                复制当前配置表
              </button>
            ) : null}
            {options?.onExportPreview ? (
              <>
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  disabled={selectedTrimIds.length < 2 || creatingDraft || exportingXlsx || exportingPdf}
                  onClick={() => options.onExportPreview?.(group, selectedTrimIds, "xlsx")}
                >
                  {exportingXlsx ? "导出中" : "导出 XLSX"}
                </button>
                <button
                  className="btn btn-sm btn-secondary"
                  type="button"
                  disabled={selectedTrimIds.length < 2 || creatingDraft || exportingXlsx || exportingPdf}
                  onClick={() => options.onExportPreview?.(group, selectedTrimIds, "pdf")}
                >
                  {exportingPdf ? "导出中" : "导出 PDF"}
                </button>
              </>
            ) : null}
            <button
              className="btn btn-sm btn-primary"
              type="button"
              disabled={creatingDraft || selectedTrimIds.length < 2 || !identityReady}
              onClick={() => options.onCreateDraft?.(group, selectedTrimIds)}
            >
              {creatingDraft ? "创建中" : createdDraft ? "已创建" : "创建可编辑配置列"}
            </button>
          </div>
        ) : null}
      </div>
      <div className="config-source-digest-trims">
        {visibleTrims.map((trim, trimIndex) => (
          <span key={`${trim.trimId || sourceDigestTrimLabel(trim)}-${trimIndex}`}>{compactTrimName(sourceDigestTrimLabel(trim))}</span>
        ))}
        {selectedTrimIds.length > visibleTrims.length ? <span>+{selectedTrimIds.length - visibleTrims.length}</span> : null}
      </div>
      {showTrimPicker ? (
        <div className="config-source-digest-trim-picker" aria-label={`${group.modelName || group.title} 可创建配置列选择`}>
          <span>选择 2-4 个配置列</span>
          <small>已选 {selectedTrimIds.length}/4；取消一个后可换入其他 trim。</small>
          <div>
            {group.trims.map((trim) => {
              const trimId = sourceDigestTrimId(trim);
              const checked = selectedTrimIds.includes(trimId);
              const disabled = creatingDraft || (checked ? selectedTrimIds.length <= 2 : selectedTrimIds.length >= 4);
              return (
                <label className={`config-source-digest-trim-picker__item ${checked ? "is-selected" : ""}`} key={trimId}>
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={disabled}
                    onChange={() => options?.onToggleTrim?.(group, trimId)}
                  />
                  <span>{compactTrimName(sourceDigestTrimLabel(trim))}</span>
                  <small>{sourceDigestTrimAnchorMeta(trim)}</small>
                </label>
              );
            })}
          </div>
        </div>
      ) : null}
      {options?.onIdentityDraftChange
        ? renderSourceDigestTemporaryIdentityEditor(
          options.sourceId,
          group,
          selectedTrimIds,
          options.sourceContext,
          options.trimIdentityDrafts,
          options.onIdentityDraftChange,
        )
        : null}
      <div className="config-source-digest-table">
        <div className="config-source-digest-table__row config-source-digest-table__row--head">
          <span>参数</span>
          {visibleTrims.map((trim, trimIndex) => (
            <span key={`${trim.trimId || sourceDigestTrimLabel(trim)}-${trimIndex}`}>{compactTrimName(sourceDigestTrimLabel(trim))}</span>
          ))}
        </div>
        {renderDigestPreviewTableRows(group, rows, visibleTrimIndexes, {
          focusedReviewRowKey: options?.focusedReviewRowKey,
          onReviewRowFocus: options?.onReviewRowFocus ? (row, rowIndex) => options.onReviewRowFocus?.(group, row, rowIndex) : undefined,
        })}
      </div>
      {remainingRows.length > 0 ? (
        <details className="config-source-digest-more-rows">
          <summary>展开剩余 {remainingRows.length} 项配置</summary>
          <div className="config-source-digest-table">
            <div className="config-source-digest-table__row config-source-digest-table__row--head">
              <span>参数</span>
              {visibleTrims.map((trim, trimIndex) => (
                <span key={`${trim.trimId || sourceDigestTrimLabel(trim)}-${trimIndex}`}>{compactTrimName(sourceDigestTrimLabel(trim))}</span>
              ))}
            </div>
            {renderDigestPreviewTableRows(group, remainingRows, visibleTrimIndexes, {
              focusedReviewRowKey: options?.focusedReviewRowKey,
              rowIndexOffset: SOURCE_DIGEST_PREVIEW_ROW_LIMIT,
              onReviewRowFocus: options?.onReviewRowFocus ? (row, rowIndex) => options.onReviewRowFocus?.(group, row, rowIndex) : undefined,
            })}
          </div>
        </details>
      ) : null}
    </div>
  );
}

function renderSourceDigest(
  digest: EngineeringConfigSourceDigest | null | undefined,
  options?: {
    sourceId?: string | null;
    sourceContext?: EngineeringConfigSourceContext | null;
    creatingDraftGroupKey?: string | null;
    createdDraftGroupKey?: string | null;
    exportingGroupKey?: string | null;
    trimSelections?: SourceDigestTrimSelectionMap;
    reviewFocuses?: SourceDigestReviewFocusMap;
    trimIdentityDrafts?: SourceDigestTrimIdentityDraftMap;
    onCreateDraft?: (group: EngineeringConfigSourceDigestGroup, selectedTrimIds?: string[]) => void;
    onCopyPreview?: (group: EngineeringConfigSourceDigestGroup, selectedTrimIds?: string[]) => void;
    onExportPreview?: (
      group: EngineeringConfigSourceDigestGroup,
      selectedTrimIds: string[] | undefined,
      format: SourceDigestExportFormat,
    ) => void;
    onToggleTrim?: (sourceId: string | null | undefined, group: EngineeringConfigSourceDigestGroup, trimId: string) => void;
    onIdentityDraftChange?: (
      sourceId: string | null | undefined,
      group: EngineeringConfigSourceDigestGroup,
      trim: EngineeringConfigSourceDigestGroup["trims"][number],
      field: SourceDigestTrimIdentityFieldKey,
      value: string,
    ) => void;
	    onReviewRowFocus?: (
	      sourceId: string | null | undefined,
	      group: EngineeringConfigSourceDigestGroup,
	      row: EngineeringConfigSourceDigestRow,
	      rowIndex: number,
	    ) => void;
  },
) {
  if (!digest) return null;
  const ocrSummaryParts = sourceDigestOcrSummaryParts(digest);
  const ocrHint = sourceDigestOcrHint(digest);
  if (digest.status === "failed") {
    return (
      <div className="config-source-digest">
        <span className="market-scan-panel-eyebrow">Source Digest</span>
        <small className="market-scan-field-hint">Digest 失败：{digest.errorMessage || "无法读取来源结构"}</small>
      </div>
    );
  }
  if (digest.status === "pending") {
    return (
      <div className="config-source-digest">
        <div className="config-source-digest__header">
          <span className="market-scan-panel-eyebrow">Source Digest</span>
          <strong>{digest.modelName || digest.fileName}</strong>
        </div>
        <div className="summary-chips">
          <span className="chip chip-warning">{sourceDigestTypeLabel(digest)}</span>
          <span className="chip chip-neutral">{sourceDigestStatusLabel(digest)}</span>
          {ocrSummaryParts.map((part) => (
            <span className="chip chip-info" key={part}>{part}</span>
          ))}
        </div>
        {renderSourceDigestOcrCandidateComparison(digest)}
        {ocrHint ? <small className="market-scan-field-hint">{ocrHint}</small> : null}
        <small className="market-scan-field-hint">{digest.message || "Source Digest 已进入来源库，但暂未生成可比配置列。"}</small>
      </div>
    );
  }
  const comparableGroups = digest.compareGroups.filter((group) => group.trimCount >= 2);
  return (
    <div className="config-source-digest">
      <div className="config-source-digest__header">
        <span className="market-scan-panel-eyebrow">Source Digest</span>
        <strong>{digest.modelName || digest.fileName}</strong>
      </div>
      <div className="summary-chips">
        <span className="chip chip-positive">{sourceDigestTypeLabel(digest)}</span>
        <span className="chip chip-info">{sourceDigestStatusLabel(digest)}</span>
        {sourceDigestSummaryParts(digest).map((part) => (
          <span className="chip chip-info" key={part}>{part}</span>
        ))}
        {ocrSummaryParts.map((part) => (
          <span className="chip chip-neutral" key={part}>{part}</span>
        ))}
      </div>
      {renderSourceDigestOcrCandidateComparison(digest)}
      {ocrHint ? <small className="market-scan-field-hint">{ocrHint}</small> : null}
      {sourceDigestHasPriceList(digest) ? (
        <small className="market-scan-field-hint">
          价格单已识别到可对比的车型 / 版型和价格字段，可以先创建可编辑配置列，再加入 FloatingDeck 对比。
        </small>
      ) : null}
      <small className="market-scan-field-hint">{SOURCE_DIGEST_AI_BOUNDARY_HINT}</small>
      {comparableGroups.length > 0 ? (
        comparableGroups.map((group, groupIndex) => {
          const actionKey = digestDraftActionKey(options?.sourceId, group.groupId);
          return renderDigestPreviewGroup(group, {
            ...options,
            groupRenderKey: `${actionKey}::${groupIndex}`,
            selectedTrimIds: options?.trimSelections?.[actionKey],
            focusedReviewRowKey: options?.reviewFocuses?.[actionKey],
            onToggleTrim: options?.onToggleTrim
              ? (_group, trimId) => options.onToggleTrim?.(options.sourceId, group, trimId)
              : undefined,
            onIdentityDraftChange: options?.onIdentityDraftChange
              ? (_group, trim, field, value) => options.onIdentityDraftChange?.(options.sourceId, group, trim, field, value)
              : undefined,
            onReviewRowFocus: options?.onReviewRowFocus
              ? (_group, row, rowIndex) => options.onReviewRowFocus?.(options.sourceId, group, row, rowIndex)
              : undefined,
          });
        })
      ) : (
        <small className="market-scan-field-hint">
          {digest.message || "已生成结构摘要，但暂未识别到 2 个以上可横向对比的配置列 / option。"}
        </small>
      )}
    </div>
  );
}

interface EngineeringConfigSourceUploadPanelProps {
  compact?: boolean;
  diagnosticsMode?: "inline" | "collapsed";
  libraryScopeMode?: "inline" | "collapsed";
  onDraftCreated?: (
    result: EngineeringConfigDigestDraftResult,
    group: EngineeringConfigSourceDigestGroup,
    selectedTrimIds: string[],
    reviewRow?: EngineeringConfigSourceDigestRow,
    sourceSnapshot?: EngineeringConfigSourceSnapshot | null,
  ) => void;
  onRequestContextBinding?: () => void;
  onRequestSourceSearch?: (query: string) => void;
  onUploaded?: (sourceSnapshot?: EngineeringConfigSourceSnapshot) => void;
  relatedContext?: EngineeringConfigSourceContext;
  showSourceLibrary?: boolean;
  sourceDigestDescription?: string;
  sourceDigestTitle?: string;
}

export function EngineeringConfigSourceUploadPanel({
  compact = false,
  diagnosticsMode = "inline",
  libraryScopeMode = "inline",
  onDraftCreated,
  onRequestContextBinding,
  onRequestSourceSearch,
  onUploaded,
  relatedContext,
  showSourceLibrary = true,
  sourceDigestDescription = "Excel / PDF / 网页 / 图片 / CSV 可识别表格后，先转成可编辑配置列。",
  sourceDigestTitle = "Source Digest（推荐）",
}: EngineeringConfigSourceUploadPanelProps) {
  const inputId = useId();
  const [stage, setStage] = useState<UploadStage>("select");
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [uploadId, setUploadId] = useState<string | null>(null);
  const [progress, setProgress] = useState({ pct: 0, detail: "" });
  const [preview, setPreview] = useState<ParsePreview | null>(null);
  const [fullPreview, setFullPreview] = useState<Record<string, unknown> | null>(null);
  const [sourceReceipt, setSourceReceipt] = useState<EngineeringConfigSourceSnapshot | null>(null);
  const [sourceSnapshots, setSourceSnapshots] = useState<EngineeringConfigSourceSnapshot[]>([]);
  const [sourceSnapshotTotalRows, setSourceSnapshotTotalRows] = useState(0);
  const [sourceSnapshotsLoading, setSourceSnapshotsLoading] = useState(false);
  const [sourceSnapshotsError, setSourceSnapshotsError] = useState<string | null>(null);
  const [sourceLibraryScope, setSourceLibraryScope] = useState<SourceLibraryScope>("country");
  const [sourceLibraryTypeFilter, setSourceLibraryTypeFilter] = useState<SourceLibraryTypeFilterKey>("all");
  const [sourceLibrarySearchValue, setSourceLibrarySearchValue] = useState("");
  const [sourceLibrarySearchQuery, setSourceLibrarySearchQuery] = useState("");
  const [sourceLibrarySearchResetKey, setSourceLibrarySearchResetKey] = useState(0);
  const [sourceLibraryFeedback, setSourceLibraryFeedback] = useState<string | null>(null);
  const [sourceLibraryActionId, setSourceLibraryActionId] = useState<string | null>(null);
  const [expandedSourceId, setExpandedSourceId] = useState<string | null>(null);
  const [sourceSnapshotDetails, setSourceSnapshotDetails] = useState<Record<string, EngineeringConfigSourceSnapshot>>({});
  const [sourceDigestLoadingId, setSourceDigestLoadingId] = useState<string | null>(null);
  const [sourceDigestErrors, setSourceDigestErrors] = useState<Record<string, string>>({});
  const [creatingDraftGroupKey, setCreatingDraftGroupKey] = useState<string | null>(null);
  const [exportingSourceDigestGroupKey, setExportingSourceDigestGroupKey] = useState<string | null>(null);
  const [sourceDigestTrimSelections, setSourceDigestTrimSelections] = useState<SourceDigestTrimSelectionMap>({});
  const [sourceDigestReviewFocuses, setSourceDigestReviewFocuses] = useState<SourceDigestReviewFocusMap>({});
  const [sourceDigestTrimIdentityDrafts, setSourceDigestTrimIdentityDrafts] = useState<SourceDigestTrimIdentityDraftMap>({});
  const [draftResult, setDraftResult] = useState<EngineeringConfigDigestDraftResult | null>(null);
  const [draftError, setDraftError] = useState<string | null>(null);
  const [sourceDigestCopyFeedback, setSourceDigestCopyFeedback] = useState<string | null>(null);
  const [requestedFloatingDeckSearchQuery, setRequestedFloatingDeckSearchQuery] = useState<string | null>(null);
  const [ocrReadiness, setOcrReadiness] = useState<EngineeringConfigOcrReadiness | null>(null);
  const [ocrReadinessError, setOcrReadinessError] = useState<string | null>(null);
  const [aiSummaryReadiness, setAiSummaryReadiness] = useState<EngineeringConfigBusinessSummaryReadiness | null>(null);
  const [aiSummaryReadinessError, setAiSummaryReadinessError] = useState<string | null>(null);
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(diagnosticsMode === "inline");
  const [libraryScopeOpen, setLibraryScopeOpen] = useState(libraryScopeMode === "inline");
  const [error, setError] = useState<string | null>(null);
  const diagnosticsVisible = diagnosticsMode === "inline" || diagnosticsOpen;
  const libraryScopeVisible = libraryScopeMode === "inline" || libraryScopeOpen;
  const uploadKind = classifyUpload(file);
  const hasRelatedContext = hasSourceContextSignal(relatedContext);
  const sourceLibraryCountry = relatedContext?.country || relatedContext?.market || null;
  const sourceLibrarySegment = relatedContext?.segment || null;
  const effectiveSourceLibraryScope: SourceLibraryScope = sourceLibraryScope === "country" && !sourceLibraryCountry ? "all" : sourceLibraryScope;
  const sourceTrashRequiresCountry = effectiveSourceLibraryScope === "trash" && !sourceLibraryCountry;
  const sourceLibraryCountryFilter = effectiveSourceLibraryScope === "country" || effectiveSourceLibraryScope === "trash"
    ? sourceLibraryCountry
    : null;
  const sourceLibrarySegmentFilter = effectiveSourceLibraryScope === "country" ? sourceLibrarySegment : null;

  useEffect(() => {
    if (diagnosticsMode === "collapsed") setDiagnosticsOpen(false);
  }, [diagnosticsMode]);

  async function loadSourceSnapshots(): Promise<void> {
    if (!showSourceLibrary) {
      setSourceSnapshots([]);
      setSourceSnapshotTotalRows(0);
      setSourceSnapshotsError(null);
      setSourceSnapshotsLoading(false);
      return;
    }
    if (sourceTrashRequiresCountry) {
      setSourceSnapshots([]);
      setSourceSnapshotTotalRows(0);
      setSourceSnapshotsError(null);
      setSourceSnapshotsLoading(false);
      return;
    }
    setSourceSnapshotsLoading(true);
    setSourceSnapshotsError(null);
    try {
      const sourceQuery = sourceLibrarySearchQuery.trim();
      const result = await api.listEngineeringConfigSourceSnapshots({
        limit: sourceQuery ? 25 : compact ? 6 : 10,
        country: sourceLibraryCountryFilter,
        segment: sourceLibrarySegmentFilter,
        ...(sourceQuery ? { q: sourceQuery } : {}),
        trashOnly: effectiveSourceLibraryScope === "trash",
      });
      setSourceSnapshots(result.items);
      setSourceSnapshotTotalRows(result.rows);
    } catch (err) {
      setSourceSnapshotsError(err instanceof Error ? err.message : "来源列表加载失败");
    } finally {
      setSourceSnapshotsLoading(false);
    }
  }

  useEffect(() => {
    void loadSourceSnapshots();
  }, [effectiveSourceLibraryScope, showSourceLibrary, sourceLibraryCountryFilter, sourceLibrarySegmentFilter, sourceLibrarySearchQuery, sourceTrashRequiresCountry]);

  useEffect(() => {
    if (!diagnosticsVisible) return undefined;
    let active = true;
    api.getEngineeringConfigOcrReadiness()
      .then((result) => {
        if (!active) return;
        setOcrReadiness(result);
        setOcrReadinessError(null);
      })
      .catch((err: unknown) => {
        if (!active) return;
        setOcrReadiness(null);
        setOcrReadinessError(err instanceof Error ? err.message : "OCR readiness 加载失败");
      });
    return () => {
      active = false;
    };
  }, [diagnosticsVisible]);

  useEffect(() => {
    if (!diagnosticsVisible) return undefined;
    let active = true;
    api.getEngineeringConfigBusinessSummaryReadiness()
      .then((result) => {
        if (!active) return;
        setAiSummaryReadiness(result);
        setAiSummaryReadinessError(null);
      })
      .catch((err: unknown) => {
        if (!active) return;
        setAiSummaryReadiness(null);
        setAiSummaryReadinessError(err instanceof Error ? err.message : "AI 摘要状态加载失败");
      });
    return () => {
      active = false;
    };
  }, [diagnosticsVisible]);

  const selectedSourceLibrarySnapshot = sourceLibrarySearchValue
    ? sourceSnapshots.find((item) => item.sourceId === sourceLibrarySearchValue) ?? sourceSnapshotDetails[sourceLibrarySearchValue] ?? null
    : null;
  const sourceSnapshotsForDisplay = selectedSourceLibrarySnapshot
    ? [
      selectedSourceLibrarySnapshot,
      ...sourceSnapshots.filter((snapshot) => snapshot.sourceId !== selectedSourceLibrarySnapshot.sourceId),
    ]
    : sourceSnapshots;
  const sourceLibraryTypeFilterItemsList = sourceLibraryTypeFilterItems(sourceSnapshotsForDisplay);
  const sourceSnapshotsByType = sourceSnapshotsForDisplay.filter((snapshot) => (
    sourceSnapshotMatchesTypeFilter(snapshot, sourceLibraryTypeFilter)
  ));
  const sourceLibrarySearchOptions = sourceSnapshotsByType.map(sourceSnapshotSearchOption);

  function selectSourceLibrarySnapshot(sourceId: string): void {
    setSourceLibrarySearchValue(sourceId);
    if (!sourceId) return;
    const snapshot = sourceSnapshots.find((item) => item.sourceId === sourceId);
    if (snapshot) {
      setSourceSnapshotDetails((current) => ({ ...current, [sourceId]: snapshot }));
      void toggleSourceSnapshotDigest(snapshot);
    }
  }

  function clearSourceLibrarySearch(): void {
    setSourceLibrarySearchValue("");
    setSourceLibrarySearchQuery("");
    setSourceLibrarySearchResetKey((current) => current + 1);
  }

  function reset(nextFile: File | null = null): void {
    setFile(nextFile);
    setUploadId(null);
    setPreview(null);
    setFullPreview(null);
    setSourceReceipt(null);
    setDraftResult(null);
    setDraftError(null);
    setSourceDigestCopyFeedback(null);
    setRequestedFloatingDeckSearchQuery(null);
    setCreatingDraftGroupKey(null);
    setExportingSourceDigestGroupKey(null);
    setSourceDigestTrimSelections({});
    setSourceDigestReviewFocuses({});
    setStage("select");
    setProgress({ pct: 0, detail: "" });
    setError(null);
  }

  function acceptFile(nextFile: File | undefined): void {
    if (!nextFile) return;
    if (classifyUpload(nextFile) === "unsupported") {
      reset();
      setError(`仅支持 ${SOURCE_EXTENSION_LABEL} 来源文件`);
      return;
    }
    reset(nextFile);
  }

  function handleDrag(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
  }

  function handleDragIn(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    if (event.dataTransfer.items?.length) setDragActive(true);
  }

  function handleDragOut(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    setDragActive(false);
  }

  function handleDrop(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    setDragActive(false);
    acceptFile(event.dataTransfer.files?.[0]);
  }

  function handleFileChange(event: ChangeEvent<HTMLInputElement>): void {
    acceptFile(event.target.files?.[0]);
    event.target.value = "";
  }

  async function uploadChunks(
    activeFile: File,
    activeUploadId: string,
    totalChunks: number,
    uploader: (uploadId: string, partNumber: number, chunk: Blob) => Promise<unknown>,
  ): Promise<void> {
    for (let index = 0; index < totalChunks; index += 1) {
      const start = index * CHUNK_SIZE;
      const end = Math.min(activeFile.size, start + CHUNK_SIZE);
      setProgress({
        pct: Math.round((index / totalChunks) * 66),
        detail: `分片 ${index + 1}/${totalChunks}`,
      });
      await uploader(activeUploadId, index, activeFile.slice(start, end));
    }
  }

  async function uploadMatrix(activeFile: File): Promise<void> {
    setStage("uploading");
    setProgress({ pct: 0, detail: "初始化 Excel 配置矩阵上传..." });
    const init = await api.initiateEngineeringConfigUpload(activeFile.name, activeFile.size, CHUNK_SIZE);
    const activeUploadId = init.uploadId;
    setUploadId(activeUploadId);
    await uploadChunks(activeFile, activeUploadId, init.totalChunks, api.uploadEngineeringConfigChunk);
    setStage("assembling");
    setProgress({ pct: 70, detail: "组装配置矩阵..." });
    await api.completeEngineeringConfigUpload(activeUploadId);
    setStage("parsing");
    setProgress({ pct: 82, detail: "解析配置大类、配置项和配置列..." });
    const parsed = await api.parseEngineeringConfigUpload(activeUploadId) as unknown as ParsePreview;
    setPreview(parsed);
    setStage("matching");
    setProgress({ pct: 92, detail: "匹配已有版本和 Draft 冲突..." });
    await api.matchEngineeringConfigUpload(activeUploadId);
    const nextPreview = await api.getEngineeringConfigUploadPreview(activeUploadId);
    setFullPreview(nextPreview);
    setStage("preview");
    setProgress({ pct: 100, detail: "预览就绪" });
  }

  async function uploadSource(activeFile: File): Promise<void> {
    setStage("uploading");
    setProgress({ pct: 0, detail: "初始化来源文件上传..." });
    const init = await api.initiateEngineeringConfigSourceUpload(
      activeFile.name,
      activeFile.size,
      CHUNK_SIZE,
      activeFile.type || undefined,
    );
    const activeUploadId = init.uploadId;
    setUploadId(activeUploadId);
    await uploadChunks(activeFile, activeUploadId, init.totalChunks, api.uploadEngineeringConfigSourceChunk);
    setStage("storing");
    setProgress({ pct: 88, detail: "生成 Source Digest..." });
    const registrationContext = sourceRegistrationContext(relatedContext);
    const receipt = await api.completeEngineeringConfigSourceUpload(
      activeUploadId,
      hasRelatedContext ? registrationContext : undefined,
    );
    setSourceReceipt(receipt);
    setRequestedFloatingDeckSearchQuery(null);
    setStage("stored");
    setProgress({ pct: 100, detail: receipt.uploadStatus === "duplicate" ? "Source Digest 已存在" : "Source Digest 已入库" });
    onUploaded?.(receipt);
    await loadSourceSnapshots();
  }

  async function startUpload(mode?: Exclude<UploadKind, "unsupported">): Promise<void> {
    if (!file) return;
    setError(null);
    try {
      const effectiveMode = mode ?? (uploadKind === "matrix" ? "matrix" : "source");
      if (effectiveMode === "matrix") await uploadMatrix(file);
      else if (effectiveMode === "source") await uploadSource(file);
      else setError("文件类型不支持");
    } catch (err) {
      setError(err instanceof Error ? err.message : "上传失败");
      setStage("select");
    }
  }

  async function confirmMatrixImport(): Promise<void> {
    if (!uploadId) return;
    setStage("confirming");
    setError(null);
    try {
      await api.confirmEngineeringConfigUpload(uploadId);
      setStage("done");
      onUploaded?.();
    } catch (err) {
      setError(err instanceof Error ? err.message : "确认导入失败");
      setStage("preview");
    }
  }

  async function createDraftFromDigestGroup(
    sourceId: string | null | undefined,
    group: EngineeringConfigSourceDigestGroup,
    selectedTrimIds?: string[],
  ): Promise<void> {
    if (!sourceId) return;
    const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
    const actionKey = digestDraftActionKey(sourceId, group.groupId);
    const focusedReviewRowKey = sourceDigestReviewFocuses[actionKey];
    const focusedReviewRow = focusedReviewRowKey
      ? group.rows.find((row, rowIndex) => (
        row.reviewNotes?.length && sourceDigestReviewRowKey(row, rowIndex) === focusedReviewRowKey
      ))
      : null;
    const fallbackReviewRow = group.rows.find((row) => row.reviewNotes?.length) ?? null;
    const sourceSnapshot = sourceReceipt?.sourceId === sourceId
      ? sourceReceipt
      : sourceSnapshotDetails[sourceId] ?? sourceSnapshots.find((snapshot) => snapshot.sourceId === sourceId) ?? null;
    const sourceContext = sourceSnapshot?.relatedContext ?? relatedContext;
    if (!sourceDigestTemporaryIdentityReady(sourceId, group, selected, sourceContext, sourceDigestTrimIdentityDrafts)) {
      setDraftError("请先补全 OCR 临时列的真实车型 / 配置列身份，再创建可编辑配置列。");
      return;
    }
    setCreatingDraftGroupKey(actionKey);
    setDraftError(null);
    try {
      const trimPayload = sourceDigestSelectedTrimPayload(group, selected);
      const trimIdentityOverrides = sourceDigestTrimIdentityOverridePayload(sourceId, group, selected, sourceContext, sourceDigestTrimIdentityDrafts);
      const draftOptions: {
        trimIds?: string[];
        trimIdentityOverrides?: EngineeringConfigDigestTrimIdentityOverride[];
      } = {
        ...(trimPayload ?? {}),
        ...(trimIdentityOverrides.length > 0 ? { trimIdentityOverrides } : {}),
      };
      const result = Object.keys(draftOptions).length > 0
        ? await api.createEngineeringConfigDraftFromSourceDigest(sourceId, group.groupId, draftOptions)
        : await api.createEngineeringConfigDraftFromSourceDigest(sourceId, group.groupId);
      setDraftResult(result);
      removeSourceDigestTrimIdentityDraft(sourceId, group.groupId);
      await loadSourceSnapshots();
      onUploaded?.();
      onDraftCreated?.(result, group, selected, focusedReviewRow ?? fallbackReviewRow ?? undefined, sourceSnapshot);
    } catch (err) {
      setDraftError(err instanceof Error ? err.message : "创建可编辑配置列失败");
    } finally {
      setCreatingDraftGroupKey(null);
    }
  }

  function focusSourceDigestReviewRow(
    sourceId: string | null | undefined,
    group: EngineeringConfigSourceDigestGroup,
    row: EngineeringConfigSourceDigestRow,
    rowIndex: number,
  ): void {
    setSourceDigestReviewFocuses((current) => ({
      ...current,
      [digestDraftActionKey(sourceId, group.groupId)]: sourceDigestReviewRowKey(row, rowIndex),
    }));
  }

  function updateSourceDigestTrimIdentityDraft(
    sourceId: string | null | undefined,
    group: EngineeringConfigSourceDigestGroup,
    trim: EngineeringConfigSourceDigestGroup["trims"][number],
    field: SourceDigestTrimIdentityFieldKey,
    value: string,
  ): void {
    const actionKey = digestDraftActionKey(sourceId, group.groupId);
    const trimId = sourceDigestTrimId(trim);
    const receiptContext = sourceReceipt && sourceReceipt.sourceId === sourceId
      ? sourceReceipt.relatedContext ?? relatedContext
      : null;
    const sourceContext = receiptContext
      ? receiptContext
      : sourceSnapshotDetails[sourceId ?? ""]?.relatedContext ?? sourceSnapshots.find((snapshot) => snapshot.sourceId === sourceId)?.relatedContext ?? relatedContext;
    setSourceDigestTrimIdentityDrafts((current) => {
      const currentGroupDrafts = current[actionKey] ?? {};
      const currentTrimDraft = currentGroupDrafts[trimId] ?? sourceDigestTrimIdentityDefaults(group, trim, sourceContext);
      return {
        ...current,
        [actionKey]: {
          ...currentGroupDrafts,
          [trimId]: {
            ...currentTrimDraft,
            trimId,
            [field]: value,
          },
        },
      };
    });
  }

  function removeSourceDigestTrimIdentityDraft(sourceId: string | null | undefined, groupId: string, trimId?: string): void {
    const actionKey = digestDraftActionKey(sourceId, groupId);
    setSourceDigestTrimIdentityDrafts((current) => {
      const next = { ...current };
      if (!trimId) {
        delete next[actionKey];
        return next;
      }
      const groupDrafts = { ...(next[actionKey] ?? {}) };
      delete groupDrafts[trimId];
      if (Object.keys(groupDrafts).length > 0) {
        next[actionKey] = groupDrafts;
      } else {
        delete next[actionKey];
      }
      return next;
    });
  }

  async function copySourceDigestPreviewGroup(
    group: EngineeringConfigSourceDigestGroup,
    selectedTrimIds?: string[],
  ): Promise<void> {
    if (!navigator.clipboard?.writeText) {
      setSourceDigestCopyFeedback("当前浏览器不支持复制当前配置表。");
      return;
    }
    const selected = normaliseSourceDigestTrimSelection(group, selectedTrimIds);
    try {
      await navigator.clipboard.writeText(sourceDigestGroupTsv(group, selected));
      setSourceDigestCopyFeedback(`已复制 ${group.modelName || group.title}：${group.rows.length} 配置项 · ${selected.length} 配置列。`);
    } catch (err) {
      setSourceDigestCopyFeedback(err instanceof Error ? err.message : "复制当前配置表失败");
    }
  }

  function toggleSourceDigestTrim(
    sourceId: string | null | undefined,
    group: EngineeringConfigSourceDigestGroup,
    trimId: string,
  ): void {
    const actionKey = digestDraftActionKey(sourceId, group.groupId);
    const selected = normaliseSourceDigestTrimSelection(group, sourceDigestTrimSelections[actionKey]);
    const checked = selected.includes(trimId);
    if (checked && selected.length > 2) removeSourceDigestTrimIdentityDraft(sourceId, group.groupId, trimId);
    setSourceDigestTrimSelections((current) => {
      const selected = normaliseSourceDigestTrimSelection(group, current[actionKey]);
      const checked = selected.includes(trimId);
      if (checked && selected.length <= 2) return current;
      if (!checked && selected.length >= 4) return current;
      const nextSelected = checked ? selected.filter((id) => id !== trimId) : [...selected, trimId];
      return { ...current, [actionKey]: nextSelected };
    });
  }

  async function toggleSourceSnapshotDigest(snapshot: EngineeringConfigSourceSnapshot): Promise<void> {
    if (expandedSourceId === snapshot.sourceId) {
      setExpandedSourceId(null);
      return;
    }
    setExpandedSourceId(snapshot.sourceId);
    setSourceDigestErrors((current) => {
      const next = { ...current };
      delete next[snapshot.sourceId];
      return next;
    });
    if (snapshot.sourceDigest || sourceSnapshotDetails[snapshot.sourceId]?.sourceDigest) return;
    setSourceDigestLoadingId(snapshot.sourceId);
    try {
      const detail = await api.getEngineeringConfigSourceSnapshot(snapshot.sourceId);
      setSourceSnapshotDetails((current) => ({
        ...current,
        [snapshot.sourceId]: detail,
      }));
    } catch (err) {
      setSourceDigestErrors((current) => ({
        ...current,
        [snapshot.sourceId]: err instanceof Error ? err.message : "Digest 详情加载失败",
      }));
    } finally {
      setSourceDigestLoadingId(null);
    }
  }

  async function moveSourceSnapshotToTrash(sourceId: string): Promise<void> {
    if (effectiveSourceLibraryScope !== "country" || !sourceLibraryCountry) {
      setSourceLibraryFeedback("请先切到当前国家来源库，再移入该国家垃圾桶。");
      return;
    }
    setSourceLibraryActionId(sourceId);
    setSourceLibraryFeedback(null);
    try {
      await api.trashEngineeringConfigSourceSnapshot(sourceId, sourceLibraryCountry);
      setSourceLibraryFeedback("已移入当前国家来源垃圾桶。");
      await loadSourceSnapshots();
      onUploaded?.();
    } catch (err) {
      setSourceSnapshotsError(err instanceof Error ? err.message : "移入垃圾桶失败");
    } finally {
      setSourceLibraryActionId(null);
    }
  }

  async function restoreSourceSnapshot(sourceId: string): Promise<void> {
    if (!sourceLibraryCountry) {
      setSourceLibraryFeedback("请先选择或关联国家，再恢复该国家的来源。");
      return;
    }
    setSourceLibraryActionId(sourceId);
    setSourceLibraryFeedback(null);
    try {
      await api.restoreEngineeringConfigSourceSnapshot(sourceId, sourceLibraryCountry);
      setSourceLibraryFeedback("已从来源垃圾桶恢复。");
      await loadSourceSnapshots();
      onUploaded?.();
    } catch (err) {
      setSourceSnapshotsError(err instanceof Error ? err.message : "恢复来源失败");
    } finally {
      setSourceLibraryActionId(null);
    }
  }

  async function clearSourceTrash(): Promise<void> {
    if (!sourceLibraryCountry) {
      setSourceLibraryFeedback("请先选择或关联国家，再清空该国家的来源垃圾桶。");
      return;
    }
    setSourceLibraryActionId("__clear_trash__");
    setSourceLibraryFeedback(null);
    try {
      const result = await api.clearEngineeringConfigSourceTrash(sourceLibraryCountry);
      setSourceLibraryFeedback(`已清空 ${result.cleared} 个垃圾桶来源。`);
      await loadSourceSnapshots();
      onUploaded?.();
    } catch (err) {
      setSourceSnapshotsError(err instanceof Error ? err.message : "清空垃圾桶失败");
    } finally {
      setSourceLibraryActionId(null);
    }
  }

  const summary = asRecord(fullPreview?.summary);
  const receiptSourceType = sourceReceipt?.fileType || "";
  const receiptImportBatchId = sourceReceipt?.importBatchId || "";
  const receiptHash = sourceReceipt?.sourceFileHash || "";
  const recentSnapshots = sourceSnapshotsByType.slice(0, compact ? 4 : 8);
  const hiddenSourceSnapshotCount = sourceLibraryTypeFilter === "all"
    ? Math.max(sourceSnapshotTotalRows - recentSnapshots.length, 0)
    : Math.max(sourceSnapshotsByType.length - recentSnapshots.length, 0);
  const sourceLibraryTitle = effectiveSourceLibraryScope === "trash"
    ? `Source Trash · ${sourceLibraryCountry || "需要国家"}`
    : effectiveSourceLibraryScope === "country"
      ? `Country Source Library · ${[sourceLibraryCountry, sourceLibrarySegment].filter(Boolean).join(" · ")}`
      : "Source Library";
  const sourceLibraryHint = sourceLibraryScopeHint(
    effectiveSourceLibraryScope,
    sourceLibraryScope,
    sourceLibraryCountry,
    sourceLibrarySegment,
  );
  const uploadedSourceSearchQuery = sourceReceipt
    ? [
      sourceReceipt.sourceFileName,
      sourceReceipt.sourceDigest?.modelName,
      sourceReceipt.relatedContext?.model,
      sourceReceipt.relatedContext?.country || sourceReceipt.relatedContext?.market,
    ].map((value) => value?.trim()).find(Boolean) ?? sourceReceipt.sourceId
    : "";
  const uploadedSourceCanSearch = Boolean(
    onRequestSourceSearch
    && sourceReceipt
    && sourceReceipt.extractStatus === "digest_ready"
    && sourceReceipt.sourceDigest?.compareGroups.some((group) => group.trimCount >= 2),
  );
  const uploadedSourceSearchRequested = Boolean(
    uploadedSourceSearchQuery
    && requestedFloatingDeckSearchQuery === uploadedSourceSearchQuery,
  );

  function requestUploadedSourceSearch(): void {
    if (!uploadedSourceSearchQuery) return;
    setRequestedFloatingDeckSearchQuery(uploadedSourceSearchQuery);
    onRequestSourceSearch?.(uploadedSourceSearchQuery);
  }

  const diagnosticsContent = (
    <>
      {renderOcrReadiness(ocrReadiness, ocrReadinessError)}
      <Suspense fallback={<small className="market-scan-field-hint">加载 AI 摘要状态...</small>}>
        <LazyEngineeringConfigAiSummaryReadinessCard
          readiness={aiSummaryReadiness}
          error={aiSummaryReadinessError}
          variant="sourceUpload"
        />
      </Suspense>
    </>
  );

  function renderSourceLibrarySharing(): ReactElement {
    const title = sourceLibraryCountry ? "上传后归档到当前国家团队来源库" : "上传后进入团队共享来源库";
    const detail = sourceLibraryCountry
      ? "同国家 / segment 的用户后续可搜索复用；恢复和清空只处理当前国家垃圾桶。"
      : "未绑定国家时仍可生成 Source Digest，但建议先在 FloatingDeck 选择 Market / Country 便于后续筛选和清理。";
    const actionButton = !sourceLibraryCountry && onRequestContextBinding ? (
      <button className="btn btn-sm btn-secondary config-source-context-action" type="button" onClick={onRequestContextBinding}>
        去选择 Market / Country
      </button>
    ) : null;
    const sharingContent = (
      <section className="config-source-library-sharing" aria-label={libraryScopeMode === "inline" ? "来源库共享范围" : undefined}>
        <div className="config-source-library-sharing__intro">
          <span className="market-scan-panel-eyebrow">共享来源库</span>
          <strong>{title}</strong>
          <small>{detail}</small>
          {libraryScopeMode === "inline" ? actionButton : null}
        </div>
        <div className="config-source-library-sharing__items">
          {sourceUploadLibraryScopeItems(sourceLibraryCountry, sourceLibrarySegment).map((item) => (
            <div className="config-source-library-sharing__item" key={item.label}>
              <span>{item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>
      </section>
    );
    if (libraryScopeMode === "inline") return sharingContent;
    return (
      <div className="config-source-library-sharing-compact">
        <details
          className="config-source-library-sharing-disclosure"
          aria-label="来源库共享范围"
          open={libraryScopeOpen}
        >
          <summary
            onClick={(event) => {
              event.preventDefault();
              setLibraryScopeOpen((open) => !open);
            }}
          >
            <span>来源归档</span>
            <strong>{sourceLibraryCountry ? `${sourceLibraryCountry}${sourceLibrarySegment ? ` / ${sourceLibrarySegment}` : ""}` : "团队共享 · 国家待绑定"}</strong>
            <small>{sourceLibraryCountry ? "展开查看共享和垃圾桶规则" : "未绑定国家；可先上传，也可先绑定国家"}</small>
          </summary>
          {libraryScopeVisible ? sharingContent : null}
        </details>
        {actionButton}
      </div>
    );
  }

  return (
    <div className={`config-source-upload-panel${compact ? " config-source-upload-panel--compact" : ""}`}>
      <p className="config-source-upload-intro">
        <strong>{sourceDigestTitle}</strong>
        <span>{sourceDigestDescription}</span>
      </p>

      {hasRelatedContext && relatedContext ? (
        <div className="config-source-context-summary">
          <span>当前关联上下文</span>
          <strong>{sourceContextParts(relatedContext).join(" · ")}</strong>
          <small>{sourceContextTaskHint(relatedContext)}</small>
        </div>
      ) : null}

      {renderSourceLibrarySharing()}

      {diagnosticsMode === "collapsed" ? (
        <details
          className="config-source-upload-diagnostics"
          aria-label="上传诊断"
          open={diagnosticsOpen}
          onToggle={(event) => setDiagnosticsOpen(event.currentTarget.open)}
        >
          <summary>
            <span>上传诊断</span>
            <small>{diagnosticsOpen ? "OCR / AI runtime 状态已展开" : "展开后检查 OCR / AI runtime 状态"}</small>
          </summary>
          {diagnosticsVisible ? diagnosticsContent : null}
        </details>
      ) : diagnosticsContent}

      <label
        className={`dropzone config-source-dropzone ${dragActive ? "dropzone-active" : ""} ${file ? "dropzone-has-file" : ""}`}
        htmlFor={inputId}
        onDragEnter={handleDragIn}
        onDragLeave={handleDragOut}
        onDragOver={handleDrag}
        onDrop={handleDrop}
      >
        {file ? (
          <div>
            <span>{file.name}</span>
            <span>{formatFileSize(file.size)} · {selectedFileKindLabel(uploadKind)}</span>
          </div>
        ) : (
          <div>
            <span>拖放配置表或来源文件</span>
            <span className="dropzone-hint">{SOURCE_EXTENSION_LABEL}</span>
          </div>
        )}
        <input id={inputId} type="file" accept={ACCEPTED_SOURCE_FILES} onChange={handleFileChange} />
      </label>

      {error ? <div className="alert alert-error config-source-upload-alert">{error}</div> : null}

      {file && stage === "select" ? (
        <div className="config-source-upload-actions">
          <button className="btn btn-primary" type="button" onClick={() => void startUpload("source")}>
            上传并生成 Source Digest
          </button>
          <button className="btn btn-secondary" type="button" onClick={() => reset()}>
            清空
          </button>
        </div>
      ) : null}

      {progress.pct > 0 && !["preview", "stored", "done"].includes(stage) ? (
        <div className="config-source-upload-progress">
          <div className="progress-bar-bg">
            <div className="progress-bar-fill" style={{ width: `${progress.pct}%` }} />
          </div>
          <span>{progress.detail}</span>
        </div>
      ) : null}

      {stage === "preview" && fullPreview ? (
        <div className="config-source-upload-result">
          <span className="market-scan-panel-eyebrow">Matrix Preview</span>
          <div className="summary-chips">
            <span className="chip chip-positive">{numericValue(summary.trimCount)} 车型</span>
            <span className="chip chip-info">{numericValue(summary.newTrims)} 新增</span>
            <span className="chip chip-warning">{numericValue(summary.existingTrims)} 已有</span>
            <span className="chip chip-neutral">{numericValue(summary.changedValues)} 变更</span>
          </div>
          {preview?.warningCount ? (
            <small className="market-scan-field-hint">解析警告 {preview.warningCount} 条，导入前建议检查字段映射。</small>
          ) : null}
          <div className="config-source-upload-actions">
            <button className="btn btn-primary" type="button" onClick={() => void confirmMatrixImport()}>
              确认导入为 Draft
            </button>
            <button className="btn btn-secondary" type="button" onClick={() => reset()}>
              重来
            </button>
          </div>
        </div>
      ) : null}

      {stage === "stored" && sourceReceipt ? (
        <div className="config-source-upload-result">
          <span className="market-scan-panel-eyebrow">Source Digest Stored</span>
          <div className="summary-chips">
            <span className="chip chip-info">{fileTypeLabel(receiptSourceType || "source")}</span>
            <span className="chip chip-positive">{sourceStatusLabel(sourceReceipt)}</span>
            <span className="chip chip-warning">{extractStatusLabel(sourceReceipt)}</span>
          </div>
          <small className="market-scan-field-hint">
            来源批次 {receiptImportBatchId || "-"}，文件指纹 {receiptHash ? `${receiptHash.slice(0, 12)}...` : "-"}。
          </small>
          {sourceReceipt.linkedToCurrentContext ? (
            <small className="market-scan-field-hint">
              已关联上下文：{sourceContextParts(sourceReceipt.relatedContext).join(" · ") || "未指定"}。
            </small>
          ) : null}
          {renderSourceReceiptNextStep(sourceReceipt)}
          <small className="market-scan-field-hint">{sourceReceiptGuide(sourceReceipt)}</small>
          {uploadedSourceCanSearch ? (
            <div className="config-source-upload-bridge" aria-label="上传来源回到 FloatingDeck 搜索">
              <div>
                <span>{uploadedSourceSearchRequested ? "已送入 FloatingDeck 来源搜索" : "可回到 FloatingDeck 来源搜索"}</span>
                <strong>{uploadedSourceSearchQuery}</strong>
                <small>
                  {uploadedSourceSearchRequested
                    ? "Source Digest 搜索框已使用这个检索词；当前主对比筛选保持不变，继续在 FloatingDeck 生成在线配置列。"
                    : "一个 source 可能包含多个 model / 配置列；点击后只把检索词送到 Source Digest 搜索框，不切换当前主对比筛选，命中后再生成在线配置列。"}
                </small>
                <small>生成后会进入显示 / 编辑工作区，可在线核对并导出 XLSX / PDF。</small>
              </div>
              <button
                className="btn btn-sm btn-secondary"
                type="button"
                onClick={requestUploadedSourceSearch}
              >
                {uploadedSourceSearchRequested ? "重新搜索这个来源" : "在 FloatingDeck 搜索这个来源"}
              </button>
            </div>
          ) : null}
          {draftResult ? renderDigestDraftResult(draftResult, Boolean(onDraftCreated)) : null}
          {draftError ? <small className="market-scan-field-hint">创建可编辑配置列失败：{draftError}</small> : null}
          {sourceDigestCopyFeedback ? <small className="market-scan-field-hint" role="status">{sourceDigestCopyFeedback}</small> : null}
          {renderSourceDigest(sourceReceipt.sourceDigest, {
            sourceId: sourceReceipt.sourceId,
            sourceContext: sourceReceipt.relatedContext ?? relatedContext,
            creatingDraftGroupKey,
            createdDraftGroupKey: draftResult ? digestDraftActionKey(draftResult.sourceId, draftResult.groupId) : null,
            exportingGroupKey: exportingSourceDigestGroupKey,
            trimSelections: sourceDigestTrimSelections,
            reviewFocuses: sourceDigestReviewFocuses,
            trimIdentityDrafts: sourceDigestTrimIdentityDrafts,
            onCreateDraft: (group, selectedTrimIds) => void createDraftFromDigestGroup(sourceReceipt.sourceId, group, selectedTrimIds),
            onCopyPreview: (group, selectedTrimIds) => void copySourceDigestPreviewGroup(group, selectedTrimIds),
            onToggleTrim: toggleSourceDigestTrim,
            onIdentityDraftChange: updateSourceDigestTrimIdentityDraft,
            onReviewRowFocus: focusSourceDigestReviewRow,
          })}
          <div className="config-source-upload-actions">
            <button className="btn btn-secondary" type="button" onClick={() => reset()}>
              继续上传
            </button>
          </div>
        </div>
      ) : null}

      {stage === "done" ? (
        <div className="config-source-upload-result">
          <span className="chip chip-positive">导入完成</span>
          <small className="market-scan-field-hint">已保存为 Draft，候选配置列刷新后可加入对比。</small>
          <div className="config-source-upload-actions">
            <button className="btn btn-secondary" type="button" onClick={() => reset()}>
              继续上传
            </button>
          </div>
        </div>
      ) : null}

      {showSourceLibrary ? <div className="config-source-snapshot-list">
        {stage !== "stored" && draftResult ? renderDigestDraftResult(draftResult, Boolean(onDraftCreated)) : null}
        <div className="config-source-snapshot-list__header">
          <div>
            <span className="market-scan-panel-eyebrow">{sourceLibraryTitle}</span>
            <small className="market-scan-field-hint">{sourceLibraryHint}</small>
          </div>
          <div className="config-source-snapshot-list__actions">
            <button
              className={`btn btn-sm ${effectiveSourceLibraryScope === "country" ? "btn-primary" : "btn-secondary"}`}
              type="button"
              disabled={!sourceLibraryCountry}
              aria-pressed={effectiveSourceLibraryScope === "country"}
              onClick={() => setSourceLibraryScope("country")}
            >
              当前国家
            </button>
            <button
              className={`btn btn-sm ${effectiveSourceLibraryScope === "all" ? "btn-primary" : "btn-secondary"}`}
              type="button"
              aria-pressed={effectiveSourceLibraryScope === "all"}
              onClick={() => setSourceLibraryScope("all")}
            >
              全部
            </button>
            <button
              className={`btn btn-sm ${effectiveSourceLibraryScope === "trash" ? "btn-primary" : "btn-secondary"}`}
              type="button"
              aria-pressed={effectiveSourceLibraryScope === "trash"}
              onClick={() => setSourceLibraryScope("trash")}
            >
              垃圾桶
            </button>
            <button className="btn btn-sm btn-secondary" type="button" onClick={() => void loadSourceSnapshots()}>
              刷新
            </button>
          </div>
        </div>
        <div className="config-source-library-search-row">
          <SearchDropdownFilter
            key={`source-library-search-${sourceLibrarySearchResetKey}`}
            label="搜索来源库"
            loading={sourceSnapshotsLoading}
            value={sourceLibrarySearchValue}
            options={sourceLibrarySearchOptions}
            placeholder="搜索文件名 / 上传人 / 车型 / 市场 / 国家 / segment / 场景 / 身份锚点..."
            emptyLabel="没有匹配来源；可切换全部范围或上传新来源"
            onChange={selectSourceLibrarySnapshot}
            onQueryChange={(query) => {
              setSourceLibrarySearchQuery(query);
              if (sourceLibrarySearchValue) setSourceLibrarySearchValue("");
            }}
          />
          {sourceLibrarySearchQuery.trim() || sourceLibrarySearchValue ? (
            <button
              className="btn btn-sm btn-secondary config-source-library-search-row__clear"
              type="button"
              aria-label="清除来源库搜索"
              onClick={clearSourceLibrarySearch}
            >
              清除搜索
            </button>
          ) : null}
        </div>
        <div className="config-source-library-type-filter" aria-label="来源库类型筛选">
          {sourceLibraryTypeFilterItemsList.map((item) => {
            const active = sourceLibraryTypeFilter === item.key;
            const disabled = item.key !== "all" && item.count === 0 && !active;
            return (
              <button
                className={`config-source-library-type-filter__chip ${active ? "is-active" : ""}`.trim()}
                key={item.key}
                type="button"
                aria-label={`筛选来源库：${item.label} ${item.count} 个`}
                aria-pressed={active}
                title={item.description}
                disabled={disabled}
                onClick={() => setSourceLibraryTypeFilter(item.key)}
              >
                <span>{item.label}</span>
                <strong>{item.count}</strong>
              </button>
            );
          })}
        </div>
        {sourceSnapshotsLoading ? <small className="market-scan-field-hint">正在加载来源快照...</small> : null}
        {sourceSnapshotsError ? <small className="market-scan-field-hint">来源列表暂不可用：{sourceSnapshotsError}</small> : null}
        {sourceLibraryFeedback ? <small className="market-scan-field-hint">{sourceLibraryFeedback}</small> : null}
        {stage !== "stored" && sourceDigestCopyFeedback ? <small className="market-scan-field-hint" role="status">{sourceDigestCopyFeedback}</small> : null}
        {!sourceSnapshotsLoading && !sourceSnapshotsError && sourceSnapshotTotalRows > 0 ? (
          <small className="market-scan-field-hint config-source-snapshot-count">
            {sourceLibraryTypeFilter === "all" ? (
              <>
                显示 {recentSnapshots.length}/{sourceSnapshotTotalRows} 个来源
                {hiddenSourceSnapshotCount > 0 ? `，还有 ${hiddenSourceSnapshotCount} 个可通过搜索定位。` : "。"}
              </>
            ) : (
              <>
                {sourceLibraryTypeFilterItemsList.find((item) => item.key === sourceLibraryTypeFilter)?.label ?? "当前类型"}：显示 {recentSnapshots.length}/{sourceSnapshotsByType.length} 个已加载来源
                {hiddenSourceSnapshotCount > 0 ? `，还有 ${hiddenSourceSnapshotCount} 个当前类型来源。` : sourceSnapshotsForDisplay.length < sourceSnapshotTotalRows ? "；更多来源可通过搜索定位。" : "。"}
              </>
            )}
          </small>
        ) : null}
        {!sourceSnapshotsLoading && !sourceSnapshotsError && recentSnapshots.length === 0 ? (
          <small className="market-scan-field-hint">
            {sourceLibraryTypeFilter !== "all"
              ? `当前已加载来源里没有${sourceLibraryTypeFilterItemsList.find((item) => item.key === sourceLibraryTypeFilter)?.label ?? "该类型"}；可切回全部或继续搜索来源库。`
              : effectiveSourceLibraryScope === "trash"
              ? sourceTrashRequiresCountry ? "未选择国家，暂不展示来源垃圾桶。" : "当前国家垃圾桶为空。"
              : "暂无来源文件。"}
          </small>
        ) : null}
        {effectiveSourceLibraryScope === "trash" && recentSnapshots.length > 0 ? (
          <button
            className="btn btn-sm btn-secondary config-source-snapshot-clear-trash"
            type="button"
            disabled={sourceLibraryActionId === "__clear_trash__"}
            onClick={() => void clearSourceTrash()}
          >
            {sourceLibraryActionId === "__clear_trash__" ? "清空中..." : "清空当前国家垃圾桶"}
          </button>
        ) : null}
        {recentSnapshots.length > 0 ? (
          <div className="config-source-snapshot-items">
            {recentSnapshots.map((snapshot, snapshotIndex) => {
              const snapshotDetail = sourceSnapshotDetails[snapshot.sourceId] ?? snapshot;
              const isExpanded = expandedSourceId === snapshot.sourceId;
              const isDigestLoading = sourceDigestLoadingId === snapshot.sourceId;
              const digestError = sourceDigestErrors[snapshot.sourceId];
              const canReviewDigest = effectiveSourceLibraryScope !== "trash" && snapshot.extractStatus !== "not_applicable";
              const canMoveToTrash = effectiveSourceLibraryScope === "country" && Boolean(sourceLibraryCountry);
              const digestStatus = snapshotDetail.sourceDigest ?? snapshotDetail.sourceDigestStatus ?? snapshot.sourceDigestStatus ?? null;
              const digestStatusParts = sourceSnapshotDigestStatusParts(digestStatus);
              const digestStatusHint = sourceSnapshotDigestStatusHint(digestStatus);
              const digestOcrComparisonText = digestStatus ? engineeringConfigOcrComparisonText(digestStatus) : null;
              const digestIdentityParts = sourceDigestIdentityParts(digestStatus);
              const searchMatchChips = sourceSnapshotSearchMatchChips(snapshotDetail);
              return (
                <div className="config-source-snapshot-item" key={`${snapshot.sourceId}-${snapshotIndex}`}>
                  <div>
                    <strong>{snapshot.sourceFileName}</strong>
                    <span>{fileTypeLabel(snapshot.fileType)} · {formatFileSize(snapshot.fileSize)} · {sourceOwnerLabel(snapshot.createdBy)} · {formatDateTime(snapshot.createdAt)}</span>
                    {searchMatchChips.length > 0 ? (
                      <div className="config-source-snapshot-match-chips" aria-label={`来源搜索命中：${searchMatchChips.join("，")}`}>
                        {searchMatchChips.map((match, matchIndex) => (
                          <span key={`${match}-${matchIndex}`}>{match}</span>
                        ))}
                      </div>
                    ) : null}
                    {sourceContextParts(snapshot.relatedContext).length > 0 ? (
                      <span>{sourceContextParts(snapshot.relatedContext).join(" · ")}</span>
                    ) : null}
                    {digestIdentityParts.length > 0 ? (
                      <span>{digestIdentityParts.join(" · ")}</span>
                    ) : null}
                  </div>
                  <div className="config-source-snapshot-item__status">
                    <span>{sourceStatusLabel(snapshot)}</span>
                    <span>{extractStatusLabel(snapshot)}</span>
                    {digestStatusParts.map((part, partIndex) => (
                      <span key={`${part}-${partIndex}`}>{part}</span>
                    ))}
                    {canReviewDigest ? (
                      <button
                        className="btn btn-sm btn-secondary"
                        type="button"
                        disabled={isDigestLoading}
                        onClick={() => void toggleSourceSnapshotDigest(snapshot)}
                      >
                        {isDigestLoading ? "加载中" : isExpanded ? "收起 Digest" : "查看 Digest"}
                      </button>
                    ) : null}
                    {effectiveSourceLibraryScope === "trash" ? (
                      <button
                        className="btn btn-sm btn-secondary"
                        type="button"
                        disabled={sourceLibraryActionId === snapshot.sourceId}
                        onClick={() => void restoreSourceSnapshot(snapshot.sourceId)}
                      >
                        {sourceLibraryActionId === snapshot.sourceId ? "恢复中" : "恢复"}
                      </button>
                    ) : canMoveToTrash ? (
                      <button
                        className="btn btn-sm btn-ghost"
                        type="button"
                        disabled={sourceLibraryActionId === snapshot.sourceId}
                        onClick={() => void moveSourceSnapshotToTrash(snapshot.sourceId)}
                      >
                        {sourceLibraryActionId === snapshot.sourceId ? "处理中" : "移入垃圾桶"}
                      </button>
                    ) : null}
                  </div>
                  {!compact && snapshot.sourceFileHash ? (
                    <small>sha256 {snapshot.sourceFileHash.slice(0, 16)}...</small>
                  ) : null}
                  {digestOcrComparisonText ? (
                    <small className="market-scan-field-hint config-source-ocr-candidates__summary">
                      {digestOcrComparisonText}
                    </small>
                  ) : null}
                  {digestStatusHint ? <small className="market-scan-field-hint">{digestStatusHint}</small> : null}
                  {isExpanded ? (
                    <div className="config-source-snapshot-item__digest">
                      {digestError ? <small className="market-scan-field-hint">Digest 详情加载失败：{digestError}</small> : null}
                      {isDigestLoading ? <small className="market-scan-field-hint">正在加载 Digest 详情...</small> : null}
                      {!isDigestLoading && !digestError ? (
                        renderSourceDigest(snapshotDetail.sourceDigest, {
                          sourceId: snapshot.sourceId,
                          sourceContext: snapshotDetail.relatedContext ?? snapshot.relatedContext ?? relatedContext,
                          creatingDraftGroupKey,
                          createdDraftGroupKey: draftResult ? digestDraftActionKey(draftResult.sourceId, draftResult.groupId) : null,
                          exportingGroupKey: exportingSourceDigestGroupKey,
                          trimSelections: sourceDigestTrimSelections,
                          reviewFocuses: sourceDigestReviewFocuses,
                          trimIdentityDrafts: sourceDigestTrimIdentityDrafts,
                          onCreateDraft: (group, selectedTrimIds) => void createDraftFromDigestGroup(snapshot.sourceId, group, selectedTrimIds),
                          onCopyPreview: (group, selectedTrimIds) => void copySourceDigestPreviewGroup(group, selectedTrimIds),
                          onToggleTrim: toggleSourceDigestTrim,
                          onIdentityDraftChange: updateSourceDigestTrimIdentityDraft,
                          onReviewRowFocus: focusSourceDigestReviewRow,
                        }) ?? (
                          <small className="market-scan-field-hint">
                            当前来源暂无 digest 详情，可能仍在抽取或不是可配置表来源。
                          </small>
                        )
                      ) : null}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        ) : null}
      </div> : null}
    </div>
  );
}
