// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { clearLlmBusinessSummaryCacheForTests } from "../../components/BusinessSummaryPanel";
import { AuthProvider } from "../../contexts/AuthContext";
import { ProductConfigComparePage } from "../../pages/ProductConfigComparePage";
import type {
  CompareResponse,
  EngineeringConfigDigestDraftResult,
  EngineeringConfigSourceContext,
  EngineeringConfigSourceDigest,
  EngineeringConfigSourceDigestGroup,
  EngineeringConfigSourceSnapshot,
  VehicleTrimItem,
} from "../../types/engineeringConfig";

vi.setConfig({ testTimeout: 15000 });

const originalClipboard = navigator.clipboard;
const CONFIG_COLUMN_TAB_NAME = /^配置列/;
const SOURCE_PANEL_TAB_NAME = /(?:Source Digest|来源 \/ 上传)/;
const DISPLAY_PANEL_TAB_NAME = /显示(?:模式| \/ 编辑)/;
const SOURCE_DIGEST_SEARCH_COMBOBOX_NAME = /搜索\s*(?:来源 \/ (?:车型|Model) \/ 配置列|Source Digest 可比组)/;
const SOURCE_DIGEST_PICKER_COMBOBOX_NAME = /选择(?:来源 \/ 车型|来源 \/ Model|Source \/ Model) \/ 配置列/;
const SOURCE_DIGEST_SCOPE_LABEL = /(?:来源|Source Digest\s*)当前搜索范围/;

vi.mock("../../api/client", () => ({
  apiUrl: (path: string) => path,
  api: {
    compareEngineeringConfigTrims: vi.fn(),
    composeEngineeringConfigBusinessSummary: vi.fn(),
    completeEngineeringConfigSourceUpload: vi.fn(),
    createEngineeringConfigFeatureValue: vi.fn(),
    createEngineeringConfigDraftFromSourceDigest: vi.fn(),
    clearEngineeringConfigSourceTrash: vi.fn(),
    clearEngineeringConfigTrimTrash: vi.fn(),
    exportEngineeringConfigComparePdf: vi.fn(),
    exportEngineeringConfigCompareXlsx: vi.fn(),
    getEngineeringConfigBusinessSummaryReadiness: vi.fn(),
    getEngineeringConfigOcrReadiness: vi.fn(),
    getEngineeringConfigLocalWorkbookDigest: vi.fn(),
    getEngineeringConfigSourceSnapshot: vi.fn(),
    initiateEngineeringConfigFeatureCatalogUpload: vi.fn(),
    initiateEngineeringConfigSourceUpload: vi.fn(),
    listEngineeringConfigCompetitorRecommendations: vi.fn(),
    listEngineeringConfigSourceSnapshots: vi.fn(),
    listEngineeringConfigTrims: vi.fn(),
    restoreEngineeringConfigSourceSnapshot: vi.fn(),
    trashEngineeringConfigSourceSnapshot: vi.fn(),
    updateEngineeringConfigFeatureValue: vi.fn(),
    updateEngineeringConfigTrim: vi.fn(),
    uploadEngineeringConfigFeatureCatalogChunk: vi.fn(),
    uploadEngineeringConfigSourceChunk: vi.fn(),
    completeEngineeringConfigFeatureCatalogUpload: vi.fn(),
  },
}));

const digest: EngineeringConfigSourceDigest = {
  digestType: "workbook",
  workbookFormat: "eu_config_resource_table",
  status: "ready",
  fileName: "compare-sample.xlsx",
  modelName: "T19C MY ICE",
  summary: {
    sheetCount: 1,
    tableCount: 1,
    candidateTrimCount: 2,
    comparableGroupCount: 1,
    featureCount: 3,
    differenceCount: 2,
  },
  sheets: [],
  compareGroups: [
    {
      groupId: "t19c",
      title: "T19C MY ICE",
      sourceSheet: "T19C MY ICE",
      modelName: "T19C MY ICE",
      trimCount: 2,
      featureCount: 3,
      differenceCount: 2,
      summary: {
        totalFeatures: 3,
        shownFeatures: 3,
        commonSameCount: 1,
        differentValueCount: 1,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        uniqueOrPartialCount: 1,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 2,
        rawConfirmedDifferenceCount: 2,
        inferredDifferenceCount: 0,
        availabilityDifferentCount: 1,
        differenceCount: 2,
        categoryCounts: {
          Safety: 1,
          Wheel: 1,
          Infotainment: 1,
        },
        differenceCategories: ["Safety", "Wheel"],
      },
      trims: [
        {
          trimId: "basic",
          trimName: "Basic",
          fullTrimName: "Basic",
          modelName: "T19C MY ICE",
          materialNo: "T71607V**MM0001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceSheet: "T19C MY ICE",
        },
        {
          trimId: "premium",
          trimName: "Premium",
          fullTrimName: "Premium",
          modelName: "T19C MY ICE",
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          sourceSheet: "T19C MY ICE",
        },
      ],
      rows: [
        {
          category: "Safety",
          featureCode: "blind_spot",
          featureName: "Blind spot",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["premium"],
          businessNote: "Premium 独有配置",
          values: [
            {
              valueId: "basic-blind",
              rawValue: "",
              normalizedValue: null,
              availability: "NOT_AVAILABLE",
              unit: null,
              displayValue: "不配备",
            },
            {
              valueId: "premium-blind",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
            },
          ],
        },
        {
          category: "Wheel",
          featureCode: "wheel_size",
          featureName: "Wheel size",
          comparisonType: "DIFFERENT_VALUE",
          uniqueTrimIds: [],
          businessNote: "配置值不同",
          values: [
            {
              valueId: "basic-wheel",
              rawValue: "18 inch",
              normalizedValue: "18 inch",
              availability: "VALUE",
              unit: null,
              displayValue: "18 inch",
            },
            {
              valueId: "premium-wheel",
              rawValue: "20 inch",
              normalizedValue: "20 inch",
              availability: "VALUE",
              unit: null,
              displayValue: "20 inch",
            },
          ],
        },
        {
          category: "Infotainment",
          featureCode: "speaker",
          featureName: "Speaker",
          comparisonType: "COMMON_SAME",
          uniqueTrimIds: [],
          businessNote: "共同配置",
          values: [
            {
              valueId: "basic-speaker",
              rawValue: "6",
              normalizedValue: "6",
              availability: "VALUE",
              unit: null,
              displayValue: "6",
            },
            {
              valueId: "premium-speaker",
              rawValue: "6",
              normalizedValue: "6",
              availability: "VALUE",
              unit: null,
              displayValue: "6",
            },
          ],
        },
      ],
    },
  ],
};

function latestDraftCompare(data: CompareResponse): CompareResponse {
  return {
    ...data,
    versionScope: "latest",
    usesDraft: true,
    versionFallbackCount: 0,
    trims: data.trims.map((trim, index) => ({
      ...trim,
      configVersionId: trim.configVersionId ?? `draft-version-${index}`,
      configVersionNo: trim.configVersionNo ?? 1,
      configVersionStatus: "draft",
      draftVersionAvailable: true,
      publishedVersionAvailable: trim.publishedVersionAvailable ?? false,
      versionFallback: false,
    })),
  };
}

const libraryTrimFixtures: VehicleTrimItem[] = [
  {
    trimId: "library-core",
    brand: "Volvo",
    modelName: "EX30",
    trimName: "Core",
    fullTrimName: "Volvo EX30 Core",
    energyType: "BEV",
    drivetrain: "RWD",
    engine: null,
    modelYear: "2026",
    status: "draft",
    market: "Germany",
    country: "Germany",
    vehicleCode: null,
    materialNo: "MAT-EX30-CORE",
    identityKey: "VOLVO-EX30-CORE-DE",
    salesVersion: "Core",
    sourceUploadId: "source-own",
    sourceFileName: "own-ex30.xlsx",
    sourceFilePath: "/tmp/own-ex30.xlsx",
    importStatus: "draft",
    hasMaterialNo: true,
    dataOrigin: "own_catalog",
  },
  {
    trimId: "library-ultra",
    brand: "Volvo",
    modelName: "EX30",
    trimName: "Ultra",
    fullTrimName: "Volvo EX30 Ultra",
    energyType: "BEV",
    drivetrain: "AWD",
    engine: null,
    modelYear: "2026",
    status: "draft",
    market: "Germany",
    country: "Germany",
    vehicleCode: null,
    materialNo: null,
    identityKey: "VOLVO-EX30-ULTRA-DE",
    salesVersion: "Ultra",
    sourceUploadId: "source-rival",
    sourceFileName: "rival-ex30.html",
    sourceFilePath: "/tmp/rival-ex30.html",
    importStatus: "draft",
    hasMaterialNo: false,
    dataOrigin: "external_or_scraped",
  },
  {
    trimId: "library-rival",
    brand: "Smart",
    modelName: "#1",
    trimName: "Premium",
    fullTrimName: "Smart #1 Premium",
    energyType: "BEV",
    drivetrain: "RWD",
    engine: null,
    modelYear: "2026",
    status: "draft",
    market: "Germany",
    country: "Germany",
    vehicleCode: null,
    materialNo: null,
    identityKey: "SMART-1-PREMIUM-DE",
    salesVersion: "Premium",
    sourceUploadId: "source-smart",
    sourceFileName: "smart-config.pdf",
    sourceFilePath: "/tmp/smart-config.pdf",
    sourceCreatedBy: "alice",
    importStatus: "draft",
    hasMaterialNo: false,
    dataOrigin: "external_or_scraped",
  },
];

function buildThreeTrimDigest(): EngineeringConfigSourceDigest {
  return {
    ...digest,
    summary: {
      ...digest.summary,
      candidateTrimCount: 3,
      featureCount: 3,
      differenceCount: 3,
    },
    compareGroups: [
      {
        ...digest.compareGroups[0],
        trimCount: 3,
        differenceCount: 3,
        summary: {
          ...digest.compareGroups[0].summary,
          totalFeatures: 3,
          shownFeatures: 3,
          confirmedDifferenceCount: 3,
          differenceCount: 3,
        },
        trims: [
          ...digest.compareGroups[0].trims,
          {
            trimId: "luxury",
            trimName: "Luxury",
            fullTrimName: "Luxury",
            modelName: "T19C MY ICE",
            hasMaterialNo: false,
            dataOrigin: "external_or_scraped",
            sourceSheet: "T19C MY ICE",
          },
        ],
        rows: digest.compareGroups[0].rows.map((row) => {
          if (row.featureCode === "speaker") {
            return {
              ...row,
              comparisonType: "DIFFERENT_VALUE",
              values: [
                row.values[0],
                row.values[1],
                {
                  valueId: "luxury-speaker",
                  rawValue: "8",
                  normalizedValue: "8",
                  availability: "VALUE",
                  unit: null,
                  displayValue: "8",
                  source: {
                    sheetName: "T19C MY ICE",
                    rowNumber: 22,
                    columnNumber: 5,
                    columnLetter: "E",
                    cell: "E22",
                    sourceCell: "E22",
                    mergedRange: null,
                  },
                },
              ],
            };
          }
          return {
            ...row,
            values: [
              row.values[0],
              row.values[1],
              row.values[0],
            ],
          };
        }),
      },
    ],
  };
}

function buildCrossMarketDigest(): EngineeringConfigSourceDigest {
  return {
    ...digest,
    compareGroups: [
      {
        ...digest.compareGroups[0],
        trims: digest.compareGroups[0].trims.map((trim, index) => ({
          ...trim,
          market: index === 0 ? "Germany" : "France",
          country: index === 0 ? "Germany" : "France",
          profile: {
            ...(trim.profile ?? {}),
            country: index === 0 ? "Germany" : "France",
          },
        })),
      },
    ],
  };
}

function buildManyGroupDigest(groupCount: number): EngineeringConfigSourceDigest {
  const groups: EngineeringConfigSourceDigestGroup[] = Array.from({ length: groupCount }, (_, index) => {
    const groupNumber = index + 1;
    const targetCountry = groupNumber === groupCount ? "Germany" : "France";
    const targetModelYear = groupNumber === groupCount ? "2026" : "2025";
    return {
      ...digest.compareGroups[0],
      groupId: `group-${groupNumber}`,
      title: `Sample Group ${groupNumber}`,
      sourceSheet: `Sheet ${groupNumber}`,
      modelName: `Sample Group ${groupNumber}`,
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        country: targetCountry,
        market: targetCountry,
        profile: {
          ...(trim.profile ?? {}),
          country: targetCountry,
          modelYear: targetModelYear,
        },
        trimId: `${trim.trimId}-${groupNumber}`,
      })),
      rows: digest.compareGroups[0].rows.map((row) => ({
        ...row,
        values: row.values.map((value, valueIndex) => (
          value == null
            ? null
            : {
              ...value,
              valueId: `${value.valueId}-${groupNumber}-${valueIndex}`,
            }
        )),
      })),
    };
  });
  return {
    ...digest,
    summary: {
      ...digest.summary,
      comparableGroupCount: groupCount,
    },
    compareGroups: groups,
  };
}

function buildSourceSnapshotFixture(
  sourceId: string,
  sourceFileName: string,
  sourceDigest: EngineeringConfigSourceDigest | null,
): EngineeringConfigSourceSnapshot {
  return {
    sourceId,
    batchId: sourceId,
    importBatchId: sourceId,
    uploadType: "source_snapshot",
    sourceFileName,
    fileType: "xlsx",
    mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    fileSize: 4,
    sourceFileHash: `${sourceId}-hash`,
    sourceFilePath: `/tmp/${sourceFileName}`,
    uploadStatus: "registered",
    extractStatus: "digest_ready",
    nextAction: "review_digest",
    createdBy: "tester",
    createdAt: "2026-06-15T00:00:00Z",
    errorMessage: null,
    linkedToCurrentContext: true,
    relatedContext: {
      brand: "OMODA",
      model: sourceDigest?.compareGroups[0]?.modelName ?? "Library Model",
      market: "EU",
      country: "EU",
      modelYear: null,
      trimIds: [],
      salesVersionIds: [],
      contextType: "compare",
    },
    contexts: [],
    sourceDigestStatus: sourceDigest
      ? {
          digestType: sourceDigest.digestType,
          status: sourceDigest.status,
          summary: {
            candidateTrimCount: sourceDigest.summary.candidateTrimCount,
            comparableGroupCount: sourceDigest.summary.comparableGroupCount,
            featureCount: sourceDigest.summary.featureCount,
            differenceCount: sourceDigest.summary.differenceCount,
          },
        }
      : null,
    sourceDigest,
  };
}

describe("ProductConfigComparePage", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    clearLlmBusinessSummaryCacheForTests();
    localStorage.setItem("jato_user_role", "editor");
    window.HTMLElement.prototype.scrollIntoView = vi.fn();
    window.requestAnimationFrame = (callback: FrameRequestCallback): number => {
      callback(0);
      return 0;
    };
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValue({ rows: 0, items: [] });
    vi.mocked(api.listEngineeringConfigCompetitorRecommendations).mockResolvedValue({
      country: null,
      modelName: null,
      rows: 0,
      items: [],
      message: "no_competitors",
    });
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 0, items: [] });
    vi.mocked(api.trashEngineeringConfigSourceSnapshot).mockResolvedValue({
      ...buildSourceSnapshotFixture("source-trashed", "source-trashed.xlsx", null),
      uploadStatus: "trashed",
      libraryStatus: "stored",
      inTrash: true,
      message: "Source moved to trash.",
    });
    vi.mocked(api.restoreEngineeringConfigSourceSnapshot).mockResolvedValue({
      ...buildSourceSnapshotFixture("source-restored", "source-restored.xlsx", null),
      uploadStatus: "registered",
      libraryStatus: "stored",
      inTrash: false,
      message: "Source restored.",
    });
    vi.mocked(api.clearEngineeringConfigSourceTrash).mockResolvedValue({
      cleared: 1,
      country: "Germany",
      message: "Cleared 1 trashed source.",
    });
    vi.mocked(api.getEngineeringConfigOcrReadiness).mockResolvedValue({
      status: "ready",
      ready: true,
      defaultEngine: "paddleocr",
      imageOcrReady: true,
      pdfOcrReady: true,
      pdfRenderReady: true,
      paddleOcrReady: true,
      legacyOcrReady: false,
      configuredLanguage: "ch",
      components: [
        { name: "pypdfium2", available: true, detail: "Scanned PDF page rendering" },
        { name: "paddleocr", available: true, detail: "PaddleOCR wrapper package" },
      ],
      warnings: [],
      notes: [],
    });
    vi.mocked(api.getEngineeringConfigBusinessSummaryReadiness).mockResolvedValue({
      ready: true,
      status: "ready",
      provider: "deepseek",
      model: "deepseek-chat",
      apiBase: "https://api.deepseek.com",
      keySource: "DEEPSEEK_API_KEY",
      providerConfigured: true,
      runtimeUrl: "http://127.0.0.1:6185/",
      runtimeUsed: false,
      runtimeStatus: "not_used_by_compare_runtime_compose",
      liveCheck: "not_performed",
      cacheSize: 0,
      cacheLimit: 64,
      pipeline: "compare_runtime_compose",
      persisted: false,
      message: "Engineering config AI summaries are composed from the current compare facts at runtime.",
      notes: [
        "Source Digest upload stores source files and extracted facts only.",
        "Business summaries are generated from the currently selected config columns.",
      ],
    });
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValue(digest);
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue({
      sourceId: "source-empty",
      batchId: "source-empty",
      importBatchId: "source-empty",
      uploadType: "source_snapshot",
      sourceFileName: "empty-source.xlsx",
      fileType: "xlsx",
      mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      fileSize: 4,
      sourceFileHash: "hash-empty",
      sourceFilePath: "/tmp/empty-source.xlsx",
      uploadStatus: "registered",
      extractStatus: "digest_ready",
      nextAction: "review_digest",
      createdBy: "tester",
      createdAt: "2026-06-15T00:00:00Z",
      errorMessage: null,
      linkedToCurrentContext: false,
      relatedContext: {
        brand: null,
        model: null,
        market: null,
        country: null,
        modelYear: null,
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
      contexts: [],
      sourceDigest: null,
    });
    vi.mocked(api.initiateEngineeringConfigSourceUpload).mockResolvedValue({ uploadId: "upload-1", totalChunks: 1 });
    vi.mocked(api.uploadEngineeringConfigSourceChunk).mockResolvedValue({ uploadId: "upload-1", partNumber: 0, receivedBytes: 4 });
    vi.mocked(api.completeEngineeringConfigSourceUpload).mockResolvedValue({
      sourceId: "source-1",
      batchId: "source-1",
      importBatchId: "source-1",
      uploadType: "source_snapshot",
      sourceFileName: "compare-sample.xlsx",
      fileType: "xlsx",
      mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      fileSize: 4,
      sourceFileHash: "hash-1",
      sourceFilePath: "/tmp/compare-sample.xlsx",
      uploadStatus: "registered",
      extractStatus: "digest_ready",
      nextAction: "review_digest",
      createdBy: "tester",
      createdAt: "2026-06-15T00:00:00Z",
      errorMessage: null,
      linkedToCurrentContext: true,
      relatedContext: {
        brand: null,
        model: null,
        market: null,
        country: null,
        modelYear: null,
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
      contexts: [],
      sourceDigest: digest,
      parseMode: "stored_source",
      message: "Source snapshot registered.",
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValue({
      sourceId: "source-1",
      groupId: "t19c",
      importBatchId: "draft-1",
      trimIds: ["draft-basic", "draft-premium"],
      compareTrimIds: ["draft-basic", "draft-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      aliasMatchedFeatureCount: 2,
      semanticAliasMatchedFeatureCount: 1,
      featureMatchReasonCounts: { alias: 1, semantic_alias: 1, created: 1 },
      featureMatchSamples: [
        {
          sourceFeatureName: "360 camera",
          matchedFeatureName: "360 round view camera / 360度高清全景影像",
          matchedFeatureCode: "digest_df9a8c8d2a1dc6e5",
          matchReason: "alias",
        },
      ],
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-basic", "version-premium"],
    });
    vi.mocked(api.updateEngineeringConfigFeatureValue).mockResolvedValue({
      valueId: "value-basic-seat",
      rawValue: "O",
      normalizedValue: "optional",
      availability: "OPTIONAL",
      displayValue: "选装",
      valueState: "marker_value",
      version: 2,
    });
    vi.mocked(api.updateEngineeringConfigTrim).mockResolvedValue({
      trimId: "library-core",
      status: "trashed",
    });
    vi.mocked(api.clearEngineeringConfigTrimTrash).mockResolvedValue({
      cleared: 1,
      market: "Germany",
      message: "Cleared 1 trashed config trims.",
    });
    vi.mocked(api.createEngineeringConfigFeatureValue).mockResolvedValue({
      valueId: "created-value",
      rawValue: "●",
      normalizedValue: "standard",
      availability: "STANDARD",
      displayValue: "标配",
      valueState: "marker_value",
      version: 1,
    });
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValue({
      trims: [],
      rows: [],
      totalFeatures: 0,
      shownFeatures: 0,
    });
    vi.mocked(api.exportEngineeringConfigCompareXlsx).mockResolvedValue(new Blob(["xlsx"]));
    vi.mocked(api.exportEngineeringConfigComparePdf).mockResolvedValue(new Blob(["pdf"], { type: "application/pdf" }));
    vi.mocked(api.initiateEngineeringConfigFeatureCatalogUpload).mockResolvedValue({
      uploadId: "feature-upload-1",
      totalChunks: 1,
      chunkSize: 5 * 1024 * 1024,
      uploadKind: "feature_catalog",
    });
    vi.mocked(api.uploadEngineeringConfigFeatureCatalogChunk).mockResolvedValue({
      uploadId: "feature-upload-1",
      partNumber: 0,
      receivedBytes: 4,
    });
    vi.mocked(api.completeEngineeringConfigFeatureCatalogUpload).mockResolvedValue({
      uploadId: "feature-upload-1",
      fileName: "配置字段映射表.xlsx",
      status: "feature_catalog_imported",
      summary: {
        totalFeatures: 12,
        createdFeatureCount: 2,
        updatedFeatureCount: 7,
        unchangedFeatureCount: 3,
        warningCount: 1,
        warnings: ["第 8 行缺少标准字段英文名，已跳过 aliases 更新。"],
        categories: ["驾驶辅助 Drive assist", "舒适便利 Comfort&Convenient"],
      },
      audit: {
        uploadId: "feature-upload-1",
        fileName: "配置字段映射表.xlsx",
        status: "feature_catalog_imported",
        importedBy: "editor-user",
        importedRole: "editor",
        importedAtUtc: "2026-07-05T12:00:00+00:00",
        artifactRef: "eng_config_uploads/feature-upload-1/session.json",
        persistedIn: "upload_session_meta",
        summary: {
          totalFeatures: 12,
          createdFeatureCount: 2,
          updatedFeatureCount: 7,
          unchangedFeatureCount: 3,
          warningCount: 1,
          warnings: ["第 8 行缺少标准字段英文名，已跳过 aliases 更新。"],
          categories: ["驾驶辅助 Drive assist", "舒适便利 Comfort&Convenient"],
        },
      },
    });
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValue({
      summaries: [],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "missing_key",
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
      },
    });
    URL.createObjectURL = vi.fn(() => "blob:config-export");
    URL.revokeObjectURL = vi.fn();
    window.HTMLAnchorElement.prototype.click = vi.fn();
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: originalClipboard,
    });
  });

  function tableDeltaFilterButton(name: RegExp): HTMLButtonElement {
    let filter = screen.queryByLabelText("差异类型筛选");
    if (!filter) {
      openSimpleTableControls();
      filter = screen.queryByLabelText("差异类型筛选");
    }
    if (!filter) throw new Error("差异类型筛选 should be visible after opening simple table controls.");
    const visibleButton = within(filter).queryByRole("button", { name }) as HTMLButtonElement | null;
    if (visibleButton) return visibleButton;
    switchSummaryMode("expert");
    const expertFilter = screen.getByLabelText("差异类型筛选");
    return within(expertFilter).getByRole("button", { name }) as HTMLButtonElement;
  }

  function tableRangeMetricText(label: string): string {
    const status = screen.getByLabelText("配置表范围状态");
    const metric = Array.from(status.querySelectorAll<HTMLElement>(".comparison-range-status__metric"))
      .find((item) => item.textContent?.includes(label));
    return metric?.textContent?.replace(/\s+/g, "") ?? "";
  }

  function tableRangeStatusText(): string {
    return screen.getByLabelText("配置表范围状态").textContent?.replace(/\s+/g, "") ?? "";
  }

  function expectTableRangeStatusParts(parts: string[]): void {
    const statusText = tableRangeStatusText();
    parts.forEach((part) => {
      expect(statusText).toContain(part.replace(/\s+/g, ""));
    });
  }

  function floatingDeckTrigger(): HTMLButtonElement {
    return screen.getByRole("button", { name: /(?:添加配置列|编辑已开启) \/ 显示/ }) as HTMLButtonElement;
  }

  function switchSummaryMode(mode: "simple" | "expert"): void {
    const currentOptions = screen.queryByLabelText("显示控制中切换配置对比视图模式");
    if (!currentOptions) {
      const controlButton = floatingDeckTrigger();
      if (controlButton.getAttribute("aria-expanded") !== "true") fireEvent.click(controlButton);
      const displayTab = screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME });
      fireEvent.click(displayTab);
    }
    const options = screen.getByLabelText("显示控制中切换配置对比视图模式");
    const label = mode === "simple" ? "简易模式" : "专家模式";
    const button = within(options).getAllByRole("button").find((item) => item.textContent?.includes(label));
    expect(button).toBeTruthy();
    fireEvent.click(button as HTMLElement);
  }

  function openSelectedObjectsPanel(): HTMLElement {
    const controlButton = floatingDeckTrigger();
    if (controlButton.getAttribute("aria-expanded") !== "true") {
      fireEvent.click(controlButton);
    }
    fireEvent.click(screen.getByRole("tab", { name: /已选对象/ }));
    const panel = document.querySelector(".deck-floating-panel") as HTMLElement | null;
    expect(panel).toBeTruthy();
    return panel as HTMLElement;
  }

  function openSimpleSelectedStrip(): HTMLElement {
    const details = screen.getByLabelText("已选配置列抽屉") as HTMLDetailsElement;
    if (!details.open) {
      const summary = details.querySelector("summary");
      expect(summary).toBeTruthy();
      fireEvent.click(summary as HTMLElement);
    }
    return details;
  }

  function clickSelectedObjectAction(name: string | RegExp): void {
    const panel = openSelectedObjectsPanel();
    fireEvent.click(within(panel).getByRole("button", { name }));
  }

  async function focusPremiumDifferenceFromQuickbar(): Promise<void> {
    openSimpleTableControls();
    fireEvent.click(await screen.findByRole("button", { name: /聚焦目标列：Premium/ }));
    fireEvent.click(tableDeltaFilterButton(/差异行/));
  }

  function closePremiumSampleFromSelectedPanel(): void {
    clickSelectedObjectAction("关闭样例 Premium");
    const controlButton = floatingDeckTrigger();
    if (controlButton.getAttribute("aria-expanded") === "true") {
      fireEvent.click(controlButton);
    }
  }

  async function openLocalSampleIfAvailable(): Promise<void> {
    const openSampleButton = screen.queryByRole("button", { name: "查看本地 xlsx 样例" });
    if (!openSampleButton) return;
    fireEvent.click(openSampleButton);
    await screen.findByText(/当前展示 \d+\/\d+ 配置行/, undefined, { timeout: 2500 });
  }

  function openSimpleTableControls(): HTMLDetailsElement {
    const controls = screen.getByLabelText("配置表筛选和目标列") as HTMLDetailsElement;
    if (!controls.open) {
      controls.open = true;
      fireEvent(controls, new Event("toggle", { bubbles: true }));
    }
    return controls;
  }

  function selectSimpleTableCategory(category: string): void {
    const controls = openSimpleTableControls();
    const categoryInput = within(controls).getByRole("combobox", { name: "配置大类" });
    fireEvent.focus(categoryInput);
    fireEvent.change(categoryInput, { target: { value: category } });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: new RegExp(category) }));
  }

  function openFloatingDisplayPanel(): HTMLElement {
    const controlButton = floatingDeckTrigger();
    if (controlButton.getAttribute("aria-expanded") !== "true") {
      fireEvent.click(controlButton);
    }
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    return screen.getByLabelText("配置对比导出控制");
  }

  async function copyCurrentRangeFromFloatingDeck(): Promise<void> {
    const exportControl = openFloatingDisplayPanel();
    const copyButton = within(exportControl).getAllByRole("button").find((button) => button.textContent?.includes("复制当前")) as HTMLButtonElement | undefined;
    expect(copyButton).toBeTruthy();
    await waitFor(() => {
      expect((copyButton as HTMLButtonElement).disabled).toBe(false);
    });
    fireEvent.click(copyButton as HTMLButtonElement);
  }

  async function exportCurrentRangeFromFloatingDeck(format: "xlsx" | "pdf"): Promise<void> {
    const exportControl = openFloatingDisplayPanel();
    const buttonLabel = format === "xlsx" ? "导出当前范围 XLSX" : "导出当前范围 PDF";
    const exportButton = within(exportControl).getByRole("button", { name: buttonLabel }) as HTMLButtonElement;
    await waitFor(() => {
      expect(exportButton.disabled).toBe(false);
    });
    fireEvent.click(exportButton);
  }

  function openSimpleAdvancedSearch(): HTMLElement {
    const advancedSearch = screen.getByLabelText("高级筛选与库内浏览");
    const advancedToggle = within(advancedSearch).getByRole("button", { name: /高级筛选 \/ 库内浏览/ });
    if (advancedToggle.getAttribute("aria-expanded") !== "true") {
      fireEvent.click(advancedToggle);
    }
    return advancedSearch;
  }

  async function openSourceDigestDetailBrowser(scope: HTMLElement): Promise<HTMLElement> {
    const detailBrowser = within(scope).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    if (!detailBrowser.open) {
      fireEvent.click(within(detailBrowser).getByText("来源组详情浏览"));
    }
    return await waitFor(() => (
      within(detailBrowser).getByText("Source Digest 可比组").closest(".market-scan-field") as HTMLElement
    ));
  }

  it("uses product compare query params as initial trim filters", async () => {
    render(
      <MemoryRouter initialEntries={["/product/compare/config?market=DEU&model=T19C%20MY%20ICE&powertrain=BEV&keyword=Premium"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      const hasPrefilteredRequest = vi.mocked(api.listEngineeringConfigTrims).mock.calls.some(([params]) => (
        params?.market === "DEU"
        && params.model_name === "T19C MY ICE"
        && params.energy_type === "BEV"
        && params.q === "Premium"
        && params.limit === 200
      ));
      expect(hasPrefilteredRequest).toBe(true);
    });
    expect(await screen.findByText("当前筛选还没有选择可比配置列。")).toBeTruthy();
    expect(screen.getAllByText("筛选 T19C MY ICE · DEU · BEV · +1").length).toBeGreaterThan(0);
    expect(screen.getByText("筛选：T19C MY ICE · DEU · BEV · +1。请从候选列表添加 2-4 个配置列；库内未命中时上传当前筛选范围的来源。")).toBeTruthy();
    const secondarySourceEntry = screen.getByLabelText("Source Digest 补充入口");
    expect(secondarySourceEntry.textContent).toContain("库内未命中时上传当前筛选范围的来源。");
    expect(within(secondarySourceEntry).queryByRole("button", { name: "查看本地 xlsx 样例" })).toBeNull();
    expect(screen.queryByText("当前展示 3/3 配置行")).toBeNull();
    expect(screen.queryByText("本地 xlsx 样例")).toBeNull();
    expect(screen.queryByText(/本地 xlsx 样例可在下方手动打开/)).toBeNull();
    expect(api.getEngineeringConfigLocalWorkbookDigest).not.toHaveBeenCalled();

    fireEvent.click(within(secondarySourceEntry).getByRole("button", { name: "上传配置表 / 价格单" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    expect(screen.getByText("当前关联上下文")).toBeTruthy();
    expect(screen.getByText("T19C MY ICE · DEU · BEV · 当前筛选来源补充 · 身份锚点 品牌 / 车型 / 市场")).toBeTruthy();
    expect(screen.getByText("这是当前筛选范围的来源补充任务：上传该国家 / 车型 / 动力的配置表或价格单，Digest 成功后生成在线配置列再加入对比。")).toBeTruthy();
    expect(screen.getByText("当前来源库查询：T19C MY ICE；范围 国家 DEU · 动力 BEV")).toBeTruthy();
    await waitFor(() => {
      const hasScopedSourceSearch = vi.mocked(api.listEngineeringConfigSourceSnapshots).mock.calls.some(([params]) => (
        typeof params === "object"
        && params !== null
        && !Array.isArray(params)
        && params.country === "DEU"
        && params.powertrain === "BEV"
        && params.q === "T19C MY ICE"
      ));
      expect(hasScopedSourceSearch).toBe(true);
    });
  });

  it("defers loading the config column library on the default local sample until advanced library browsing opens", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    await new Promise((resolve) => window.setTimeout(resolve, 260));
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();
    expect(screen.queryByText("选择顺序")).toBeNull();
    const selectionHint = screen.getByLabelText("配置列选择提示") as HTMLDetailsElement;
    expect(selectionHint.open).toBe(false);
    expect(within(selectionHint).getByText("车型优先，配置列可多选")).toBeTruthy();
    fireEvent.click(within(selectionHint).getByText("选择提示"));
    expect(selectionHint.open).toBe(true);
    expect(within(selectionHint).getByText(/缺物料号的竞品/)).toBeTruthy();
    const advancedSearch = screen.getByLabelText("高级筛选与库内浏览");
    const advancedToggle = within(advancedSearch).getByRole("button", { name: /高级筛选 \/ 库内浏览/ });
    expect(advancedToggle.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("combobox", { name: "Model / 车型" })).toBeNull();
    expect(document.querySelector("#product-config-deck-advanced-search-body")).toBeNull();
    fireEvent.click(advancedToggle);
    expect(advancedToggle.getAttribute("aria-expanded")).toBe("true");
    expect(document.querySelector("#product-config-deck-advanced-search-body")).toBeTruthy();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });
    expect(screen.getByRole("combobox", { name: "Model / 车型" })).toBeTruthy();
    expect(screen.getByText(/本地样例 1 组 \/ 3 个可预览选项/)).toBeTruthy();
    expect(screen.getByText(/覆盖 1 个来源 \/ 1 个车型/)).toBeTruthy();
    expect(screen.getByText(/本地样例仅用于预览；上传或搜索来源库后可生成正式配置列。/)).toBeTruthy();
    const directDiagnostics = screen.getByLabelText("直接搜索配置列诊断") as HTMLDetailsElement;
    expect(directDiagnostics.open).toBe(false);
    expect(screen.queryByLabelText("直接搜索配置列结果拆解")).toBeNull();
    fireEvent.click(within(directDiagnostics).getByText("搜索诊断"));
    expect(directDiagnostics.open).toBe(true);
    const directSummary = screen.getByLabelText("直接搜索配置列结果拆解");
    expect(directSummary.textContent).toContain("正式配置列");
    expect(directSummary.textContent).toContain("暂无可加入配置列");
    expect(directSummary.textContent).toContain("Source Digest");
    expect(directSummary.textContent).toContain("搜索来源库或上传文件");
    expect(directSummary.textContent).toContain("待生成");
    expect(directSummary.textContent).toContain("同一来源至少暂选 2 列");
    const directCoverage = screen.getByLabelText("直接搜索 Source Digest 覆盖");
    expect(directCoverage.textContent).toContain("本地样例");
    expect(directCoverage.textContent).toContain("1 来源 · 1 车型");
    expect(directCoverage.textContent).toContain("1 组 / 3 选项 · 仅预览");

    const directPicker = screen.getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("预览配置列 · T19C MY ICE · Basic / Premium");
      expect(screen.getByRole("listbox").textContent).toContain("预览");
      expect(screen.getByRole("listbox").textContent).toContain("本地样例来源 / 品牌待补 / 动力 ICE / compare-sample.xlsx / T19C MY ICE / 整组配置列");
      expect(screen.getByRole("listbox").textContent).toContain("本地样例 · compare-sample.xlsx · T19C MY ICE");
      expect(screen.getByRole("listbox").textContent).not.toContain("本地样例仅预览");
      expect(screen.getByRole("listbox").textContent).not.toContain("生成配置列 · T19C MY ICE");
    });
  });

  it("defers loading the config column library on a formal compare URL until advanced library browsing opens", async () => {
    const formalCompare: CompareResponse = {
      trims: digest.compareGroups[0].trims.map((trim) => ({ ...trim, brand: "OMODA" })),
      rows: digest.compareGroups[0].rows,
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(formalCompare as unknown as Record<string, unknown>);

    render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["basic", "premium"], false);
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    await new Promise((resolve) => window.setTimeout(resolve, 260));
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();
    openSimpleAdvancedSearch();

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });
  });

  it("does not reload the config column library when reopening the display edit panel", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    const controlButton = screen.getByRole("button", { name: /添加配置列 \/ 显示/ });
    fireEvent.click(controlButton);
    await new Promise((resolve) => window.setTimeout(resolve, 260));
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    expect(screen.getByLabelText("配置对比导出控制")).toBeTruthy();

    vi.mocked(api.listEngineeringConfigTrims).mockClear();
    fireEvent.click(controlButton);
    expect(screen.queryByLabelText("配置对比导出控制")).toBeNull();
    fireEvent.click(controlButton);
    expect(screen.getByLabelText("配置对比导出控制")).toBeTruthy();
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });
  });

  it("pauses direct config-column search while the floating deck is closed", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    const controlButton = floatingDeckTrigger();
    fireEvent.click(controlButton);
    await new Promise((resolve) => window.setTimeout(resolve, 260));
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    vi.mocked(api.listEngineeringConfigTrims).mockClear();
    const directPicker = screen.getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "Premium" } });
    fireEvent.click(controlButton);

    await new Promise((resolve) => window.setTimeout(resolve, 320));
    expect(api.listEngineeringConfigTrims).not.toHaveBeenCalled();

    fireEvent.click(controlButton);
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ q: "Premium", limit: 80 }));
    });
  });

  it("shows advanced-analysis competitor recommendations inside the config column drawer", async () => {
    vi.mocked(api.listEngineeringConfigCompetitorRecommendations).mockResolvedValue({
      country: "Germany",
      modelName: "Target C-SUV",
      powertrain: "PHEV",
      segment: "SUV C",
      rows: 3,
      items: [
        {
          rank: 1,
          sourceRank: 1,
          modelName: "Rival C-SUV",
          brand: "RivalBrand",
          profile: { segment: "SUV C", powertrain: "PHEV" },
          role: "likely_source",
          similarityScore: 84,
          salesTarget: 1200,
          salesBase: 980,
          deltaVolume: 220,
          shareTarget: 0.12,
          shareChange: 0.02,
          pureShareShift: 80,
          estimatedFlow: 35,
          sharedDimensions: ["segment", "powertrain"],
          matchEvidence: [{ field: "segment", detail: "Segment: same SUV C", score: 100 }],
          recommendationReason: "Segment: same SUV C",
          configAvailable: true,
          configTrimCount: 2,
          sourceDigestAvailable: true,
          sourceDigestSourceCount: 1,
          sourceDigestGroupCount: 1,
          sourceDigestTrimCount: 2,
          sourceDigestSearchQuery: "RivalBrand Rival C-SUV Germany PHEV SUV C",
          sourceDigestMatches: [
            {
              sourceId: "source-rival-ready",
              sourceFileName: "rival-ready-config.xlsx",
              groupCount: 1,
              trimCount: 2,
            },
          ],
          nextAction: "select_config_trim",
          trims: [
            {
              trimId: "rival-premium",
              brand: "RivalBrand",
              modelName: "Rival C-SUV",
              trimName: "Premium AWD",
              fullTrimName: "Rival Premium AWD",
              energyType: "PHEV",
              drivetrain: "AWD",
              engine: null,
              modelYear: "2026",
              status: "active",
              market: "Germany",
              materialNo: null,
              salesVersion: "RIVAL-PREM",
              identityKey: "rival-premium",
              hasMaterialNo: false,
              dataOrigin: "external_or_scraped",
            },
            {
              trimId: "rival-luxury",
              brand: "RivalBrand",
              modelName: "Rival C-SUV",
              trimName: "Luxury AWD",
              fullTrimName: "Rival Luxury AWD",
              energyType: "PHEV",
              drivetrain: "AWD",
              engine: null,
              modelYear: "2026",
              status: "active",
              market: "Germany",
              materialNo: null,
              salesVersion: "RIVAL-LUX",
              identityKey: "rival-luxury",
              hasMaterialNo: false,
              dataOrigin: "external_or_scraped",
            },
          ],
        },
        {
          rank: 2,
          sourceRank: 2,
          modelName: "Digest Rival",
          brand: "DigestBrand",
          profile: { segment: "SUV C", powertrain: "PHEV" },
          role: "adjacent",
          similarityScore: 65,
          salesTarget: 700,
          salesBase: 650,
          deltaVolume: 50,
          shareTarget: 0.07,
          shareChange: 0.01,
          pureShareShift: 20,
          estimatedFlow: 5,
          sharedDimensions: ["segment"],
          matchEvidence: [],
          recommendationReason: "同 segment，来源库已有待生成配置列",
          configAvailable: false,
          configTrimCount: 0,
          sourceDigestAvailable: true,
          sourceDigestSourceCount: 1,
          sourceDigestGroupCount: 1,
          sourceDigestTrimCount: 2,
          sourceDigestSearchQuery: "DigestBrand Digest Rival Germany PHEV SUV C",
          sourceDigestMatches: [
            {
              sourceId: "source-digest-rival",
              sourceFileName: "digest-rival-config.xlsx",
              groupCount: 1,
              trimCount: 2,
            },
            {
              sourceId: "source-digest-rival-site-b",
              sourceFileName: "digest-rival-site-b.xlsx",
              groupCount: 1,
              trimCount: 2,
            },
            {
              sourceId: "source-digest-rival-price",
              sourceFileName: "digest-rival-price-list.xlsx",
              groupCount: 2,
              trimCount: 4,
            },
            {
              sourceId: "source-digest-rival-site-c",
              sourceFileName: "digest-rival-site-c.xlsx",
              groupCount: 1,
              trimCount: 3,
            },
            {
              sourceId: "source-digest-rival-brochure",
              sourceFileName: "digest-rival-brochure.pdf",
              groupCount: 1,
              trimCount: 2,
            },
          ],
          nextAction: "create_from_source_digest",
          trims: [],
        },
        {
          rank: 3,
          sourceRank: 3,
          modelName: "Missing Rival",
          brand: "MissingBrand",
          profile: { segment: "SUV C", powertrain: "PHEV" },
          role: "adjacent",
          similarityScore: 0,
          salesTarget: 800,
          salesBase: 900,
          deltaVolume: -100,
          shareTarget: 0.08,
          shareChange: -0.01,
          pureShareShift: -40,
          estimatedFlow: 0,
          sharedDimensions: [],
          matchEvidence: [],
          recommendationReason: "同 segment / powertrain",
          configAvailable: false,
          configTrimCount: 0,
          nextAction: "upload_source",
          trims: [],
        },
      ],
      message: "ok",
      source: {
        type: "advanced_analysis_competitor_set",
        analysisMode: "profile",
        targetPeriod: "2026-05",
        basePeriod: "2025-05",
        scopeModelCount: 21,
        advancedAnalysisCountry: "德国",
        advancedAnalysisSegment: "SUV C及以上",
      },
    });
    const digestRivalGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "digest-rival-model",
      title: "Digest Rival",
      sourceSheet: "Digest Rival Sheet",
      modelName: "Digest Rival",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        country: "Germany",
        market: "Germany",
        modelName: "Digest Rival",
        profile: {
          ...(trim.profile ?? {}),
          brand: "DigestBrand",
          country: "Germany",
          powertrain: "PHEV",
          segment: "SUV C",
        },
        trimId: `digest-rival-${trim.trimId}`,
      })),
    };
    const digestRivalDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "digest-rival-config.xlsx",
      compareGroups: [digestRivalGroup],
    };
    const digestRivalSnapshotBase = buildSourceSnapshotFixture("source-digest-rival", "digest-rival-config.xlsx", digestRivalDigest);
    const digestRivalSnapshot: EngineeringConfigSourceSnapshot = {
      ...digestRivalSnapshotBase,
      relatedContext: {
        ...digestRivalSnapshotBase.relatedContext,
        brand: "DigestBrand",
        model: "Digest Rival",
        market: "Germany",
        country: "Germany",
        powertrain: "PHEV",
        segment: "SUV C",
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockImplementation(async (options) => {
      const query = typeof options === "object" && options !== null ? options.q : null;
      if (query === "digest-rival-config.xlsx") {
        return { rows: 1, items: [digestRivalSnapshot] };
      }
      return { rows: 0, items: [] };
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-digest-rival",
      groupId: "digest-rival-model",
      importBatchId: "draft-digest-rival",
      trimIds: ["draft-digest-basic", "draft-digest-premium"],
      compareTrimIds: ["draft-digest-basic", "draft-digest-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      aliasMatchedFeatureCount: 2,
      semanticAliasMatchedFeatureCount: 1,
      featureMatchReasonCounts: { alias: 1, semantic_alias: 1, created: 1 },
      featureMatchSamples: [
        {
          sourceFeatureName: "360 camera",
          matchedFeatureName: "360 round view camera / 360度高清全景影像",
          matchedFeatureCode: "digest_df9a8c8d2a1dc6e5",
          matchReason: "alias",
        },
      ],
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-digest-basic", "version-digest-premium"],
    });
    const appendedCompetitorCompare: CompareResponse = {
      trims: [
        {
          trimId: "rival-premium",
          fullTrimName: "Rival Premium AWD",
          brand: "RivalBrand",
          modelName: "Rival C-SUV",
          trimName: "Premium AWD",
          market: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "RIVAL-PREM",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "rival-luxury",
          fullTrimName: "Rival Luxury AWD",
          brand: "RivalBrand",
          modelName: "Rival C-SUV",
          trimName: "Luxury AWD",
          market: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "RIVAL-LUX",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-digest-basic",
          fullTrimName: "Digest Rival Basic",
          brand: "DigestBrand",
          modelName: "Digest Rival",
          trimName: "Basic",
          market: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "DIGEST-BASIC",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-digest-premium",
          fullTrimName: "Digest Rival Premium",
          brand: "DigestBrand",
          modelName: "Digest Rival",
          trimName: "Premium",
          market: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "DIGEST-PREM",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 0,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Safety"],
      },
      rows: [
        {
          category: "Safety",
          featureId: "feature-competitor-camera",
          featureCode: "competitor_camera",
          featureName: "Competitor camera",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["draft-digest-premium"],
          businessNote: "推荐竞品 Source Digest 建列后导出验证",
          values: ["-", "-", "-", "●"].map((rawValue, index) => ({
            valueId: `competitor-camera-${index}`,
            rawValue,
            normalizedValue: rawValue === "●" ? "standard" : "not_available",
            availability: rawValue === "●" ? "STANDARD" : "NOT_AVAILABLE",
            unit: null,
            displayValue: rawValue === "●" ? "标配" : "不配备",
            valueState: "marker_value",
            version: 1,
            inferred: false,
          })),
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockImplementation(async (ids) => {
      if (Array.isArray(ids) && ids.includes("draft-digest-basic")) {
        return appendedCompetitorCompare as unknown as Record<string, unknown>;
      }
      return {
        trims: [],
        rows: [],
        totalFeatures: 0,
        shownFeatures: 0,
      };
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?market=Germany&model=Target%20C-SUV&powertrain=PHEV&segment=SUV%20C"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );
    const hero = container.querySelector(".product-config-hero") as HTMLElement;

    expect(api.listEngineeringConfigCompetitorRecommendations).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    expect(api.listEngineeringConfigCompetitorRecommendations).not.toHaveBeenCalled();

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    openSimpleAdvancedSearch();

    await waitFor(() => {
      expect(vi.mocked(api.listEngineeringConfigCompetitorRecommendations).mock.calls.some(([params]) => (
        params.country === "Germany"
        && params.model_name === "Target C-SUV"
        && params.powertrain === "PHEV"
        && params.segment === "SUV C"
        && params.limit === 10
      ))).toBe(true);
    });

    const segmentInput = within(drawer).getByLabelText("Segment") as HTMLInputElement;
    expect(segmentInput.value).toBe("SUV C");
    let recommendations = within(drawer).getByText("高级分析推荐竞品").closest(".market-scan-field") as HTMLElement;
    const recommendationCoverage = await within(recommendations).findByLabelText("推荐竞品配置覆盖", undefined, { timeout: 3500 });
    expect(recommendationCoverage.textContent).toContain("Top 3/10");
    expect(recommendationCoverage.textContent).toContain("库内可用1");
    expect(recommendationCoverage.textContent).toContain("待生成1");
    expect(recommendationCoverage.textContent).toContain("待上传1");
    expect(recommendationCoverage.textContent).toContain("Germany · PHEV · SUV C");
    expect(recommendationCoverage.textContent).toContain("模式 profile · 2025-05 → 2026-05 · 样本 21 models · AA 德国 / SUV C及以上");
    const competitorBacklog = within(recommendations).getByLabelText("推荐竞品补齐队列");
    expect(competitorBacklog.textContent).toContain("优先处理 Source Digest 待生成");
    expect(competitorBacklog.textContent).toContain("Digest Rival");
    expect(competitorBacklog.textContent).toContain("库内可用1");
    expect(competitorBacklog.textContent).toContain("待生成1");
    expect(competitorBacklog.textContent).toContain("待上传1");
    fireEvent.click(within(competitorBacklog).getByRole("button", { name: "生成 Digest Rival 配置列" }));
    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    expect(screen.getByText("当前来源库查询：DigestBrand Digest Rival Germany PHEV SUV C；范围 国家 Germany · 动力 PHEV · Segment SUV C")).toBeTruthy();
    fireEvent.click(screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    recommendations = within(drawer).getByText("高级分析推荐竞品").closest(".market-scan-field") as HTMLElement;
    expect(await within(recommendations).findByText("Rival C-SUV")).toBeTruthy();
    expect(within(recommendations).getByLabelText("Rival C-SUV 推荐依据").textContent).toContain("Segment: same SUV C");
    const readyRivalMigration = within(recommendations).getByLabelText("Rival C-SUV 高级分析蝴蝶图迁移指标");
    expect(readyRivalMigration.textContent).toContain("AA 迁移指标");
    expect(readyRivalMigration.textContent).toContain("份额变化 +2.0%");
    expect(readyRivalMigration.textContent).toContain("纯份额迁移 +80");
    expect(readyRivalMigration.textContent).toContain("估算流向 +35");
    expect(within(recommendations).getByText("库内 2 配置列")).toBeTruthy();
    expect(within(recommendations).getByText("来源库 1 来源 · 1 组 · 2 配置列")).toBeTruthy();
    expect(within(recommendations).getByLabelText("Rival C-SUV 来源库检索词").textContent).toContain("RivalBrand Rival C-SUV Germany PHEV SUV C");
    const readyRivalSourceMatches = within(recommendations).getByLabelText("Rival C-SUV Source Digest 命中来源");
    expect(readyRivalSourceMatches.textContent).toContain("rival-ready-config.xlsx");
    expect(within(readyRivalSourceMatches).getByRole("button", { name: "按来源 rival-ready-config.xlsx 搜索 Source Digest" })).toBeTruthy();
    expect(within(recommendations).getByRole("button", { name: "核对 Rival C-SUV 来源" })).toBeTruthy();
    expect(within(recommendations).getByRole("button", { name: "加入库内配置列 2" })).toBeTruthy();
    expect(within(recommendations).getByText("Digest Rival")).toBeTruthy();
    expect(within(recommendations).getByLabelText("Digest Rival 推荐依据").textContent).toContain("同 Segment");
    expect(within(recommendations).getByText("来源库 1 组待生成")).toBeTruthy();
    expect(within(recommendations).getByText("来源库已有")).toBeTruthy();
    expect(within(recommendations).getByText("Digest Rival 有 1 个 Source Digest 可比组")).toBeTruthy();
    expect(within(recommendations).getByText("先打开来源库检索，创建 2 个候选配置列后再加入对比。")).toBeTruthy();
    expect(within(recommendations).getByLabelText("Digest Rival 来源库检索词").textContent).toContain("DigestBrand Digest Rival Germany PHEV SUV C");
    const digestRivalSourceMatches = within(recommendations).getByLabelText("Digest Rival Source Digest 命中来源");
    expect(digestRivalSourceMatches.textContent).toContain("digest-rival-config.xlsx");
    expect(digestRivalSourceMatches.textContent).toContain("1 组 · 2 配置列");
    expect(digestRivalSourceMatches.textContent).toContain("digest-rival-site-b.xlsx");
    expect(digestRivalSourceMatches.textContent).toContain("digest-rival-price-list.xlsx");
    expect(digestRivalSourceMatches.textContent).toContain("+2 个来源 · 查看全部");
    fireEvent.click(within(digestRivalSourceMatches).getByRole("button", { name: "查看全部 2 个 Digest Rival 命中来源" }));
    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    let contextSummary = screen.getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("DigestBrand · Digest Rival · Germany · PHEV · SUV C · 推荐竞品来源已入库 · 身份锚点 品牌 / 车型 / 市场");
    let digestSearchInput = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(digestSearchInput.value).toBe("DigestBrand Digest Rival Germany PHEV SUV C");
    expect((screen.getByLabelText("来源组详情浏览") as HTMLDetailsElement).open).toBe(true);
    expect(screen.queryByText(/当前只看来源/)).toBeNull();
    expect(screen.getByText("当前来源库查询：DigestBrand Digest Rival Germany PHEV SUV C；范围 国家 Germany · 动力 PHEV · Segment SUV C")).toBeTruthy();
    expect(hero.textContent).toContain("筛选 Target C-SUV · Germany · PHEV · +1");
    expect(hero.textContent).not.toContain("筛选 Digest Rival");
    fireEvent.click(screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    const recommendationsAfterAllSource = within(drawer).getByText("高级分析推荐竞品").closest(".market-scan-field") as HTMLElement;
    expect(await within(recommendationsAfterAllSource).findByRole("button", { name: "打开 Digest Rival Source Digest" })).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByText("Missing Rival")).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByLabelText("Missing Rival 推荐依据").textContent).toContain("AA 原始排序 #3");
    const missingRivalMigration = within(recommendationsAfterAllSource).getByLabelText("Missing Rival 高级分析蝴蝶图迁移指标");
    expect(missingRivalMigration.textContent).toContain("份额变化 -1.0%");
    expect(missingRivalMigration.textContent).toContain("纯份额迁移 -40");
    expect(missingRivalMigration.textContent).toContain("估算流向 0");
    expect(within(recommendationsAfterAllSource).getByText(/AA 排序/)).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByText("库内缺失")).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByText("配置资料缺口")).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByText("Missing Rival 暂无库内配置列")).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByText("先搜索来源库，未命中再上传 Germany / PHEV 的配置表或价格单；Digest 后可转成可编辑配置列。")).toBeTruthy();
    expect(within(recommendationsAfterAllSource).getByLabelText("Missing Rival 来源库检索词").textContent).toContain("MissingBrand Missing Rival Germany PHEV SUV C");

    fireEvent.click(within(recommendationsAfterAllSource).getByRole("button", { name: "加入库内配置列 2" }));
    expect(within(recommendationsAfterAllSource).getByRole("button", { name: /RIVAL-PREM/ }).textContent).toContain("移除");
    expect(within(recommendationsAfterAllSource).getByRole("button", { name: /RIVAL-LUX/ }).textContent).toContain("移除");
    expect((within(recommendationsAfterAllSource).getByRole("button", { name: "已全部加入" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(within(recommendationsAfterAllSource).getByRole("button", { name: "搜索 / 上传 Missing Rival 来源" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    contextSummary = screen.getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("MissingBrand · Missing Rival · Germany · PHEV · SUV C · 推荐竞品缺口 · 身份锚点 品牌 / 车型 / 市场");
    expect(contextSummary?.textContent).toContain("这是推荐竞品资料缺口：先上传该国家 / 动力 / segment 的配置表或价格单，Digest 成功后创建可编辑配置列并加入当前对比。");
    digestSearchInput = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(digestSearchInput.value).toBe("MissingBrand Missing Rival Germany PHEV SUV C");
    expect((screen.getByLabelText("来源组详情浏览") as HTMLDetailsElement).open).toBe(true);
    expect(screen.queryByText(/当前只看来源/)).toBeNull();
    expect(screen.getByText("当前来源库查询：MissingBrand Missing Rival Germany PHEV SUV C；范围 国家 Germany · 动力 PHEV · Segment SUV C")).toBeTruthy();
    expect(hero.textContent).toContain("筛选 Target C-SUV · Germany · PHEV · +1");
    expect(hero.textContent).not.toContain("筛选 Missing Rival");
    expect(screen.getByText("来源归档")).toBeTruthy();
    expect(screen.queryByText(/未绑定国家时仍可生成 Source Digest|上传后进入共享来源库/)).toBeNull();
    fireEvent.click(screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    const recommendationsAfterMissingSource = within(drawer).getByText("高级分析推荐竞品").closest(".market-scan-field") as HTMLElement;
    const digestRivalSourceMatchesAfterMissing = await within(recommendationsAfterMissingSource).findByLabelText("Digest Rival Source Digest 命中来源");
    fireEvent.click(within(digestRivalSourceMatchesAfterMissing).getByRole("button", { name: "按来源 digest-rival-config.xlsx 搜索 Source Digest" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    contextSummary = screen.getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("DigestBrand · Digest Rival · Germany · PHEV · SUV C · 推荐竞品来源已入库 · 身份锚点 品牌 / 车型 / 市场");
    digestSearchInput = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(digestSearchInput.value).toBe("digest-rival-config.xlsx");
    expect((screen.getByLabelText("来源组详情浏览") as HTMLDetailsElement).open).toBe(true);
    expect(screen.getByText("当前来源库查询：digest-rival-config.xlsx；范围 国家 Germany · 动力 PHEV · Segment SUV C")).toBeTruthy();
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        country: "Germany",
        q: "digest-rival-config.xlsx",
      }));
    });
    const focusedSource = await screen.findByLabelText("当前 Source Digest 来源聚焦");
    expect(focusedSource.textContent).toContain("当前只看来源");
    expect(focusedSource.textContent).toContain("digest-rival-config.xlsx");

    fireEvent.click(await screen.findByRole("button", { name: /选择 Source Digest 可比组：Digest Rival/ }));
    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-digest-rival", "digest-rival-model");
    });
    await waitFor(() => {
      expect(vi.mocked(api.compareEngineeringConfigTrims).mock.calls.some(([ids, onlyDifferences]) => (
        Array.isArray(ids)
        && ids.join("|") === "rival-premium|rival-luxury|draft-digest-basic|draft-digest-premium"
        && onlyDifferences === false
      ))).toBe(true);
    });
    expect(screen.getByText(/已追加到当前推荐竞品对比/)).toBeTruthy();
    const digestDraftSuccess = screen.getByLabelText("来源建列成功");
    await waitFor(() => {
      expect((within(digestDraftSuccess).getByRole("button", { name: "导出建列结果 XLSX" }) as HTMLButtonElement).disabled).toBe(false);
      expect((within(digestDraftSuccess).getByRole("button", { name: "导出建列结果 PDF" }) as HTMLButtonElement).disabled).toBe(false);
    });
    fireEvent.click(within(digestDraftSuccess).getByRole("button", { name: "导出建列结果 XLSX" }));
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalled();
    });
    fireEvent.click(within(digestDraftSuccess).getByRole("button", { name: "导出建列结果 PDF" }));
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalled();
    });

    fireEvent.click(screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    const recommendationsAfterSourceMatch = within(drawer).getByText("高级分析推荐竞品").closest(".market-scan-field") as HTMLElement;
    fireEvent.click(
      await within(recommendationsAfterSourceMatch).findByRole("button", { name: "打开 Digest Rival Source Digest" }),
    );

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    contextSummary = screen.getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("DigestBrand · Digest Rival · Germany · PHEV · SUV C · 推荐竞品来源已入库 · 身份锚点 品牌 / 车型 / 市场");
    expect(contextSummary?.textContent).toContain("优先用已入库 Source Digest 创建可编辑配置列，创建后回到 FloatingDeck 加入当前对比");
    expect(contextSummary?.textContent).not.toContain("已选 2 配置列");
    digestSearchInput = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(digestSearchInput.value).toBe("DigestBrand Digest Rival Germany PHEV SUV C");
    expect((screen.getByLabelText("来源组详情浏览") as HTMLDetailsElement).open).toBe(true);
    expect(screen.queryByText(/当前只看来源/)).toBeNull();
    expect(screen.getByText("当前来源库查询：DigestBrand Digest Rival Germany PHEV SUV C；范围 国家 Germany · 动力 PHEV · Segment SUV C")).toBeTruthy();
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        country: "Germany",
        segment: "SUV C",
        q: "DigestBrand Digest Rival Germany PHEV SUV C",
      }));
    });
  });

  it("keeps uploaded missing-competitor source candidates scoped to the recommendation context", async () => {
    vi.mocked(api.listEngineeringConfigCompetitorRecommendations).mockResolvedValue({
      country: "Germany",
      modelName: "Target C-SUV",
      powertrain: "PHEV",
      segment: "SUV C",
      rows: 1,
      items: [
        {
          rank: 1,
          sourceRank: 1,
          modelName: "Missing Rival",
          brand: "MissingBrand",
          profile: { segment: "SUV C", powertrain: "PHEV" },
          role: "adjacent",
          similarityScore: 72,
          salesTarget: 800,
          salesBase: 900,
          deltaVolume: -100,
          shareTarget: 0.08,
          shareChange: -0.01,
          pureShareShift: -40,
          estimatedFlow: 0,
          sharedDimensions: ["segment", "powertrain"],
          matchEvidence: [],
          recommendationReason: "同 segment / powertrain",
          configAvailable: false,
          configTrimCount: 0,
          nextAction: "upload_source",
          trims: [],
        },
      ],
      message: "ok",
      source: {
        type: "advanced_analysis_competitor_set",
        analysisMode: "profile",
        targetPeriod: "2026-05",
        basePeriod: "2025-05",
        scopeModelCount: 21,
        advancedAnalysisCountry: "德国",
        advancedAnalysisSegment: "SUV C及以上",
      },
    });
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 0, items: [] });
    const missingRivalGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "missing-rival-model",
      title: "Missing Rival",
      sourceSheet: "Missing Rival Sheet",
      modelName: "Missing Rival",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        country: "Germany",
        market: "Germany",
        modelName: "Missing Rival",
        profile: {
          ...(trim.profile ?? {}),
          brand: "MissingBrand",
          country: "Germany",
          powertrain: "PHEV",
          segment: "SUV C",
        },
        trimId: `missing-rival-${trim.trimId}`,
      })),
    };
    const missingRivalDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "missing-rival-config.xlsx",
      compareGroups: [missingRivalGroup],
    };
    const uploadedMissingSource: EngineeringConfigSourceSnapshot & { parseMode: string; message: string } = {
      ...buildSourceSnapshotFixture("source-missing-rival", "missing-rival-config.xlsx", missingRivalDigest),
      parseMode: "source_snapshot",
      message: "Source digest ready.",
      relatedContext: {
        brand: "MissingBrand",
        model: "Missing Rival",
        market: "Germany",
        country: "Germany",
        powertrain: "PHEV",
        segment: "SUV C",
        modelYear: null,
        trimIds: [],
        salesVersionIds: [],
        contextType: "competitor_recommendation_upload",
        scenario: "recommended_competitor_config_gap",
        identityAnchor: "brand_model_market",
      },
    };
    vi.mocked(api.completeEngineeringConfigSourceUpload).mockResolvedValueOnce(uploadedMissingSource);
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-missing-rival",
      groupId: "missing-rival-model",
      importBatchId: "draft-missing-rival",
      trimIds: ["draft-missing-basic", "draft-missing-premium"],
      compareTrimIds: ["draft-missing-basic", "draft-missing-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      aliasMatchedFeatureCount: 2,
      semanticAliasMatchedFeatureCount: 1,
      featureMatchReasonCounts: { alias: 1, semantic_alias: 1, created: 1 },
      featureMatchSamples: [
        {
          sourceFeatureName: "360 camera",
          matchedFeatureName: "360 round view camera / 360度高清全景影像",
          matchedFeatureCode: "digest_df9a8c8d2a1dc6e5",
          matchReason: "alias",
        },
      ],
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-missing-basic", "version-missing-premium"],
    });
    const missingCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-missing-basic",
          fullTrimName: "Missing Rival Basic",
          brand: "MissingBrand",
          modelName: "Missing Rival",
          trimName: "Basic",
          market: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "MISSING-BASIC",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-missing-premium",
          fullTrimName: "Missing Rival Premium",
          brand: "MissingBrand",
          modelName: "Missing Rival",
          trimName: "Premium",
          market: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "MISSING-PREM",
          msrp: null,
          targetPrice: null,
        },
      ],
      rows: digest.compareGroups[0].rows,
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(missingCompare) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?market=Germany&model=Target%20C-SUV&powertrain=PHEV&segment=SUV%20C"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    openSimpleAdvancedSearch();
    const recommendations = await within(drawer).findByText("高级分析推荐竞品");
    const recommendationPanel = recommendations.closest(".market-scan-field") as HTMLElement;
    fireEvent.click(await within(recommendationPanel).findByRole("button", { name: "搜索 / 上传 Missing Rival 来源" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    const sourceSearchInput = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(sourceSearchInput.value).toBe("MissingBrand Missing Rival Germany PHEV SUV C");
    const hero = container.querySelector(".product-config-hero") as HTMLElement;
    expect(hero.textContent).toContain("筛选 Target C-SUV · Germany · PHEV · +1");
    expect(hero.textContent).not.toContain("筛选 Missing Rival");

    const fileInput = await waitFor(() => {
      const input = container.querySelector<HTMLInputElement>(".deck-floating-panel input[type='file']");
      if (!input) throw new Error("Source upload input not mounted");
      return input;
    });
    const file = new File([new Uint8Array([80, 75, 3, 4])], "missing-rival-config.xlsx", {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    fireEvent.change(fileInput, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));

    await waitFor(() => {
      expect(api.completeEngineeringConfigSourceUpload).toHaveBeenCalledWith("upload-1", expect.objectContaining({
        brand: "MissingBrand",
        model: "Missing Rival",
        market: "Germany",
        country: "Germany",
        powertrain: "PHEV",
        segment: "SUV C",
        contextType: "competitor_recommendation_upload",
        scenario: "recommended_competitor_config_gap",
      }));
    });
    const candidateButton = await screen.findByRole("button", { name: /选择 Source Digest 可比组：Missing Rival/ });
    expect(candidateButton.textContent).toContain("来源库");
    expect(candidateButton.textContent).toContain("missing-rival-config.xlsx");
    await waitFor(() => {
      expect((candidateButton as HTMLButtonElement).disabled).toBe(false);
    });
    expect(hero.textContent).toContain("筛选 Target C-SUV · Germany · PHEV · +1");
    expect(hero.textContent).not.toContain("筛选 Missing Rival");

    fireEvent.click(candidateButton);
    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-missing-rival", "missing-rival-model");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-missing-basic", "draft-missing-premium"], false, "latest");
    });
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
  });

  it("exports the current visible config table as xlsx and pdf", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValue({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "Premium",
          headline: "Premium 相比 Basic 的主要升级集中在泊车辅助。",
          mainUpgrades: ["泊车辅助：倒车影像升级为 360 全景影像"],
          replacementsOrReductions: ["手动折叠后视镜被电动折叠替代"],
          evidenceStatus: ["1 项来自规则推断，不是 Excel 原文"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "premium:ADDED:camera_360",
            },
          ],
          recommendedUse: "适合导出给业务侧预览，引用前核对 evidence。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 80,
        completionTokens: 40,
        totalTokens: 120,
      },
    });
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(await screen.findByText("Premium 相比 Basic 的主要升级集中在泊车辅助。", undefined, { timeout: 3500 })).toBeTruthy();
    const aiPreview = screen.getByLabelText("AI 结论和证据：Premium");
    expect(within(aiPreview).getByText("展开 AI 要点")).toBeTruthy();
    expect(within(aiPreview).getByText("证据提示：含规则推断，引用前核对来源证据。")).toBeTruthy();
    const aiDetails = aiPreview.querySelector("details") as HTMLDetailsElement | null;
    expect(aiDetails?.open).toBe(false);
    fireEvent.click(within(aiPreview).getByText("展开 AI 要点"));
    expect(aiDetails?.open).toBe(true);
    expect(within(aiPreview).getAllByText("泊车辅助：倒车影像升级为 360 全景影像")).toHaveLength(1);
    expect(within(aiPreview).getByText("手动折叠后视镜被电动折叠替代")).toBeTruthy();
    expect(within(aiPreview).queryByText("1 项来自规则推断，不是 Excel 原文")).toBeNull();
    expect(within(aiPreview).queryByLabelText("Premium AI 证据边界")).toBeNull();
    expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
    expect(screen.queryByLabelText("Excel 首屏速读")).toBeNull();
    expect(screen.queryByLabelText("版本差异速读")).toBeNull();
    expect(screen.queryByLabelText("目标配置列结论抽屉")).toBeNull();
    const quickCopyButton = screen.getByRole("button", { name: "复制当前 AI 摘要" });
    expect(quickCopyButton.textContent).toBe("复制");
    fireEvent.click(quickCopyButton);
    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });
    expect(writeText.mock.calls[0][0]).toContain("Premium 相比 Basic 的主要升级集中在泊车辅助。");
    expect(await screen.findByText("AI 摘要已复制。")).toBeTruthy();
    const aiSummaryPanel = screen.getByLabelText("AI 配置对比摘要");
    expect(screen.getByLabelText("AI 摘要操作")).toBeTruthy();
    expect(within(aiSummaryPanel).queryByRole("button", { name: "重新生成" })).toBeNull();
    expect(screen.queryByLabelText("配置表操作")).toBeNull();
    await copyCurrentRangeFromFloatingDeck();
    const exportControl = screen.getByLabelText("配置对比导出控制");
    const aiExportScope = within(exportControl).getByLabelText("AI 结论导出口径");
    expect(aiExportScope.textContent).toContain("deepseek / deepseek-chat");
    expect(aiExportScope.textContent).toContain("按当前表格运行时生成");
    expect(aiExportScope.textContent).toContain("不回写来源解析记录");
    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(2);
    });
    expect(writeText.mock.calls[1][0]).toContain("配置项\t大类\t差异类型");
    await exportCurrentRangeFromFloatingDeck("xlsx");
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const xlsxPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(xlsxPayload).toEqual({
      trimIds: ["basic", "premium"],
      baseTrimId: "basic",
      versionScope: "published",
      factSource: {
        kind: "local_workbook_digest",
        fileName: "compare-sample.xlsx",
        groupId: "t19c",
      },
      filters: {
        deltaFilter: "ALL",
        category: null,
        search: null,
        targetTrimId: null,
        includeBusinessSummary: true,
      },
    });

    await exportCurrentRangeFromFloatingDeck("pdf");
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const pdfPayload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0][0];
    expect(pdfPayload).toEqual(xlsxPayload);
    expect(URL.createObjectURL).toHaveBeenCalledTimes(2);
  });

  it("exposes current table xlsx and pdf export actions in the FloatingDeck display panel", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    expect(screen.getByLabelText("FloatingDeck 当前表格范围：全部配置行")).toBeTruthy();

    const exportControl = screen.getByLabelText("配置对比导出控制");
    expect(within(exportControl).getByText("导出当前表格")).toBeTruthy();
    expect(exportControl.textContent).toContain("3 行 · 2 列");
    expect(
      exportControl.textContent?.includes("表格导出动作正在准备")
        || exportControl.textContent?.includes("导出范围跟当前表格一致：全部配置行"),
    ).toBe(true);
    await waitFor(() => {
      expect(exportControl.textContent).toContain("导出范围跟当前表格一致：全部配置行");
      expect((within(exportControl).getByRole("button", { name: "导出当前范围 XLSX" }) as HTMLButtonElement).disabled).toBe(false);
    });

    fireEvent.click(within(exportControl).getByRole("button", { name: "导出当前范围 XLSX" }));
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const xlsxPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(xlsxPayload).toEqual({
      trimIds: ["basic", "premium"],
      baseTrimId: "basic",
      versionScope: "published",
      factSource: {
        kind: "local_workbook_digest",
        fileName: "compare-sample.xlsx",
        groupId: "t19c",
      },
      filters: {
        deltaFilter: "ALL",
        category: null,
        search: null,
        targetTrimId: null,
        includeBusinessSummary: false,
      },
    });

    fireEvent.click(within(exportControl).getByRole("button", { name: "导出当前范围 PDF" }));
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const pdfPayload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0][0];
    expect(pdfPayload).toEqual(xlsxPayload);
  });

  it("lets editors import a feature mapping table from the FloatingDeck display panel", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));

    const mappingControl = screen.getByLabelText("字段映射表导入");
    expect(mappingControl.textContent).toContain("导入配置字段映射表");
    expect(mappingControl.textContent).toContain("只导入已审核映射");
    expect(mappingControl.textContent).toContain("不会直接修改当前配置值");

    const file = new File(
      [new Uint8Array([80, 75, 3, 4])],
      "配置字段映射表.xlsx",
      { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" },
    );
    fireEvent.change(within(mappingControl).getByLabelText("选择字段映射表"), {
      target: { files: [file] },
    });
    fireEvent.click(within(mappingControl).getByRole("button", { name: "导入字段映射表" }));

    await waitFor(() => {
      expect(api.completeEngineeringConfigFeatureCatalogUpload).toHaveBeenCalledWith("feature-upload-1");
    });
    expect(api.initiateEngineeringConfigFeatureCatalogUpload).toHaveBeenCalledWith(
      "配置字段映射表.xlsx",
      file.size,
      5 * 1024 * 1024,
    );
    expect(api.uploadEngineeringConfigFeatureCatalogChunk).toHaveBeenCalledWith(
      "feature-upload-1",
      0,
      expect.any(Blob),
    );
    expect(mappingControl.textContent).toContain("字段映射已导入：更新 7 项，新增 2 项。");
    expect(mappingControl.textContent).toContain("总字段");
    expect(mappingControl.textContent).toContain("12");
    expect(mappingControl.textContent).toContain("提示");
    expect(mappingControl.textContent).toContain("1");
    expect(mappingControl.textContent).toContain("大类：驾驶辅助 Drive assist / 舒适便利 Comfort&Convenient");
    expect(mappingControl.textContent).toContain("新别名会用于后续 Source Digest 建列");
    expect(mappingControl.textContent).toContain("已建列配置如需应用新映射，请重新从来源生成配置列");
    expect(mappingControl.textContent).toContain("导入审计");
    expect(mappingControl.textContent).toContain("Upload feature-upload-1");
    expect(mappingControl.textContent).toContain("editor-user / editor");
    expect(mappingControl.textContent).toContain("eng_config_uploads/feature-upload-1/session.json");
    expect(mappingControl.textContent).toContain("第 8 行缺少标准字段英文名，已跳过 aliases 更新。");

    fireEvent.click(within(mappingControl).getByRole("button", { name: "复制审计摘要" }));
    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });
    const copiedAudit = writeText.mock.calls[0][0];
    expect(copiedAudit).toContain("Feature Catalog Mapping Import Audit");
    expect(copiedAudit).toContain("Upload ID: feature-upload-1");
    expect(copiedAudit).toContain("File: 配置字段映射表.xlsx");
    expect(copiedAudit).toContain("Updated: 7");
    expect(copiedAudit).toContain("Warning details:");
    expect(await screen.findByText("字段映射审计摘要已复制。")).toBeTruthy();

    fireEvent.click(within(mappingControl).getByRole("button", { name: "去 Source Digest 重新建列" }));
    expect(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }).getAttribute("aria-selected")).toBe("true");
    const sourceSearchInput = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(sourceSearchInput.value).toContain("T19C MY ICE");
    const sourceMappingStatus = screen.getByLabelText("Source Digest 字段映射状态");
    expect(sourceMappingStatus.textContent).toContain("字段映射已更新");
    expect(sourceMappingStatus.textContent).toContain("更新 7 项 · 新增 2 项");
    expect(sourceMappingStatus.textContent).toContain("重新从来源生成配置列后，新别名才会进入跨来源匹配");
    fireEvent.click(screen.getByRole("button", { name: "清空 搜索来源 / 车型 / 配置列" }));
    await waitFor(() => {
      expect(screen.getAllByText(/字段映射待应用 · 建列时按 FeatureCatalog 别名归并/).length).toBeGreaterThan(0);
    });
    expect(await screen.findByText("字段映射已更新；请重新从来源生成配置列以应用新别名。")).toBeTruthy();
    fireEvent.click(within(sourceMappingStatus).getByRole("button", { name: "查看字段映射审计" }));
    expect(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }).getAttribute("aria-selected")).toBe("true");
    expect(screen.getByLabelText("字段映射导入审计")).toBeTruthy();
  });

  it("clears stale AI summary from export payload when the table scope changes before regeneration finishes", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary)
      .mockResolvedValueOnce({
        summaries: [
          {
            targetTrimId: "premium",
            targetLabel: "Premium",
            headline: "旧口径 AI 摘要不应进入新导出。",
            mainUpgrades: ["旧口径升级"],
            replacementsOrReductions: [],
            evidenceStatus: ["旧口径证据边界"],
            recommendedUse: "旧口径使用建议。",
          },
        ],
        usage: {
          provider: "deepseek",
          model: "deepseek-chat",
          status: "ok",
          promptTokens: 80,
          completionTokens: 40,
          totalTokens: 120,
        },
      })
      .mockReturnValue(new Promise(() => undefined));

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("旧口径 AI 摘要不应进入新导出。", undefined, { timeout: 2500 })).toBeTruthy();
    fireEvent.click(tableDeltaFilterButton(/差异行/));
    await waitFor(() => {
      expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    });

    await exportCurrentRangeFromFloatingDeck("xlsx");
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const xlsxPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(xlsxPayload.filters).toEqual(expect.objectContaining({
      deltaFilter: "DIFFERENCE",
      includeBusinessSummary: false,
    }));
    expect(xlsxPayload).not.toHaveProperty("businessSummary");
  });

  it("exports the currently scoped difference rows instead of the whole table", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(tableDeltaFilterButton(/差异行/));
    await waitFor(() => {
      expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    });

    await exportCurrentRangeFromFloatingDeck("xlsx");
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const xlsxPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(xlsxPayload.filters).toEqual(expect.objectContaining({
      deltaFilter: "DIFFERENCE",
      includeBusinessSummary: false,
    }));
    expect(xlsxPayload).not.toHaveProperty("rows");

    await exportCurrentRangeFromFloatingDeck("pdf");
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const pdfPayload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0][0];
    expect(pdfPayload).toEqual(xlsxPayload);
  });

  it("defers the local workbook digest sample until the browser is idle", async () => {
    const originalRequestIdleCallback = window.requestIdleCallback;
    const originalCancelIdleCallback = window.cancelIdleCallback;
    const idleCallbacks: Array<() => void> = [];
    const requestIdleCallback = vi.fn((callback: () => void) => {
      idleCallbacks.push(callback);
      return 1;
    });
    const cancelIdleCallback = vi.fn();
    Object.defineProperty(window, "requestIdleCallback", { configurable: true, value: requestIdleCallback });
    Object.defineProperty(window, "cancelIdleCallback", { configurable: true, value: cancelIdleCallback });

    try {
      const { container } = render(
        <MemoryRouter>
          <ProductConfigComparePage />
        </MemoryRouter>,
      );

      expect(screen.getByRole("button", { name: "查看本地 xlsx 样例" })).toBeTruthy();
      expect(requestIdleCallback).not.toHaveBeenCalled();
      expect(api.getEngineeringConfigLocalWorkbookDigest).not.toHaveBeenCalled();

      fireEvent.click(screen.getByRole("button", { name: "查看本地 xlsx 样例" }));

      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(1);
      });
      expect(screen.getByText("正在准备本地 xlsx 样例")).toBeTruthy();
      expect(screen.getByText("页面先加载配置列库；样例 digest 会在首屏稳定后拉取，不阻塞 FloatingDeck 搜索和上传。")).toBeTruthy();
      expect(api.getEngineeringConfigLocalWorkbookDigest).not.toHaveBeenCalled();

      idleCallbacks[0]?.();

      await waitFor(() => {
        expect(api.getEngineeringConfigLocalWorkbookDigest).toHaveBeenCalledWith("欧盟在售车型可控资源表20260226.xlsx");
      });
      expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

      fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
      const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
      const selectedPreview = within(drawer).getByLabelText("当前已选配置列");
      expect(selectedPreview.textContent).toContain("已选配置列 2/4");
      expect(selectedPreview.textContent).toContain("Basic");
      expect(selectedPreview.textContent).toContain("Premium");
      expect(selectedPreview.textContent).toContain("本品资料");
      expect(within(selectedPreview).queryByRole("button", { name: /移除 Basic/ })).toBeNull();
    } finally {
      Object.defineProperty(window, "requestIdleCallback", { configurable: true, value: originalRequestIdleCallback });
      Object.defineProperty(window, "cancelIdleCallback", { configurable: true, value: originalCancelIdleCallback });
    }
  });

  it("defers the simple AI summary panel until after the compare table yields to idle", async () => {
    const originalRequestIdleCallback = window.requestIdleCallback;
    const originalCancelIdleCallback = window.cancelIdleCallback;
    const idleCallbacks: Array<() => void> = [];
    const requestIdleCallback = vi.fn((callback: () => void) => {
      idleCallbacks.push(callback);
      return idleCallbacks.length;
    });
    const cancelIdleCallback = vi.fn();
    Object.defineProperty(window, "requestIdleCallback", { configurable: true, value: requestIdleCallback });
    Object.defineProperty(window, "cancelIdleCallback", { configurable: true, value: cancelIdleCallback });
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "Premium",
          headline: "Premium 相比 Basic 的摘要延后到首屏之后生成。",
          mainUpgrades: ["泊车辅助升级"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用前核对 evidence"],
          recommendedUse: "适合先看配置表，再看 AI 结论。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 60,
        completionTokens: 30,
        totalTokens: 90,
      },
    });

    function runNextIdleCallback(): void {
      const callback = idleCallbacks.shift();
      expect(callback).toBeTruthy();
      callback?.();
    }

    try {
      render(
        <MemoryRouter>
          <ProductConfigComparePage />
        </MemoryRouter>,
      );

      expect(screen.getByRole("button", { name: "查看本地 xlsx 样例" })).toBeTruthy();
      expect(requestIdleCallback).not.toHaveBeenCalled();
      expect(screen.queryByLabelText("AI 配置对比摘要")).toBeNull();
      expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();

      fireEvent.click(screen.getByRole("button", { name: "查看本地 xlsx 样例" }));
      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(1);
      });
      runNextIdleCallback();
      expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(2);
      });
      expect(screen.queryByLabelText("AI 配置对比摘要")).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
      fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
      const exportControl = screen.getByLabelText("配置对比导出控制");
      expect(exportControl.textContent).toContain("3 行 · 2 列");
      await waitFor(() => {
        expect(exportControl.textContent).toContain("导出范围跟当前表格一致：全部配置行");
      });
      expect(screen.queryByLabelText("AI 配置对比摘要")).toBeNull();
      expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();

      runNextIdleCallback();
      expect(await screen.findByLabelText("AI 配置对比摘要")).toBeTruthy();
      expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
      expect(screen.getByText("AI 摘要将在首屏稳定后自动生成；下方配置表可以先查看。")).toBeTruthy();
      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(3);
      });
      expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();

      runNextIdleCallback();
      expect(await screen.findByText("Premium 相比 Basic 的摘要延后到首屏之后生成。", undefined, { timeout: 3500 })).toBeTruthy();
      expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
      expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
    } finally {
      Object.defineProperty(window, "requestIdleCallback", { configurable: true, value: originalRequestIdleCallback });
      Object.defineProperty(window, "cancelIdleCallback", { configurable: true, value: originalCancelIdleCallback });
    }
  });

  it("does not keep the simple AI summary mounted while a formal compare refresh is pending", async () => {
    const formalCompare: CompareResponse = {
      trims: digest.compareGroups[0].trims.map((trim) => ({ ...trim, brand: "OMODA" })),
      rows: digest.compareGroups[0].rows,
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    let resolveRefreshCompare!: (value: CompareResponse) => void;
    const refreshComparePromise = new Promise<CompareResponse>((resolve) => {
      resolveRefreshCompare = resolve;
    });
    vi.mocked(api.compareEngineeringConfigTrims)
      .mockResolvedValueOnce(formalCompare as unknown as Record<string, unknown>)
      .mockReturnValueOnce(refreshComparePromise as unknown as Promise<Record<string, unknown>>);

    const originalRequestIdleCallback = window.requestIdleCallback;
    const originalCancelIdleCallback = window.cancelIdleCallback;
    let nextIdleHandle = 1;
    const idleCallbacks = new Map<number, () => void>();
    const requestIdleCallback = vi.fn((callback: () => void) => {
      const handle = nextIdleHandle;
      nextIdleHandle += 1;
      idleCallbacks.set(handle, callback);
      return handle;
    });
    const cancelIdleCallback = vi.fn((handle: number) => {
      idleCallbacks.delete(handle);
    });
    Object.defineProperty(window, "requestIdleCallback", { configurable: true, value: requestIdleCallback });
    Object.defineProperty(window, "cancelIdleCallback", { configurable: true, value: cancelIdleCallback });

    function runOldestIdleCallback(): void {
      const entry = idleCallbacks.entries().next().value as [number, () => void] | undefined;
      expect(entry).toBeTruthy();
      if (!entry) return;
      const [handle, callback] = entry;
      idleCallbacks.delete(handle);
      callback();
    }

    try {
      render(
        <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic"]}>
          <ProductConfigComparePage />
        </MemoryRouter>,
      );

      expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(1);
      });
      runOldestIdleCallback();
      expect(await screen.findByLabelText("AI 配置对比摘要")).toBeTruthy();
      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(2);
      });
      expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();

      openFloatingDisplayPanel();
      fireEvent.click(screen.getByRole("button", { name: "刷新配置对比" }));

      await waitFor(() => {
        expect(api.compareEngineeringConfigTrims).toHaveBeenCalledTimes(2);
      });
      expect(screen.queryByLabelText("AI 配置对比摘要")).toBeNull();
      expect(screen.getByRole("button", { name: "刷新中..." })).toBeTruthy();
      expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();
      expect(cancelIdleCallback).toHaveBeenCalled();
      expect(idleCallbacks.size).toBe(0);

      resolveRefreshCompare(formalCompare);
      await waitFor(() => {
        expect(screen.getByRole("button", { name: "刷新配置对比" })).toBeTruthy();
      });
      await waitFor(() => {
        expect(requestIdleCallback).toHaveBeenCalledTimes(3);
      });
      runOldestIdleCallback();
      expect(await screen.findByLabelText("AI 配置对比摘要")).toBeTruthy();
      expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();
    } finally {
      Object.defineProperty(window, "requestIdleCallback", { configurable: true, value: originalRequestIdleCallback });
      Object.defineProperty(window, "cancelIdleCallback", { configurable: true, value: originalCancelIdleCallback });
    }
  });

  it("does not keep the simple AI pending bridge after summary generation fails", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "missing_key",
        fallbackReason: "Provider key missing",
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
      },
    });

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("AI 摘要暂不可用；配置表和来源证据仍可继续查看。", undefined, { timeout: 3000 })).toBeTruthy();
    expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
    expect(screen.getByLabelText("配置表范围状态").textContent).toContain("当前展示 3/3 行");
  });

  it("shows identity anchors for own catalog and external trims", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    expect(await screen.findByText("身份锚点 物料号 T71607V**MM0001")).toBeTruthy();
    expect(screen.getByText("身份锚点 Sales version Premium")).toBeTruthy();
    expect(screen.getAllByText("来源 compare-sample.xlsx").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("本品与外部配置列").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("无需切换本品 / 竞品模式；本品用物料号锚定，外部抓取对象用来源、车型、市场和 sales version 锚定。")).toBeTruthy();
    expect(screen.getByText("身份锚点混合")).toBeTruthy();
    expect(screen.getByText("本品通常有物料号；竞品 / 网页抓取对象可能只能用 sales version、车型、市场和来源锚定。这里只提示证据口径，不需要先选择本品或竞品模式。")).toBeTruthy();
    expect(screen.getByText("无物料号 1")).toBeTruthy();
    expect(screen.getByText("无物料号对象不会按 BOM 直接对齐，需要结合来源证据判断配置差异。")).toBeTruthy();
    expect(screen.getByText("缺源值 6，缺值 0，推断 0，合并格 0")).toBeTruthy();
  });

  it("keeps large local digest browse lists search-first with an explicit expand option", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValue(buildManyGroupDigest(12));

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    await waitFor(() => {
      expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    });
    expect(container.querySelector(".product-config-local-digest")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    openSimpleAdvancedSearch();
    const digestCandidates = within(drawer).getByText("Source Digest 可比组").closest(".market-scan-field") as HTMLElement;
    expect(within(digestCandidates).getByText("默认先预览 6/12 个可比组")).toBeTruthy();
    expect(within(digestCandidates).getByText("多来源入库后建议直接搜索车型 / 来源 / 上传人；需要浏览全量时再展开。")).toBeTruthy();
    let groupButtons = within(digestCandidates).getAllByRole("button", { name: /选择 Source Digest 可比组/ });
    expect(groupButtons).toHaveLength(6);
    expect(within(digestCandidates).queryByRole("button", { name: /Sample Group 12/ })).toBeNull();

    fireEvent.click(within(digestCandidates).getByRole("button", { name: "展开全部 12 个" }));
    groupButtons = within(digestCandidates).getAllByRole("button", { name: /选择 Source Digest 可比组/ });
    expect(groupButtons).toHaveLength(12);
    expect(groupButtons[11].textContent).toContain("Sample Group 12");

    fireEvent.click(groupButtons[11]);

    await waitFor(() => {
      expect(within(digestCandidates).getAllByRole("button", { name: /选择 Source Digest 可比组/ })[11].closest(".product-config-source-digest-card")?.classList.contains("is-selected")).toBe(true);
    });
    expect(screen.getByText(/本地样例 12 组 \/ 36 个可预览选项/)).toBeTruthy();
    expect(container.querySelector(".product-config-local-digest")).toBeNull();

    switchSummaryMode("expert");
    const expertDigestSwitcher = container.querySelector(".product-config-local-digest__switcher") as HTMLDetailsElement | null;
    expect(expertDigestSwitcher?.open).toBe(true);
    expect(screen.getByText("当前展开全部本地 xlsx 可比组")).toBeTruthy();
  });

  it("expands hidden Source Digest path preview into the full detail browser", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValue(buildManyGroupDigest(12));

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const pathPreview = within(drawer).getByLabelText("Source Digest 命中路径预览");
    expect(pathPreview.textContent).toContain("12 个来源路径");
    expect(pathPreview.textContent).toContain("路径卡已收起");
    expect(pathPreview.textContent).toContain("优先命中");
    expect(pathPreview.querySelectorAll(".product-config-source-path-preview__item")).toHaveLength(0);

    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    fireEvent.click(within(pathPreview).getByRole("button", { name: "展开全部路径" }));

    await waitFor(() => {
      expect(detailBrowser.open).toBe(true);
    });
    await waitFor(() => {
      expect(pathPreview.querySelectorAll(".product-config-source-path-preview__item")).toHaveLength(4);
      expect(pathPreview.textContent).toContain("聚焦来源");
      expect(pathPreview.textContent).toContain("聚焦车型");
    });
    const digestCandidates = within(detailBrowser).getByText("Source Digest 可比组").closest(".market-scan-field") as HTMLElement;
    const sourceBrowser = within(digestCandidates).getByLabelText("Source Digest 按来源和品牌浏览");
    const browserGroups = sourceBrowser.querySelectorAll(".product-config-source-digest-browser-group");
    expect(browserGroups).toHaveLength(12);
    expect(browserGroups[11]?.textContent).toContain("Sample Group 12");
  });

  it("infers Source Digest powertrain from model and trim text when structured fields are missing", async () => {
    const phevGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "t22-phev-text-only",
      title: "T22-PHEV（二阶段）",
      modelName: "T22-PHEV（二阶段）",
      sourceSheet: "T22-PHEV（二阶段）",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "T22-PHEV（二阶段）",
        trimName: trim.trimName.replace("两驱", "尊贵型-AWD "),
        fullTrimName: trim.fullTrimName.replace("两驱", "尊贵型-AWD "),
        profile: {
          ...(trim.profile ?? {}),
          powertrain: null,
          energyType: null,
          drivetrain: null,
          engine: null,
        },
      })),
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValue({
      ...digest,
      modelName: "T22-PHEV（二阶段）",
      compareGroups: [phevGroup],
      summary: {
        ...digest.summary,
        comparableGroupCount: 1,
      },
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    expect(within(drawer).queryByText("Source Digest 可比组")).toBeNull();
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    const phevCandidate = within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T22-PHEV/ });
    expect(phevCandidate.textContent).toContain("动力 PHEV");

    const digestSearchInput = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME });
    fireEvent.focus(digestSearchInput);
    expect(within(screen.getByRole("listbox")).getByText("PHEV")).toBeTruthy();
    fireEvent.change(digestSearchInput, { target: { value: "PHEV" } });

    await waitFor(() => {
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T22-PHEV/ })).toBeTruthy();
    });
  });

  it("surfaces OCR temporary column identity in the FloatingDeck source digest browser", async () => {
    const ocrHeaderlessGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "ocr-headerless-local",
      title: "OCR Headerless Model",
      sourceKind: "ocr_headerless",
      identityStatus: "temporary_ocr_column",
      identityNote: "OCR 未识别到配置列标题，已按列位置生成临时配置列。",
      sourceSheet: "OCR Image 1",
      modelName: "OCR Headerless Model",
      rows: digest.compareGroups[0].rows.map((row, index) => (
        index === 0
          ? {
              ...row,
              reviewFlags: ["ocr_possible_feature_text_in_value_cell"],
              reviewNotes: ["OCR 值单元格像配置项文本（seats），可能是特征名换行或单位被切入值列。"],
            }
          : row
      )),
      trims: digest.compareGroups[0].trims.map((trim, index) => ({
        ...trim,
        trimId: `ocr-column-${index + 1}`,
        trimName: `OCR Column ${index + 1}`,
        fullTrimName: `OCR Column ${index + 1} · 待补配置列身份`,
        modelName: "OCR Headerless Model",
        materialNo: null,
        hasMaterialNo: false,
        salesVersion: null,
        dataOrigin: "external_or_scraped",
        sourceSheet: "OCR Image 1",
        identityStatus: "temporary_ocr_column",
        identityNote: "OCR 未识别到配置列标题，当前列名为临时身份。",
      })),
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValue({
      ...digest,
      digestType: "image_ocr",
      sourceFormat: "image_ocr",
      ocrEngine: "paddleocr",
      fileName: "ocr-headerless.png",
      modelName: "OCR Headerless Model",
      summary: {
        ...digest.summary,
        candidateTrimCount: 2,
        comparableGroupCount: 1,
      },
      compareGroups: [ocrHeaderlessGroup],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    const reviewFilter = within(digestCandidates).getByRole("button", {
      name: "筛选 Source Digest：需核对 1 个",
    });
    expect(reviewFilter).toBeTruthy();
    fireEvent.click(reviewFilter);
    expect(within(digestCandidates).getByRole("button", {
      name: /选择 Source Digest 可比组：OCR Headerless Model/,
    })).toBeTruthy();
    const temporaryFilter = within(digestCandidates).getByRole("button", {
      name: "筛选 Source Digest：临时 OCR 列 1 个",
    });
    expect(temporaryFilter).toBeTruthy();
    fireEvent.click(temporaryFilter);

    const groupButton = within(digestCandidates).getByRole("button", {
      name: /选择 Source Digest 可比组：OCR Headerless Model/,
    });
    expect(groupButton.textContent).toContain("OCR 临时列");
    expect(groupButton.textContent).toContain("临时 OCR 列身份待补");
    expect(groupButton.textContent).toContain("OCR 临时列身份 2/2 列");
    expect(groupButton.textContent).toContain("创建前需补真实车型 / 配置列身份");
    expect(groupButton.textContent).not.toContain("创建后需");
    expect(groupButton.textContent).toContain("需核对 1 行 OCR 对齐");
    expect(groupButton.textContent).toContain("可能是特征名换行或单位被切入值列");
    expect(within(digestCandidates).getAllByText("OCR 未识别到配置列标题，当前列名为临时身份。").length).toBeGreaterThanOrEqual(2);

    const digestSearchInput = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME });
    fireEvent.focus(digestSearchInput);
    expect(within(drawer).getByRole("listbox").textContent).toContain("临时 OCR 列");
    expect(within(drawer).getByRole("listbox").textContent).toContain("需核对 1 行 OCR 对齐");
  });

  it("requires OCR temporary column identity mapping before creating source digest drafts", async () => {
    const ocrHeaderlessGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "ocr-headerless-library",
      title: "OCR Headerless Model",
      sourceKind: "ocr_headerless",
      identityStatus: "temporary_ocr_column",
      identityNote: "OCR 未识别到配置列标题，已按列位置生成临时配置列。",
      sourceSheet: "OCR Image 1",
      modelName: "OCR Headerless Model",
      rows: digest.compareGroups[0].rows.map((row, index) => {
        if (index === 0) {
          return {
            ...row,
            reviewFlags: ["ocr_possible_feature_text_in_value_cell"],
            reviewNotes: ["OCR 值单元格像配置项文本（seats），可能是特征名换行或单位被切入值列。"],
          };
        }
        if (index === 1) {
          return {
            ...row,
            featureCode: "ocr_headerless_selected_review",
            featureName: "OCR Headerless selected review",
            reviewFlags: ["ocr_low_confidence_feature_label"],
            reviewNotes: ["OCR 第二条风险行，创建后应该定位到这一行。"],
          };
        }
        return row;
      }),
      trims: digest.compareGroups[0].trims.map((trim, index) => ({
        ...trim,
        trimId: `ocr-library-column-${index + 1}`,
        trimName: `OCR Column ${index + 1}`,
        fullTrimName: `OCR Column ${index + 1} · 待补配置列身份`,
        modelName: "OCR Headerless Model",
        materialNo: null,
        hasMaterialNo: false,
        salesVersion: null,
        dataOrigin: "external_or_scraped",
        sourceSheet: "OCR Image 1",
        identityStatus: "temporary_ocr_column",
        identityNote: "OCR 未识别到配置列标题，当前列名为临时身份。",
      })),
    };
    const libraryDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "image_ocr",
      sourceFormat: "image_ocr",
      ocrEngine: "paddleocr",
      fileName: "ocr-headerless.png",
      modelName: "OCR Headerless Model",
      summary: {
        ...digest.summary,
        candidateTrimCount: 2,
        comparableGroupCount: 1,
      },
      compareGroups: [ocrHeaderlessGroup],
    };
    const listSnapshot = {
      ...buildSourceSnapshotFixture("source-ocr-headerless", "ocr-headerless.png", null),
      sourceDigestStatus: {
        digestType: "image_ocr",
        status: "ready",
        sourceFormat: "image_ocr",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    const detailSnapshot = buildSourceSnapshotFixture("source-ocr-headerless", "ocr-headerless.png", libraryDigest);
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 1, items: [listSnapshot] });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(detailSnapshot);
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-ocr-headerless",
      groupId: "ocr-headerless-library",
      importBatchId: "draft-ocr-headerless",
      trimIds: ["draft-ocr-comfort", "draft-ocr-premium"],
      compareTrimIds: ["draft-ocr-comfort", "draft-ocr-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["draft-ocr-comfort-version", "draft-ocr-premium-version"],
    });
    const formalOcrHeaderlessCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-ocr-comfort",
          fullTrimName: "Comfort-FWD",
          brand: "OCR Brand",
          modelName: "OCR Headerless Model",
          trimName: "Comfort-FWD",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Comfort-FWD",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-ocr-premium",
          fullTrimName: "Premium-FWD",
          brand: "OCR Brand",
          modelName: "OCR Headerless Model",
          trimName: "Premium-FWD",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Premium-FWD",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 0,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Safety"],
      },
      rows: [
        {
          category: "Safety",
          featureId: "feature-ocr-headerless-selected-review",
          featureCode: "ocr_headerless_selected_review",
          featureName: "OCR Headerless selected review",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["draft-ocr-premium"],
          businessNote: "OCR 第二条风险行，创建后应该定位到这一行。",
          values: [
            {
              valueId: "value-ocr-comfort-selected-review",
              rawValue: "-",
              normalizedValue: "not_available",
              availability: "NOT_AVAILABLE",
              unit: null,
              displayValue: "不配备",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "OCR Image 1",
                rowNumber: 18,
                columnNumber: 4,
                columnLetter: "D",
                cell: "D18",
                sourceCell: "D18",
                mergedRange: null,
                sourceType: "image_ocr",
                pageNumber: 1,
                ocrEngine: "paddleocr",
              },
            },
            {
              valueId: "value-ocr-premium-selected-review",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "OCR Image 1",
                rowNumber: 18,
                columnNumber: 5,
                columnLetter: "E",
                cell: "E18",
                sourceCell: "E18",
                mergedRange: null,
                sourceType: "image_ocr",
                pageNumber: 1,
                ocrEngine: "paddleocr",
              },
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalOcrHeaderlessCompare) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    fireEvent.change(within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "OCR Headerless" },
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    await waitFor(() => {
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("ocr-headerless.png");
    });
    expect(detailBrowser.open).toBe(false);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-ocr-headerless");
    });

    const sourceDigestPanel = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-ocr-headerless");
    });
    const groupButton = within(sourceDigestPanel).getByRole("button", {
      name: /选择 Source Digest 可比组：OCR Headerless Model/,
    }) as HTMLButtonElement;
    expect(groupButton.disabled).toBe(true);
    expect(within(sourceDigestPanel).getByText("OCR 临时列身份映射")).toBeTruthy();
    const reviewRows = within(sourceDigestPanel).getByLabelText("OCR Headerless Model 需核对配置行定位") as HTMLDetailsElement;
    fireEvent.click(within(reviewRows).getByText("需核对行 2"));
    fireEvent.click(within(reviewRows).getByRole("button", {
      name: "建列后定位此行：OCR Headerless selected review",
    }));
    expect(within(reviewRows).getByRole("button", {
      name: "已设为建列后定位：OCR Headerless selected review",
    }).getAttribute("aria-pressed")).toBe("true");

    fireEvent.change(within(sourceDigestPanel).getByLabelText("OCR Column 1 配置列"), {
      target: { value: "Comfort-FWD" },
    });
    fireEvent.change(within(sourceDigestPanel).getByLabelText("OCR Column 2 配置列"), {
      target: { value: "Premium-FWD" },
    });

    await waitFor(() => {
      expect(groupButton.disabled).toBe(false);
    });
    fireEvent.click(groupButton);

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith(
        "source-ocr-headerless",
        "ocr-headerless-library",
        {
          trimIdentityOverrides: [
            expect.objectContaining({
              trimId: "ocr-library-column-1",
              modelName: "OCR Headerless Model",
              trimName: "Comfort-FWD",
            }),
            expect.objectContaining({
              trimId: "ocr-library-column-2",
              modelName: "OCR Headerless Model",
              trimName: "Premium-FWD",
            }),
          ],
        },
      );
    });
    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-ocr-comfort", "draft-ocr-premium"], false, "latest");
    });
    expect(screen.getByText(/定位到需核对行：OCR Headerless selected review/)).toBeTruthy();
    const draftSuccess = screen.getByLabelText("来源建列成功");
    expect(within(draftSuccess).getByRole("button", { name: "跳到需核对行" })).toBeTruthy();
    await waitFor(() => {
      const focusedRow = document.getElementById("config-feature-ocr-headerless-selected-review");
      expect(focusedRow).toBeTruthy();
      expect(focusedRow?.classList.contains("compare-row-active")).toBe(true);
      expect(focusedRow?.getAttribute("aria-selected")).toBe("true");
    });
    vi.mocked(window.HTMLElement.prototype.scrollIntoView).mockClear();
    fireEvent.click(within(draftSuccess).getByRole("button", { name: "跳到需核对行" }));
    expect(window.HTMLElement.prototype.scrollIntoView).toHaveBeenCalled();
  });

  it("keeps duplicate review feature codes distinct in the FloatingDeck source digest picker", async () => {
    const duplicateReviewGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "duplicate-review-library",
      title: "Duplicate Review Model",
      sourceKind: "ocr_headerless",
      identityStatus: "temporary_ocr_column",
      identityNote: "OCR 未识别到配置列标题，已按列位置生成临时配置列。",
      sourceSheet: "OCR Image 1",
      modelName: "Duplicate Review Model",
      rows: digest.compareGroups[0].rows.slice(0, 2).map((row, index) => ({
        ...row,
        featureCode: "digest_duplicate_review_feature",
        featureName: index === 0 ? "Duplicate first review" : "Duplicate selected review",
        reviewFlags: [index === 0 ? "ocr_duplicate_first" : "ocr_duplicate_second"],
        reviewNotes: [index === 0 ? "第一条同 code 风险行。" : "第二条同 code 风险行。"],
      })),
      trims: digest.compareGroups[0].trims.map((trim, index) => ({
        ...trim,
        trimId: `duplicate-review-column-${index + 1}`,
        trimName: `OCR Column ${index + 1}`,
        fullTrimName: `OCR Column ${index + 1} · 待补配置列身份`,
        modelName: "Duplicate Review Model",
        materialNo: null,
        hasMaterialNo: false,
        salesVersion: null,
        dataOrigin: "external_or_scraped",
        sourceSheet: "OCR Image 1",
        identityStatus: "temporary_ocr_column",
      })),
    };
    const duplicateReviewDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "image_ocr",
      sourceFormat: "image_ocr",
      ocrEngine: "paddleocr",
      fileName: "duplicate-review.png",
      modelName: "Duplicate Review Model",
      summary: {
        ...digest.summary,
        candidateTrimCount: 2,
        comparableGroupCount: 1,
      },
      compareGroups: [duplicateReviewGroup],
    };
    const listSnapshot = {
      ...buildSourceSnapshotFixture("source-duplicate-review", "duplicate-review.png", null),
      sourceDigestStatus: {
        digestType: "image_ocr",
        status: "ready",
        sourceFormat: "image_ocr",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 2,
          differenceCount: 2,
        },
      },
    };
    const detailSnapshot = buildSourceSnapshotFixture("source-duplicate-review", "duplicate-review.png", duplicateReviewDigest);
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 1, items: [listSnapshot] });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(detailSnapshot);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    fireEvent.change(within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "Duplicate Review" },
    });
    await waitFor(() => {
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("duplicate-review.png");
    });

    const sourceDigestPanel = await openSourceDigestDetailBrowser(drawer);
    const reviewRows = within(sourceDigestPanel).getByLabelText("Duplicate Review Model 需核对配置行定位") as HTMLDetailsElement;
    fireEvent.click(within(reviewRows).getByText("需核对行 2"));
    fireEvent.click(within(reviewRows).getByRole("button", {
      name: "建列后定位此行：Duplicate selected review",
    }));

    expect(within(reviewRows).getByRole("button", {
      name: "已设为建列后定位：Duplicate selected review",
    }).getAttribute("aria-pressed")).toBe("true");
    expect(within(reviewRows).queryByRole("button", {
      name: "已设为建列后定位：Duplicate first review",
    })).toBeNull();
    expect(consoleErrorSpy.mock.calls.some((call) => String(call[0]).includes("Encountered two children with the same key"))).toBe(false);
    consoleErrorSpy.mockRestore();
  });

  it("keeps source digest groups visible when the source context model matches the current page", async () => {
    const contextModel = "Business Context Model";
    const contextCountry = "Context Market";
    const ocrFileModel = "ocr file inferred model";
    const contextMatchedGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "context-matched-group",
      title: "OCR File Sheet",
      sourceSheet: "OCR Image 1",
      modelName: ocrFileModel,
      sourceKind: "ocr_headerless",
      identityStatus: "temporary_ocr_column",
      trims: digest.compareGroups[0].trims.map((trim, index) => ({
        ...trim,
        trimId: `context-ocr-column-${index + 1}`,
        trimName: `OCR Column ${index + 1}`,
        fullTrimName: `OCR Column ${index + 1} · 待补配置列身份`,
        modelName: ocrFileModel,
        materialNo: null,
        hasMaterialNo: false,
        salesVersion: null,
        dataOrigin: "external_or_scraped",
        sourceSheet: "OCR Image 1",
        identityStatus: "temporary_ocr_column",
      })),
    };
    const contextMatchedDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "image_ocr",
      sourceFormat: "image_ocr",
      fileName: "context-matched-ocr.png",
      modelName: ocrFileModel,
      summary: {
        ...digest.summary,
        candidateTrimCount: 2,
        comparableGroupCount: 1,
      },
      compareGroups: [contextMatchedGroup],
    };
    const relatedContext = {
      brand: "SmokeBrand",
      model: contextModel,
      market: contextCountry,
      country: contextCountry,
      powertrain: "ICE",
      segment: "Smoke Segment",
      modelYear: "2026",
      trimIds: [],
      salesVersionIds: [],
      contextType: "compare",
    };
    const listSnapshot: EngineeringConfigSourceSnapshot = {
      ...buildSourceSnapshotFixture("source-context-match", "context-matched-ocr.png", null),
      relatedContext,
      sourceSearchMatches: [`上下文 ${contextModel}`],
      sourceDigestStatus: {
        digestType: "image_ocr",
        status: "ready",
        sourceFormat: "image_ocr",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    const detailSnapshot: EngineeringConfigSourceSnapshot = {
      ...buildSourceSnapshotFixture("source-context-match", "context-matched-ocr.png", contextMatchedDigest),
      relatedContext,
      sourceSearchMatches: [`上下文 ${contextModel}`],
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 1, items: [listSnapshot] });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(detailSnapshot);

    const { container } = render(
      <MemoryRouter initialEntries={[`/product/compare/config?market=${encodeURIComponent(contextCountry)}&model=${encodeURIComponent(contextModel)}&powertrain=ICE&segment=Smoke%20Segment`]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;

    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        country: contextCountry,
        modelYear: null,
        powertrain: "ICE",
        q: contextModel,
        segment: "Smoke Segment",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("context-matched-ocr.png");
    });
    expect(detailBrowser.open).toBe(false);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-context-match");
    });

    const sourceDigestPanel = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-context-match");
    });
    expect(within(sourceDigestPanel).getByText("来源命中")).toBeTruthy();
    expect(within(sourceDigestPanel).getByText("SmokeBrand · Business Context Model · Context Market · ICE · MY 2026 · Smoke Segment")).toBeTruthy();
    expect(within(sourceDigestPanel).getByRole("button", {
      name: /选择 Source Digest 可比组：ocr file inferred model/,
    })).toBeTruthy();
    expect(within(sourceDigestPanel).getByText("来源库命中 1 个来源，当前显示 1/1 个可转配置列组。")).toBeTruthy();
  });

  it("keeps card-level Source Digest model focus scoped to the digest group model", async () => {
    const realModelName = "T19C-BEV（2025款）";
    const sourceContextModel = "EU config resource table";
    const sourceFileName = "欧盟在售车型可控资源表20260226.xlsx";
    const makeWorkbookGroup = (
      groupId: string,
      modelName: string,
      title: string,
      trimNames: [string, string],
      differenceCount: number,
    ): EngineeringConfigSourceDigestGroup => ({
      ...digest.compareGroups[0],
      groupId,
      title,
      sourceSheet: modelName,
      modelName,
      differenceCount,
      trims: digest.compareGroups[0].trims.map((trim, index) => ({
        ...trim,
        trimId: `${groupId}-${index + 1}`,
        trimName: trimNames[index],
        fullTrimName: trimNames[index],
        modelName,
        sourceSheet: modelName,
      })),
    });
    const workbookGroups = [
      makeWorkbookGroup(
        "t19c-ice",
        "T19C MY ICE",
        "T19C MY ICE · The Specification of Omoda 5",
        ["两驱基本型 Basic-FWD", "两驱尊贵型 Premium-FWD"],
        37,
      ),
      makeWorkbookGroup(
        "t19c-bev",
        realModelName,
        `${realModelName} · The Specification of OMODA5 EV`,
        ["两驱长续航舒适型 Comfort-FWD", "两驱长续航尊贵型 Premium-FWD"],
        25,
      ),
      makeWorkbookGroup(
        "t19c-hev",
        "T19C-HEV",
        "T19C-HEV · The Specification of Omoda 5 HEV",
        ["FLEET-HEV", "两驱尊贵型-HEV Premium-FWD"],
        31,
      ),
    ];
    const featureCount = workbookGroups.reduce((total, group) => total + group.rows.length, 0);
    const differenceCount = workbookGroups.reduce((total, group) => total + group.differenceCount, 0);
    const candidateTrimCount = workbookGroups.reduce((total, group) => total + group.trimCount, 0);
    const comparableGroupCount = workbookGroups.length;
    const workbookDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: sourceFileName,
      modelName: realModelName,
      summary: {
        ...digest.summary,
        candidateTrimCount,
        comparableGroupCount,
        featureCount,
        differenceCount,
      },
      compareGroups: workbookGroups,
    };
    const relatedContext = {
      brand: "OMODA",
      model: sourceContextModel,
      market: "EU",
      country: "EU",
      modelYear: null,
      trimIds: [],
      salesVersionIds: [],
      contextType: "compare",
    };
    const listSnapshot: EngineeringConfigSourceSnapshot = {
      ...buildSourceSnapshotFixture("eu-workbook", sourceFileName, null),
      relatedContext,
      sourceSearchMatches: ["Model T19C MY ICE", `Model ${realModelName}`, "Sheet T19C-BEV（2025款）"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount,
          comparableGroupCount,
          featureCount,
          differenceCount,
        },
      },
    };
    const detailSnapshot: EngineeringConfigSourceSnapshot = {
      ...buildSourceSnapshotFixture("eu-workbook", sourceFileName, workbookDigest),
      relatedContext,
      sourceSearchMatches: ["Model T19C MY ICE", `Model ${realModelName}`, "Sheet T19C-BEV（2025款）"],
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 1, items: [listSnapshot] });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(detailSnapshot);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "T19C" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "T19C",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain(sourceFileName);
    });
    const sourceDigestPanel = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(within(sourceDigestPanel).getByRole("button", {
        name: new RegExp(`选择 Source Digest 可比组：${realModelName}`),
      })).toBeTruthy();
      expect(within(sourceDigestPanel).getByRole("button", {
        name: /选择 Source Digest 可比组：T19C MY ICE/,
      })).toBeTruthy();
      expect(within(sourceDigestPanel).getByRole("button", {
        name: /选择 Source Digest 可比组：T19C-HEV/,
      })).toBeTruthy();
    });

    const cardQuickSearch = within(sourceDigestPanel).getByLabelText(`${realModelName} Source Digest 路径快速搜索`);
    expect(within(cardQuickSearch).queryByRole("button", {
      name: `聚焦车型：${realModelName} / ${sourceContextModel}`,
    })).toBeNull();
    fireEvent.click(within(cardQuickSearch).getByRole("button", {
      name: `聚焦车型：${realModelName}`,
    }));

    await waitFor(() => {
      expect((within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value)
        .toBe(realModelName);
      expect(within(sourceDigestPanel).getByRole("button", {
        name: new RegExp(`选择 Source Digest 可比组：${realModelName}`),
      })).toBeTruthy();
      expect(within(sourceDigestPanel).getByLabelText("Source Digest 检索覆盖").textContent).toContain("Model1");
      expect(within(sourceDigestPanel).getByLabelText("Source Digest 检索覆盖").textContent).toContain("可比配置列2");
    });

    const focusedCardQuickSearch = within(sourceDigestPanel).getByLabelText(`${realModelName} Source Digest 路径快速搜索`);
    expect(within(focusedCardQuickSearch).queryByRole("button", {
      name: `聚焦来源：${sourceFileName} / ${realModelName}`,
    })).toBeNull();
    fireEvent.click(within(focusedCardQuickSearch).getByRole("button", {
      name: `聚焦来源：${sourceFileName}`,
    }));

    await waitFor(() => {
      expect((within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value)
        .toBe("");
      expect(within(sourceDigestPanel).getByLabelText("当前 Source Digest 来源聚焦").textContent).toContain(sourceFileName);
      expect(within(sourceDigestPanel).getByRole("button", { name: "解除来源聚焦" })).toBeTruthy();
      const activeScope = within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL).textContent;
      expect(activeScope).toContain("只看来源");
      expect(activeScope).not.toContain(`关键词${sourceFileName}`);
      const coverage = within(sourceDigestPanel).getByLabelText("Source Digest 检索覆盖").textContent;
      expect(coverage).toContain("来源1");
      expect(coverage).toContain("Model3");
      expect(coverage).toContain("配置列6");
      expect(within(sourceDigestPanel).getByRole("button", {
        name: /选择 Source Digest 可比组：T19C MY ICE/,
      })).toBeTruthy();
      expect(within(sourceDigestPanel).getByRole("button", {
        name: new RegExp(`选择 Source Digest 可比组：${realModelName}`),
      })).toBeTruthy();
      expect(within(sourceDigestPanel).getByRole("button", {
        name: /选择 Source Digest 可比组：T19C-HEV/,
      })).toBeTruthy();
    });

    fireEvent.click(within(sourceDigestPanel).getByRole("button", { name: "解除来源聚焦" }));

    await waitFor(() => {
      expect((within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value)
        .toBe(sourceFileName);
      expect(within(sourceDigestPanel).queryByLabelText("当前 Source Digest 来源聚焦")).toBeNull();
      const activeScope = within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL).textContent;
      expect(activeScope).toContain(`关键词${sourceFileName}`);
      expect(activeScope).not.toContain("只看来源");
      const coverage = within(sourceDigestPanel).getByLabelText("Source Digest 检索覆盖").textContent;
      expect(coverage).toContain("来源1");
      expect(coverage).toContain("Model3");
      expect(coverage).toContain("配置列6");
    });
  });

  it("creates editable config columns from uploaded source digest groups in the floating deck", async () => {
    const libraryGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "library-model",
      title: "Library Model",
      sourceSheet: "Uploaded Sheet",
      modelName: "Library Model",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        country: "Germany",
        market: "Germany",
        modelName: "Library Model",
        profile: {
          ...(trim.profile ?? {}),
          country: "Germany",
          modelYear: "2026",
          powertrain: "BEV",
          drivetrain: trim.materialNo ? "RWD" : "AWD",
        },
        salesVersion: trim.materialNo ? null : "Premium SV",
        trimId: `library-${trim.trimId}`,
      })),
    };
    const libraryDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "uploaded-source.xlsx",
      sourceFormat: "image_ocr",
      ocrEngine: "paddleocr",
      ocrEvaluation: {
        strategy: "highest_config_semantic_score",
        reason: "highest_config_semantic_score",
        candidateCount: 2,
        comparableCandidateCount: 1,
        selectedCandidateCount: 1,
        selectedEngine: "paddleocr",
        selectedEngines: ["paddleocr"],
        selectedScore: {
          semanticScore: 1,
          comparableGroupCount: 1,
          featureCount: 1,
          differenceCount: 1,
          candidateTrimCount: 2,
          totalFeatureCount: 1,
          totalDifferenceCount: 1,
          totalCandidateTrimCount: 2,
          tableShapeScore: 16,
          rowCount: 4,
          columnCount: 4,
          nonEmptyCount: 12,
        },
        selectedSheetName: "Uploaded Sheet",
        selectedPageNumber: 1,
        selectedReasonDetails: [
          "paddleocr 识别到可比配置表；legacy_image_ocr 未形成可比配置表。",
          "paddleocr 选中结果：配置项 1、配置列 2、差异 1、表格 4 x 4、非空 12。",
        ],
      },
      summary: {
        ...digest.summary,
        comparableGroupCount: 1,
      },
      compareGroups: [libraryGroup],
    };
    const listSnapshot: EngineeringConfigSourceSnapshot = {
      ...buildSourceSnapshotFixture("source-library", "uploaded-source.xlsx", null),
      sourceSearchMatches: ["文件 uploaded-source.xlsx", "Model Library Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    const detailSnapshot = buildSourceSnapshotFixture("source-library", "uploaded-source.xlsx", libraryDigest);
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({ rows: 12, items: [listSnapshot] });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(detailSnapshot);
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-library",
      groupId: "library-model",
      importBatchId: "draft-library",
      trimIds: ["draft-library-basic", "draft-library-premium"],
      compareTrimIds: ["draft-library-basic", "draft-library-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      aliasMatchedFeatureCount: 2,
      semanticAliasMatchedFeatureCount: 1,
      featureMatchReasonCounts: { alias: 1, semantic_alias: 1, created: 1 },
      featureMatchSamples: [
        {
          sourceFeatureName: "360 camera",
          matchedFeatureName: "360 round view camera / 360度高清全景影像",
          matchedFeatureCode: "digest_df9a8c8d2a1dc6e5",
          matchReason: "alias",
        },
      ],
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-library-basic", "version-library-premium"],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    const controlButton = await screen.findByRole("button", { name: /添加配置列 \/ 显示/ });
    fireEvent.click(controlButton);

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    openSimpleAdvancedSearch();
    const digestCandidates = within(drawer).getByText("Source Digest 可比组").closest(".market-scan-field") as HTMLElement;
    expect(within(digestCandidates).getByText("输入车型 / 来源 / 上传人 / 物料号 / sales version 后再搜索来源库；本地样例仍可直接预览。")).toBeTruthy();
    expect(api.getEngineeringConfigSourceSnapshot).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    expect(within(drawer).queryByText("Source Digest 可比组")).toBeNull();
    const digestSearchInput = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME });
    expect(digestSearchInput.getAttribute("placeholder")).toBe("搜索来源文件 / 车型 / 市场 / 年款 / 上传人 / 物料号...");
    fireEvent.change(digestSearchInput, { target: { value: "Library Model" } });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;

    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Library Model",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("uploaded-source.xlsx");
    });
    expect(detailBrowser.open).toBe(false);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-library");
    });
    const sourceDigestPanel = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-library");
    });
    expect(within(sourceDigestPanel).getByRole("button", { name: /选择 Source Digest 可比组：Library Model/ })).toBeTruthy();
    const sourceSnapshotHints = within(sourceDigestPanel).getByLabelText("来源库轻量命中");
    expect(within(sourceSnapshotHints).getByText("来源命中")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("uploaded-source.xlsx")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("Digest ready · 可比组 1 · 候选配置列 2 · 配置项 3 · 差异 2")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("OMODA · Library Model · EU")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("上传人 tester")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("命中 文件 uploaded-source.xlsx / Model Library Model")).toBeTruthy();
    expect(within(sourceDigestPanel).getAllByText(/来源库 · 点击创建可编辑配置列/).length).toBeGreaterThan(0);
    expect(within(sourceDigestPanel).getAllByText(/上传人 tester/).length).toBeGreaterThan(1);
    expect(within(sourceDigestPanel).getByText("EU / Germany · MY 2026 · 动力 BEV / RWD / AWD · 物料号 1/2 · 无物料号 1，Sales version 1")).toBeTruthy();
    expect(within(sourceDigestPanel).getByText("图片 OCR · OCR paddleocr · 候选 2 · 可比候选 1/2 · 按配置表语义选优")).toBeTruthy();
    expect(within(sourceDigestPanel).getByText("paddleocr 识别到可比配置表；legacy_image_ocr 未形成可比配置表。")).toBeTruthy();
    expect(within(sourceDigestPanel).getByText("paddleocr 选中结果：配置项 1、配置列 2、差异 1、表格 4 x 4、非空 12。")).toBeTruthy();
    expect(within(sourceDigestPanel).getAllByText("命中 文件 uploaded-source.xlsx / Model Library Model").length).toBeGreaterThan(1);
    expect(within(sourceDigestPanel).getByText("来源库命中 12 个来源，当前显示 1/1 个可转配置列组。结果较多时继续输入车型 / 市场 / 年款 / 来源 / 关键词精确定位。")).toBeTruthy();
    fireEvent.focus(digestSearchInput);
    const digestSearchOptions = within(drawer).getByRole("listbox");
    expect(within(digestSearchOptions).getByText("Germany")).toBeTruthy();
    expect(within(digestSearchOptions).getByText("2026")).toBeTruthy();
    expect(within(digestSearchOptions).getByText("BEV")).toBeTruthy();
    expect(within(digestSearchOptions).getByText("AWD")).toBeTruthy();
    expect(within(digestSearchOptions).getByText("paddleocr")).toBeTruthy();
    expect(digestSearchOptions.textContent).toContain("上传人 tester");

    const libraryGroupButton = within(sourceDigestPanel).getByRole("button", {
      name: /选择 Source Digest 可比组：Library Model/,
    }) as HTMLButtonElement;
    fireEvent.click(libraryGroupButton);

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-library", "library-model");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-library-basic", "draft-library-premium"], false, "latest");
    });
    expect(screen.getByText("Library Model 已按 2 个配置列创建为可编辑配置列：2 配置列（新建 2，复用 0） · 3 配置项（新建 3，复用 0） · 写入 6 条值（新增 6，更新 0）。")).toBeTruthy();
    expect(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }).getAttribute("aria-selected")).toBe("true");
    const draftSuccess = screen.getByLabelText("来源建列成功");
    expect(draftSuccess.textContent).toContain("已转成正式配置列");
    expect(draftSuccess.textContent).toContain("新配置列已加入当前对比表");
    expect(draftSuccess.textContent).toContain("创建为可编辑配置列");
    expect(draftSuccess.textContent).toContain("2 配置列");
    expect(within(draftSuccess).getByLabelText("建列字段归并摘要").textContent).toContain("alias/语义别名命中 2 项");
    expect(within(draftSuccess).getByLabelText("建列字段归并摘要").textContent).toContain("360 camera -> 360 round view camera / 360度高清全景影像");
    const draftPath = within(draftSuccess).getByLabelText("建列来源路径");
    expect(within(draftPath).getByText("Source")).toBeTruthy();
    expect(draftPath.textContent).toContain("uploaded-source.xlsx");
    expect(draftPath.textContent).toContain("Uploaded Sheet");
    expect(within(draftPath).getByText("Model")).toBeTruthy();
    expect(draftPath.textContent).toContain("Library Model");
    expect(within(draftPath).getByText("配置列")).toBeTruthy();
    expect(draftPath.textContent).toContain("Basic");
    expect(draftPath.textContent).toContain("Premium");
    const draftMetrics = within(draftSuccess).getByLabelText("建列结果摘要");
    expect(draftMetrics.textContent).toContain("新建列2");
    expect(draftMetrics.textContent).toContain("复用列0");
    expect(draftMetrics.textContent).toContain("配置项3");
    expect(draftMetrics.textContent).toContain("配置值6");
    expect(draftSuccess.textContent).toContain("下一步都在 FloatingDeck 内完成");
    expect(draftSuccess.textContent).toContain("如需编辑，请在下方“在线编辑”控制里开启");
    expect(within(draftSuccess).getByLabelText("建列后工作区").textContent).toContain("已切到“显示 / 编辑”工作区");
    expect(within(draftSuccess).getByLabelText("建列后 AI 摘要边界").textContent).toContain("AI 业务摘要会按当前对比表运行时生成");
    expect(draftSuccess.textContent).toContain("不写回来源解析记录");
    expect(within(draftSuccess).queryByRole("button", { name: "开启建列结果编辑" })).toBeNull();
    expect(within(draftSuccess).getByRole("button", { name: "导出建列结果 XLSX" })).toBeTruthy();
    expect(within(draftSuccess).getByRole("button", { name: "导出建列结果 PDF" })).toBeTruthy();
    expect(within(draftSuccess).getByRole("button", { name: "查看已选配置列" })).toBeTruthy();
    expect(within(draftSuccess).getByRole("button", { name: "继续添加来源" })).toBeTruthy();
  });

  it("lets the source digest tab search library groups and create editable config columns", async () => {
    const otherGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "other-source-model",
      title: "Other Source Model",
      modelName: "Other Source Model",
      sourceSheet: "Other Source Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Other Source Model",
        trimName: trim.trimName.replace("两驱", "Other "),
      })),
    };
    const libraryGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-tab-model",
      title: "Source Tab Model",
      modelName: "Source Tab Model",
      sourceSheet: "Source Tab Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Source Tab Model",
        trimName: trim.trimName.replace("两驱", "Source Tab "),
      })),
    };
    const sourceTabDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "uploaded-source.xlsx",
      summary: {
        ...digest.summary,
        candidateTrimCount: 4,
        comparableGroupCount: 2,
      },
      compareGroups: [otherGroup, libraryGroup],
    };
    const sourceTabSummary = {
      ...buildSourceSnapshotFixture("source-tab", "uploaded-source.xlsx", null),
      sourceSearchMatches: ["文件 uploaded-source.xlsx", "Model Source Tab Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 4,
          comparableGroupCount: 2,
          featureCount: 6,
          differenceCount: 4,
        },
      },
    };
    const pendingSourceSummary: EngineeringConfigSourceSnapshot = {
      ...buildSourceSnapshotFixture("source-tab-pending-image", "source-tab-camera.png", null),
      fileType: "image",
      mimeType: "image/png",
      extractStatus: "pending",
      nextAction: "extractor_pending",
      relatedContext: {
        brand: "OMODA",
        model: "Source Tab Model",
        market: "EU",
        country: "EU",
        powertrain: "PHEV",
        modelYear: null,
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
      sourceSearchMatches: ["文件 source-tab-camera.png", "Model Source Tab Model"],
      sourceDigestStatus: {
        digestType: "image_ocr",
        status: "pending",
        message: "PaddleOCR is not installed.; OCR engine is not configured.",
        summary: {
          candidateTrimCount: 0,
          comparableGroupCount: 0,
          featureCount: 0,
          differenceCount: 0,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 2,
      items: [sourceTabSummary, pendingSourceSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(
      buildSourceSnapshotFixture("source-tab", "uploaded-source.xlsx", sourceTabDigest),
    );
    const sourceTabDraftResult: EngineeringConfigDigestDraftResult = {
      sourceId: "source-tab",
      groupId: "source-tab-model",
      importBatchId: "draft-source-tab",
      trimIds: ["draft-source-tab-basic", "draft-source-tab-premium"],
      compareTrimIds: ["draft-source-tab-basic", "draft-source-tab-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-source-tab-basic", "version-source-tab-premium"],
    };
    const sourceTabDraftDeferred: {
      resolve?: (result: EngineeringConfigDigestDraftResult) => void;
    } = {};
    const sourceTabDraftPromise = new Promise<EngineeringConfigDigestDraftResult>((resolve) => {
      sourceTabDraftDeferred.resolve = resolve;
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockReturnValueOnce(sourceTabDraftPromise);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "Source Tab Model" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Source Tab Model",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("uploaded-source.xlsx");
    });
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-tab");
    });
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-tab");
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Source Tab Model/ })).toBeTruthy();
    });
    expect(within(drawer).getByText("当前来源库查询：Source Tab Model；未按国家、动力或 Segment 收窄")).toBeTruthy();
    const sourceScopeStrip = within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL);
    expect(sourceScopeStrip.textContent).toMatch(/当前(?:来源| Source )范围/);
    expect(sourceScopeStrip.textContent).toContain("聚焦车型");
    expect(sourceScopeStrip.textContent).toContain("Source Tab Model");
    expect(sourceScopeStrip.textContent).toContain("1 来源");
    const pathPreview = within(drawer).getByLabelText("Source Digest 命中路径预览");
    expect(pathPreview.textContent).toContain("uploaded-source.xlsx");
    expect(pathPreview.textContent).toContain("Source Tab Model");
    expect(pathPreview.textContent).toContain("聚焦来源");
    expect(pathPreview.textContent).toContain("聚焦车型");
    expect(pathPreview.textContent).toContain("2 可比配置列");
    const sourceTabFlow = within(pathPreview).getByLabelText("uploaded-source.xlsx 来源车型配置列路径");
    expect(within(sourceTabFlow).getByText("来源")).toBeTruthy();
    expect(within(sourceTabFlow).getByText("uploaded-source.xlsx")).toBeTruthy();
    expect(within(sourceTabFlow).getByText("车型")).toBeTruthy();
    expect(within(sourceTabFlow).getByText("Source Tab Model")).toBeTruthy();
    expect(within(sourceTabFlow).getByText("配置列")).toBeTruthy();
    expect(sourceTabFlow.textContent).toContain("2 可比配置列");
    const sourceDigestCoverage = within(digestCandidates).getByLabelText("Source Digest 检索覆盖");
    expect(sourceDigestCoverage.textContent).toContain("来源1");
    expect(sourceDigestCoverage.textContent).toContain("Model1");
    expect(sourceDigestCoverage.textContent).toContain("可比配置列2");
    expect(sourceDigestCoverage.textContent).toContain("配置行3");
    expect(sourceDigestCoverage.textContent).toContain("差异2");
    expect(sourceDigestCoverage.textContent).toContain("覆盖当前搜索与筛选后的来源 / 车型 / 配置列");
    expect(within(digestCandidates).getByText("来源库命中 2 个来源，当前显示 1/2 个可转配置列组。已按当前车型 / 市场 / 来源 / 关键词或来源聚焦收窄。")).toBeTruthy();
    const sourceTabCandidateButton = within(digestCandidates).getByRole("button", {
      name: /选择 Source Digest 可比组：Source Tab Model/,
    });
    const otherCandidateButton = within(digestCandidates).queryByRole("button", {
      name: /选择 Source Digest 可比组：Other Source Model/,
    });
    expect(sourceTabCandidateButton.textContent).toContain("Source Tab Model");
    expect(sourceTabCandidateButton.textContent).toContain("来源库");
    const sourceTabPath = within(sourceTabCandidateButton).getByLabelText("Source Tab Model 来源库路径");
    expect(sourceTabPath.textContent).toContain("品牌OMODA");
    expect(sourceTabPath.textContent).toContain("车型Source Tab Model");
    expect(sourceTabPath.textContent).toContain("市场EU");
    expect(sourceTabPath.textContent).toContain("来源uploaded-source.xlsx / Source Tab Sheet");
    expect(sourceTabCandidateButton.textContent).toContain("配置列2");
    expect(sourceTabCandidateButton.textContent).toContain("差异2");
    expect(sourceTabCandidateButton.textContent).toContain("配置行3");
    expect(sourceTabCandidateButton.textContent).toContain("uploaded-source.xlsx / Source Tab Sheet");
    expect(sourceTabCandidateButton.textContent).toContain("上传人 tester");
    expect(sourceTabCandidateButton.getAttribute("aria-label")).toContain("选择 Source Digest 可比组：Source Tab Model");
    expect(within(sourceTabCandidateButton).getByLabelText("Source Tab Model digest 指标")).toBeTruthy();
    expect(otherCandidateButton).toBeNull();
    const sourceSnapshotHints = within(digestCandidates).getByLabelText("来源库轻量命中");
    expect(within(sourceSnapshotHints).getByText("来源命中")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("已匹配 2 个来源，1 个可转配置列来源")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("uploaded-source.xlsx")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("source-tab-camera.png")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("已入库，等待解析")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("Digest 待处理 · 可比组 0 · 候选配置列 0 · 配置项 0 · 差异 0")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("OMODA · Source Tab Model · EU · PHEV")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("PaddleOCR is not installed.; OCR engine is not configured.")).toBeTruthy();
    expect(within(sourceSnapshotHints).getByText("Digest ready · 可比组 2 · 候选配置列 4 · 配置项 6 · 差异 4")).toBeTruthy();
    expect(within(sourceSnapshotHints).getAllByText("上传人 tester").length).toBeGreaterThanOrEqual(2);
    expect(within(sourceSnapshotHints).getByText("命中 文件 uploaded-source.xlsx / Model Source Tab Model")).toBeTruthy();
    expect(api.getEngineeringConfigSourceSnapshot).not.toHaveBeenCalledWith("source-tab-pending-image");
    expect(within(digestCandidates).getAllByText("命中 文件 uploaded-source.xlsx / Model Source Tab Model").length).toBeGreaterThan(0);
    expect(within(digestCandidates).getAllByText(/来源库 · 点击创建可编辑配置列/).length).toBeGreaterThan(0);
    fireEvent.click(within(pathPreview).getByRole("button", { name: "聚焦来源：uploaded-source.xlsx" }));
    expect((screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value).toBe("");
    const refreshedSourceScopeStrip = within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL);
    expect(refreshedSourceScopeStrip.textContent).toContain("只看来源");
    expect(refreshedSourceScopeStrip.textContent).toContain("uploaded-source.xlsx");
    fireEvent.click(within(refreshedSourceScopeStrip).getByRole("button", {
      name: /清除(?:来源| Source Digest)搜索条件：只看来源 uploaded-source\.xlsx/,
    }));
    expect((screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value).toBe("uploaded-source.xlsx");

    const sourceDigestSearch = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME });
    fireEvent.change(sourceDigestSearch, { target: { value: "tester" } });
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "tester",
      }));
    });
    fireEvent.focus(sourceDigestSearch);
    const sourceSearchListbox = await screen.findByRole("listbox");
    fireEvent.click(within(sourceSearchListbox).getByRole("option", {
      name: /Source Tab Model.*整组.*uploaded-source\.xlsx/,
    }));
    const sourcePanelDirectPicker = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_PICKER_COMBOBOX_NAME }) as HTMLInputElement;
    await waitFor(() => {
      expect(sourcePanelDirectPicker.value).toContain("生成配置列");
      expect(sourcePanelDirectPicker.value).toContain("Source Tab Model");
    });
    const sourcePanelGroupConfirm = within(drawer).getByLabelText("当前来源整组确认");
    fireEvent.click(within(sourcePanelGroupConfirm).getByRole("button", { name: "生成当前整组配置列" }));
    await waitFor(() => {
      const busyButton = within(sourcePanelGroupConfirm).getByRole("button", { name: "正在生成当前整组" }) as HTMLButtonElement;
      expect(busyButton.disabled).toBe(true);
      expect(busyButton.getAttribute("aria-busy")).toBe("true");
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-tab", "source-tab-model");
    });
    expect(sourceTabDraftDeferred.resolve).toBeTruthy();
    sourceTabDraftDeferred.resolve?.(sourceTabDraftResult);

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-tab", "source-tab-model");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-source-tab-basic", "draft-source-tab-premium"], false, "latest");
    });
    const draftSuccess = screen.getByLabelText("来源建列成功");
    expect(draftSuccess.textContent).toContain("Source Tab Model 已按 2 个配置列创建为可编辑配置列");
    const currentCompareStatus = within(draftSuccess).getByLabelText("当前对比已加入配置列");
    expect(currentCompareStatus.textContent).toContain("基准 Basic；目标 Premium");
    expect(currentCompareStatus.textContent).toContain("已成为当前对比 · 当前 2 个配置列");
    const draftPath = within(draftSuccess).getByLabelText("建列来源路径");
    expect(draftPath.textContent).toContain("uploaded-source.xlsx");
    expect(draftPath.textContent).toContain("Source Tab Model");
    expect(draftPath.textContent).not.toContain("Other Source Model");
    expect(draftPath.textContent).toContain("Basic");
    expect(draftPath.textContent).toContain("Premium");
    expect(within(draftSuccess).getByLabelText("建列结果摘要").textContent).toContain("配置值6");
  });

  it("prioritizes current selected model paths in the Source Digest preview", async () => {
    const otherGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "other-source-model",
      title: "Other Source Model",
      modelName: "Other Source Model",
      sourceSheet: "Other Source Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Other Source Model",
        trimName: trim.trimName.replace("两驱", "Other "),
      })),
    };
    const t19cGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "t19c-source-model",
      title: "T19C MY ICE",
      modelName: "T19C MY ICE",
      sourceSheet: "T19C Source Sheet",
    };
    const multiModelDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "multi-model-source.xlsx",
      modelName: "Mixed Source",
      summary: {
        ...digest.summary,
        candidateTrimCount: 4,
        comparableGroupCount: 2,
      },
      compareGroups: [otherGroup, t19cGroup],
    };
    const sharedSummary = {
      ...buildSourceSnapshotFixture("source-shared", "multi-model-source.xlsx", null),
      sourceSearchMatches: ["文件 multi-model-source.xlsx", "Model Other Source Model", "Model T19C MY ICE"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 4,
          comparableGroupCount: 2,
          featureCount: 6,
          differenceCount: 4,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 1,
      items: [sharedSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(
      buildSourceSnapshotFixture("source-shared", "multi-model-source.xlsx", multiModelDigest),
    );

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "multi-model" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "multi-model",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("multi-model-source.xlsx");
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-shared");
    });

    await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-shared");
    });
    const pathPreview = await within(drawer).findByLabelText("Source Digest 命中路径预览");
    const previewItems = pathPreview.querySelectorAll(".product-config-source-path-preview__item");
    expect(previewItems.length).toBeGreaterThanOrEqual(2);
    expect(previewItems[0]?.textContent).toContain("multi-model-source.xlsx");
    expect(previewItems[0]?.textContent).toContain("T19C MY ICE");
    expect(previewItems[1]?.textContent).toContain("multi-model-source.xlsx");
    expect(previewItems[1]?.textContent).toContain("Other Source Model");
    const sourceBrowser = within(detailBrowser).getByLabelText("Source Digest 按来源和品牌浏览");
    const browserGroups = sourceBrowser.querySelectorAll(".product-config-source-digest-browser-group");
    expect(browserGroups.length).toBeGreaterThanOrEqual(2);
    expect(browserGroups[0]?.textContent).toContain("multi-model-source.xlsx");
    expect(browserGroups[0]?.textContent).toContain("T19C MY ICE");
    expect(browserGroups[1]?.textContent).toContain("multi-model-source.xlsx");
    expect(browserGroups[1]?.textContent).toContain("Other Source Model");

    const directSourcePicker = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_PICKER_COMBOBOX_NAME });
    fireEvent.focus(directSourcePicker);
    const directSourceListbox = screen.getByRole("listbox");
    expect(directSourceListbox.textContent).toContain("聚焦来源 · multi-model-source.xlsx");
    expect(directSourceListbox.textContent).toContain("聚焦车型 · T19C MY ICE");
    expect(directSourceListbox.textContent).toContain("聚焦车型 · Other Source Model");
    const directSourceOptions = within(directSourceListbox).getAllByRole("option");
    const focusSourceOptionIndex = directSourceOptions.findIndex((option) => (
      option.textContent?.includes("聚焦来源 · multi-model-source.xlsx")
    ));
    const focusModelOptionIndex = directSourceOptions.findIndex((option) => (
      option.textContent?.includes("聚焦车型 · T19C MY ICE")
    ));
    const previewGroupOptionIndex = directSourceOptions.findIndex((option) => (
      option.textContent?.includes("T19C MY ICE") && option.textContent?.includes("生成配置列")
    ));
    const trimOptionIndex = directSourceOptions.findIndex((option) => (
      option.textContent?.includes("T19C MY ICE") && option.textContent?.includes("暂选配置列")
    ));
    expect(focusSourceOptionIndex).toBeGreaterThanOrEqual(0);
    expect(focusModelOptionIndex).toBeGreaterThanOrEqual(0);
    expect(previewGroupOptionIndex).toBeGreaterThanOrEqual(0);
    expect(trimOptionIndex).toBeGreaterThanOrEqual(0);
    expect(directSourceOptions[focusSourceOptionIndex]?.querySelector(".version-comparison-model-option-badge")?.textContent).toBe("来源");
    expect(directSourceOptions[focusModelOptionIndex]?.querySelector(".version-comparison-model-option-badge")?.textContent).toBe("车型");
    expect(directSourceOptions[previewGroupOptionIndex]?.querySelector(".version-comparison-model-option-badge")?.textContent).toBe("配置组");
    expect(directSourceOptions[trimOptionIndex]?.querySelector(".version-comparison-model-option-badge")?.textContent).toBe("配置列");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("Model");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("T19C MY ICE");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("Other Source Model");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("Source Digest 待生成范围");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("配置列");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("Basic");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).toContain("4 可比配置列");
    expect(directSourceOptions[focusSourceOptionIndex]?.textContent).not.toContain("Brand ");
    const focusSourceText = directSourceOptions[focusSourceOptionIndex]?.textContent ?? "";
    expect(focusSourceText.indexOf("4 可比配置列")).toBeLessThan(focusSourceText.indexOf("配置列 Basic"));
    expect(focusSourceText.indexOf("4 差异")).toBeLessThan(focusSourceText.indexOf("配置列 Basic"));
    const focusModelText = directSourceOptions[focusModelOptionIndex]?.textContent ?? "";
    expect(focusModelText).toContain("Source Digest 待生成范围");
    expect(focusModelText.indexOf("2 可比配置列")).toBeLessThan(focusModelText.indexOf("配置列 Basic"));
    expect(focusModelText.indexOf("2 差异")).toBeLessThan(focusModelText.indexOf("配置列 Basic"));
    expect(focusSourceOptionIndex).toBeLessThan(focusModelOptionIndex);
    expect(focusModelOptionIndex).toBeLessThan(previewGroupOptionIndex);
    expect(focusModelOptionIndex).toBeLessThan(trimOptionIndex);
    fireEvent.change(directSourcePicker, { target: { value: "T19C" } });
    const searchedDirectSourceListbox = screen.getByRole("listbox");
    const searchedDirectSourceOptions = within(searchedDirectSourceListbox).getAllByRole("option");
    const searchedFocusSourceOptionIndex = searchedDirectSourceOptions.findIndex((option) => (
      option.textContent?.includes("聚焦来源 · multi-model-source.xlsx")
    ));
    const searchedFocusModelOptionIndex = searchedDirectSourceOptions.findIndex((option) => (
      option.textContent?.includes("聚焦车型 · T19C MY ICE")
    ));
    const searchedPreviewGroupOptionIndex = searchedDirectSourceOptions.findIndex((option) => (
      option.textContent?.includes("T19C MY ICE") && option.textContent?.includes("生成配置列")
    ));
    const searchedTrimOptionIndex = searchedDirectSourceOptions.findIndex((option) => (
      option.textContent?.includes("T19C MY ICE") && option.textContent?.includes("暂选配置列")
    ));
    expect(searchedFocusSourceOptionIndex).toBeGreaterThanOrEqual(0);
    expect(searchedFocusModelOptionIndex).toBeGreaterThanOrEqual(0);
    expect(searchedPreviewGroupOptionIndex).toBeGreaterThanOrEqual(0);
    expect(searchedTrimOptionIndex).toBeGreaterThanOrEqual(0);
    expect(searchedFocusSourceOptionIndex).toBeLessThan(searchedFocusModelOptionIndex);
    expect(searchedFocusModelOptionIndex).toBeLessThan(searchedPreviewGroupOptionIndex);
    expect(searchedFocusModelOptionIndex).toBeLessThan(searchedTrimOptionIndex);
    fireEvent.change(directSourcePicker, { target: { value: "Basic" } });
    const trimSearchedDirectSourceListbox = screen.getByRole("listbox");
    const trimSearchedDirectSourceOptions = within(trimSearchedDirectSourceListbox).getAllByRole("option");
    const trimSearchedFocusSourceOptionIndex = trimSearchedDirectSourceOptions.findIndex((option) => (
      option.textContent?.includes("聚焦来源 · multi-model-source.xlsx")
    ));
    const trimSearchedFocusModelOptionIndex = trimSearchedDirectSourceOptions.findIndex((option) => (
      option.textContent?.includes("聚焦车型 · Other Source Model")
    ));
    expect(trimSearchedFocusSourceOptionIndex).toBeGreaterThanOrEqual(0);
    expect(trimSearchedFocusModelOptionIndex).toBeGreaterThanOrEqual(0);
    expect(trimSearchedDirectSourceOptions[trimSearchedFocusSourceOptionIndex]?.textContent).toContain("配置列");
    expect(trimSearchedDirectSourceOptions[trimSearchedFocusSourceOptionIndex]?.textContent).toContain("Basic");
    const trimSearchedFocusSourceText = trimSearchedDirectSourceOptions[trimSearchedFocusSourceOptionIndex]?.textContent ?? "";
    expect(trimSearchedFocusSourceText.indexOf("4 可比配置列")).toBeLessThan(trimSearchedFocusSourceText.indexOf("配置列 Basic"));
    expect(trimSearchedFocusSourceText.indexOf("4 差异")).toBeLessThan(trimSearchedFocusSourceText.indexOf("配置列 Basic"));
    expect(trimSearchedFocusSourceOptionIndex).toBeLessThan(trimSearchedFocusModelOptionIndex);
    fireEvent.change(directSourcePicker, { target: { value: "T19C" } });
    const refreshedDirectSourceListbox = screen.getByRole("listbox");
    const t19cModelFocusOption = within(refreshedDirectSourceListbox)
      .getAllByRole("option", { name: /聚焦车型 · T19C MY ICE/ })
      .find((option) => option.textContent?.includes("multi-model-source.xlsx"));
    expect(t19cModelFocusOption).toBeTruthy();
    fireEvent.click(t19cModelFocusOption as HTMLElement);
    expect((within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value).toBe("T19C MY ICE");
    await waitFor(() => {
      expect(within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL).textContent).toContain("聚焦车型");
      expect(within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL).textContent).toContain("只看来源");
      expect(within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL).textContent).toContain("multi-model-source.xlsx");
    });
    const focusedDetailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    if (!focusedDetailBrowser.open) {
      fireEvent.click(within(focusedDetailBrowser).getByText("来源组详情浏览"));
    }
    const focusedDigestCandidates = (await within(focusedDetailBrowser).findByText("Source Digest 可比组")).closest(".market-scan-field") as HTMLElement;
    expect(within(focusedDigestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeTruthy();
    expect(within(focusedDigestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：Other Source Model/ })).toBeNull();
  });

  it("uses source snapshot context as the country and segment anchor for digest candidates", async () => {
    const contextSource = {
      ...buildSourceSnapshotFixture("source-context", "germany-rival.xlsx", digest),
      sourceSearchMatches: ["上下文 Germany · SUV C", "Model T19C"],
      relatedContext: {
        brand: "OMODA",
        model: "T19C MY ICE",
        market: "Germany",
        country: "Germany",
        segment: "SUV C",
        modelYear: "2026",
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockImplementation(async (options) => {
      if (typeof options !== "number" && options?.q === "T19C") {
        return { rows: 1, items: [contextSource] };
      }
      return { rows: 0, items: [] };
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(contextSource);

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?market=Germany&segment=SUV%20C"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("当前筛选还没有选择可比配置列。")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "T19C" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        country: "Germany",
        segment: "SUV C",
        q: "T19C",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("germany-rival.xlsx");
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    expect(api.getEngineeringConfigSourceSnapshot).not.toHaveBeenCalledWith("source-context");
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeTruthy();
    });
    expect(within(digestCandidates).getByText("来源库命中 1 个来源，当前显示 1/1 个可转配置列组。")).toBeTruthy();
    const contextCandidate = within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ });
    expect(contextCandidate.textContent).toContain("上下文 OMODA / T19C MY ICE / Germany / +2");
    expect(contextCandidate.textContent).toContain("Germany · MY 2026 · Segment SUV C");
    const contextPath = within(contextCandidate).getByLabelText("T19C MY ICE 来源库路径");
    expect(contextPath.textContent).toContain("品牌OMODA");
    expect(contextPath.textContent).toContain("市场Germany");
    expect(contextPath.textContent).toContain("年款2026");
    expect(contextPath.textContent).toContain("级别SUV C");

    fireEvent.click(within(drawer).getAllByRole("tab", { name: /配置列/ })[0]);
    openSimpleAdvancedSearch();
    const brandInput = within(drawer).getByRole("combobox", { name: "Brand" });
    fireEvent.focus(brandInput);
    const brandListbox = screen.getByRole("listbox");
    expect(brandListbox.textContent).toContain("OMODA");
    fireEvent.click(within(brandListbox).getByText("OMODA"));
    await waitFor(() => {
      expect((within(drawer).getByRole("combobox", { name: "Brand" }) as HTMLInputElement).value).toBe("OMODA");
    });
    fireEvent.click(within(drawer).getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    const brandFilteredDigestCandidates = within(drawer).getByText("Source Digest 可比组").closest(".market-scan-field") as HTMLElement;
    expect(within(brandFilteredDigestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeTruthy();
  });

  it("groups source digest candidates by source, brand, and model before selection", async () => {
    const omodaGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-omoda-model",
      title: "T19C MY ICE",
      modelName: "T19C MY ICE",
      sourceSheet: "OMODA Sheet",
    };
    const rivalGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-rival-model",
      title: "Rival C SUV",
      modelName: "Rival C SUV",
      sourceSheet: "Rival Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Rival C SUV",
        trimName: trim.trimName.replace("两驱", "Rival "),
      })),
    };
    const pdfGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-pdf-model",
      title: "PDF C SUV",
      modelName: "PDF C SUV",
      sourceSheet: "PDF Text Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "PDF C SUV",
        trimName: trim.trimName.replace("两驱", "PDF "),
      })),
    };
    const htmlGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-html-model",
      title: "HTML C SUV",
      modelName: "HTML C SUV",
      sourceSheet: "HTML Table",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "HTML C SUV",
        trimName: trim.trimName.replace("两驱", "HTML "),
      })),
    };
    const omodaDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "image_ocr",
      sourceFormat: "image_ocr",
      fileName: "germany-source.png",
      ocrEngine: "paddleocr",
      ocrEvaluation: {
        strategy: "highest_config_semantic_score",
        reason: "highest_config_semantic_score",
        candidateCount: 2,
        comparableCandidateCount: 1,
        selectedCandidateCount: 1,
        selectedEngine: "paddleocr",
        selectedEngines: ["paddleocr"],
        selectedScore: {
          semanticScore: 1,
          comparableGroupCount: 1,
          featureCount: 12,
          differenceCount: 4,
          candidateTrimCount: 3,
          totalFeatureCount: 12,
          totalDifferenceCount: 4,
          totalCandidateTrimCount: 3,
          tableShapeScore: 1,
          rowCount: 18,
          columnCount: 6,
          nonEmptyCount: 72,
        },
        selectedSheetName: "OCR Image 1",
      },
      ocrEngineCandidates: [
        {
          engine: "legacy_image_ocr",
          sourceType: "image_ocr",
          sheetName: "OCR Image 1",
          selected: false,
          comparableTableDetected: false,
          score: {
            semanticScore: 0,
            comparableGroupCount: 0,
            featureCount: 0,
            differenceCount: 0,
            candidateTrimCount: 0,
            totalFeatureCount: 0,
            totalDifferenceCount: 0,
            totalCandidateTrimCount: 0,
            tableShapeScore: 0,
            rowCount: 0,
            columnCount: 0,
            nonEmptyCount: 0,
          },
          textPreview: "T19C blurry source",
          lineCount: 1,
          message: "legacy_image_ocr OCR text did not contain comparable table rows.",
        },
        {
          engine: "paddleocr",
          sourceType: "image_ocr",
          sheetName: "OCR Image 1",
          selected: true,
          comparableTableDetected: true,
          score: {
            semanticScore: 1,
            comparableGroupCount: 1,
            featureCount: 12,
            differenceCount: 4,
            candidateTrimCount: 3,
            totalFeatureCount: 12,
            totalDifferenceCount: 4,
            totalCandidateTrimCount: 3,
            tableShapeScore: 1,
            rowCount: 18,
            columnCount: 6,
            nonEmptyCount: 72,
          },
          textPreview: "Category Feature Basic Premium Comfort Seat heating - S",
          lineCount: 2,
        },
      ],
      compareGroups: [omodaGroup],
    };
    const rivalDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "france-rival.xlsx",
      compareGroups: [rivalGroup],
    };
    const pdfDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "pdf_text",
      sourceFormat: "pdf_text",
      fileName: "text-config.pdf",
      compareGroups: [pdfGroup],
    };
    const htmlDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "tabular",
      sourceFormat: "tabular",
      fileName: "web-table.html",
      compareGroups: [htmlGroup],
    };
    const omodaSource = {
      ...buildSourceSnapshotFixture("source-omoda", "germany-source.png", omodaDigest),
      fileType: "image",
      mimeType: "image/png",
      relatedContext: {
        brand: "OMODA",
        model: "T19C MY ICE",
        market: "Germany",
        country: "Germany",
        segment: "SUV C",
        modelYear: "2026",
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
    };
    const rivalSource = {
      ...buildSourceSnapshotFixture("source-rival", "france-rival.xlsx", rivalDigest),
      relatedContext: {
        brand: "RivalBrand",
        model: "Rival C SUV",
        market: "France",
        country: "France",
        segment: "SUV C",
        modelYear: "2026",
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
    };
    const pdfSource = {
      ...buildSourceSnapshotFixture("source-pdf", "text-config.pdf", pdfDigest),
      fileType: "pdf",
      mimeType: "application/pdf",
      relatedContext: {
        brand: "PdfBrand",
        model: "PDF C SUV",
        market: "Italy",
        country: "Italy",
        segment: "SUV C",
        modelYear: "2026",
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
    };
    const htmlSource = {
      ...buildSourceSnapshotFixture("source-html", "web-table.html", htmlDigest),
      fileType: "html",
      mimeType: "text/html",
      relatedContext: {
        brand: "HtmlBrand",
        model: "HTML C SUV",
        market: "Spain",
        country: "Spain",
        segment: "SUV C",
        modelYear: "2026",
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 4,
      items: [omodaSource, rivalSource, pdfSource, htmlSource],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "SUV C" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "SUV C",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("germany-source.png");
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeTruthy();
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Rival C SUV/ })).toBeTruthy();
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：PDF C SUV/ })).toBeTruthy();
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：HTML C SUV/ })).toBeTruthy();
    });
    const sourceBrowser = within(digestCandidates).getByLabelText("Source Digest 按来源和品牌浏览");
    const omodaBrowserGroup = within(sourceBrowser).getByLabelText(/germany-source\.png .*T19C MY ICE.* Source Digest 分组/);
    const qualityFilters = within(digestCandidates).getByLabelText("Source Digest 来源类型筛选");
    expect(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：全部 4 个" })).toBeTruthy();
    expect(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：来源库 4 个" })).toBeTruthy();
    expect(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：Excel 1 个" })).toBeTruthy();
    expect(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：表格文本 1 个" })).toBeTruthy();
    expect(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：PDF 1 个" })).toBeTruthy();
    expect(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：OCR 1 个" })).toBeTruthy();
    expect((within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：价格单 0 个" }) as HTMLButtonElement).disabled).toBe(true);
    expect(omodaBrowserGroup.textContent).toContain("来源库");
    expect(omodaBrowserGroup.textContent).toContain("图片 OCR");
    expect(omodaBrowserGroup.textContent).toContain("germany-source.png");
    expect(omodaBrowserGroup.textContent).toContain("品牌OMODA");
    expect(omodaBrowserGroup.textContent).toContain("市场Germany");
    expect(omodaBrowserGroup.textContent).toContain("车型T19C MY ICE");
    expect(omodaBrowserGroup.textContent).toContain("1 车型");
    expect(omodaBrowserGroup.textContent).toContain("2 可比配置列");
    expect(omodaBrowserGroup.textContent).toContain("选用 paddleocr");
    expect(omodaBrowserGroup.textContent).toContain("可比候选 1/2");
    expect(omodaBrowserGroup.textContent).toContain("配置项12");
    expect(omodaBrowserGroup.textContent).toContain("配置列3");
    expect(omodaBrowserGroup.textContent).toContain("差异4");
    expect(omodaBrowserGroup.textContent).toContain("表格18x6");
    expect(omodaBrowserGroup.textContent).toContain(
      "OCR 对比：paddleocr 胜出；legacy_image_ocr 未识别可比表；相对 legacy_image_ocr 多识别 12 个配置项，多识别 3 个配置列，多识别 4 个差异。",
    );
    expect(omodaBrowserGroup.textContent).toContain("识别原文 2 行：Category Feature Basic Premium Comfort Seat heating - S");
    expect(omodaBrowserGroup.textContent).not.toContain("T19C blurry source");
    const rivalBrowserGroup = within(sourceBrowser).getByLabelText(/france-rival\.xlsx .*Rival C SUV.* Source Digest 分组/);
    expect(rivalBrowserGroup.textContent).toContain("Excel");
    expect(rivalBrowserGroup.textContent).toContain("品牌RivalBrand");
    expect(rivalBrowserGroup.textContent).toContain("市场France");
    expect(rivalBrowserGroup.textContent).toContain("车型Rival C SUV");
    const pdfBrowserGroup = within(sourceBrowser).getByLabelText(/text-config\.pdf .*PDF C SUV.* Source Digest 分组/);
    expect(pdfBrowserGroup.textContent).toContain("文本 PDF");
    expect(pdfBrowserGroup.textContent).toContain("品牌PdfBrand");
    expect(pdfBrowserGroup.textContent).toContain("市场Italy");
    expect(pdfBrowserGroup.textContent).toContain("车型PDF C SUV");
    const htmlBrowserGroup = within(sourceBrowser).getByLabelText(/web-table\.html .*HTML C SUV.* Source Digest 分组/);
    expect(htmlBrowserGroup.textContent).toContain("表格文本");
    expect(htmlBrowserGroup.textContent).toContain("品牌HtmlBrand");
    expect(htmlBrowserGroup.textContent).toContain("市场Spain");
    expect(htmlBrowserGroup.textContent).toContain("车型HTML C SUV");

    fireEvent.click(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：OCR 1 个" }));
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeTruthy();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：Rival C SUV/ })).toBeNull();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：PDF C SUV/ })).toBeNull();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：HTML C SUV/ })).toBeNull();
    expect(within(digestCandidates).getByLabelText("Source Digest 检索覆盖").textContent).toContain("来源1");
    expect(within(digestCandidates).getByText("来源库命中 4 个来源，当前显示 1/4 个可转配置列组。已按来源类型、当前车型 / 市场 / 来源 / 关键词或来源聚焦收窄。")).toBeTruthy();

    fireEvent.click(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：PDF 1 个" }));
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeNull();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：Rival C SUV/ })).toBeNull();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：PDF C SUV/ })).toBeTruthy();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：HTML C SUV/ })).toBeNull();

    fireEvent.click(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：表格文本 1 个" }));
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeNull();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：Rival C SUV/ })).toBeNull();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：PDF C SUV/ })).toBeNull();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：HTML C SUV/ })).toBeTruthy();

    fireEvent.click(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：Excel 1 个" }));
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeNull();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Rival C SUV/ })).toBeTruthy();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：PDF C SUV/ })).toBeNull();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：HTML C SUV/ })).toBeNull();

    fireEvent.click(within(qualityFilters).getByRole("button", { name: "筛选 Source Digest：全部 4 个" }));
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ })).toBeTruthy();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Rival C SUV/ })).toBeTruthy();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：PDF C SUV/ })).toBeTruthy();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：HTML C SUV/ })).toBeTruthy();
  });

  it("moves, restores, and clears source snapshots in the current-country trash from the floating deck", async () => {
    const sourceTrashDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "trash-source.xlsx",
      compareGroups: [
        {
          ...digest.compareGroups[0],
          groupId: "trash-source-model",
          title: "Trash Source Model",
          modelName: "Trash Source Model",
          sourceSheet: "Trash Source Sheet",
        },
      ],
    };
    const activeSource = {
      ...buildSourceSnapshotFixture("source-trash-active", "trash-source.xlsx", sourceTrashDigest),
      relatedContext: {
        ...buildSourceSnapshotFixture("source-trash-active", "trash-source.xlsx", sourceTrashDigest).relatedContext,
        market: "Germany",
        country: "Germany",
      },
    };
    const trashedSourceA = {
      ...buildSourceSnapshotFixture("source-trash-a", "trashed-source-a.xlsx", null),
      uploadStatus: "trashed",
      libraryStatus: "stored",
      inTrash: true,
      relatedContext: {
        ...buildSourceSnapshotFixture("source-trash-a", "trashed-source-a.xlsx", null).relatedContext,
        market: "Germany",
        country: "Germany",
      },
    };
    const trashedSourceB = {
      ...buildSourceSnapshotFixture("source-trash-b", "trashed-source-b.xlsx", null),
      uploadStatus: "trashed",
      libraryStatus: "stored",
      inTrash: true,
      relatedContext: {
        ...buildSourceSnapshotFixture("source-trash-b", "trashed-source-b.xlsx", null).relatedContext,
        market: "Germany",
        country: "Germany",
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockImplementation(async (options) => {
      if (typeof options !== "number" && options?.trashOnly) {
        return {
          rows: 2,
          items: [trashedSourceA, trashedSourceB],
        };
      }
      return {
        rows: 1,
        items: [activeSource],
      };
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(activeSource);

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?market=Germany"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "Trash Source Model" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Trash Source Model",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("trash-source.xlsx");
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    expect(api.getEngineeringConfigSourceSnapshot).not.toHaveBeenCalledWith("source-trash-active");
    const sourceDigestPanel = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(within(sourceDigestPanel).getByRole("button", { name: "聚焦来源 trash-source.xlsx" })).toBeTruthy();
      expect(within(sourceDigestPanel).getByLabelText(/trash-source\.xlsx .*Trash Source Model.* Source Digest 分组/)).toBeTruthy();
    });
    fireEvent.click(within(sourceDigestPanel).getByRole("button", { name: "移入来源垃圾桶 trash-source.xlsx" }));
    await waitFor(() => {
      expect(api.trashEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-trash-active", "Germany");
    });
    expect(await within(sourceDigestPanel).findByText("trash-source.xlsx 已移入 Germany 来源垃圾桶。")).toBeTruthy();

    const sourceTrashPanel = within(sourceDigestPanel).getByLabelText("来源库垃圾桶");
    expect(sourceTrashPanel.textContent).toContain("只移动 / 恢复这个国家的来源关联");
    expect(sourceTrashPanel.textContent).toContain("不影响其他国家仍在用的同一来源文件");
    fireEvent.click(within(sourceTrashPanel).getByRole("button", { name: "查看 Germany 来源垃圾桶" }));
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        country: "Germany",
        trashOnly: true,
        limit: 100,
      }));
    });
    expect(await within(sourceTrashPanel).findByText("trashed-source-a.xlsx")).toBeTruthy();
    expect(within(sourceTrashPanel).getByText("trashed-source-b.xlsx")).toBeTruthy();

    fireEvent.click(within(sourceTrashPanel).getByRole("button", { name: "恢复来源 trashed-source-a.xlsx" }));
    await waitFor(() => {
      expect(api.restoreEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-trash-a", "Germany");
    });
    expect(await within(sourceTrashPanel).findByText("trashed-source-a.xlsx 已从 Germany 来源垃圾桶恢复。")).toBeTruthy();

    fireEvent.click(within(sourceTrashPanel).getByRole("button", { name: "清空 Germany 来源垃圾桶（1 项）" }));
    expect(api.clearEngineeringConfigSourceTrash).not.toHaveBeenCalled();
    expect(await within(sourceTrashPanel).findByText("再次点击确认清空 Germany 来源垃圾桶，才会永久清空 1 项。")).toBeTruthy();
    fireEvent.click(within(sourceTrashPanel).getByRole("button", { name: "确认清空 Germany 来源垃圾桶" }));
    await waitFor(() => {
      expect(api.clearEngineeringConfigSourceTrash).toHaveBeenCalledWith("Germany");
    });
    expect(await within(sourceTrashPanel).findByText("已清空 Germany 来源垃圾桶 1 项。")).toBeTruthy();
  });

  it("creates a source digest draft with the selected trim subset", async () => {
    const trimIds = ["multi-basic", "multi-comfort", "multi-premium", "multi-elite", "multi-flagship"];
    const multiTrimGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "multi-trim-model",
      title: "Multi Trim Model",
      modelName: "Multi Trim Model",
      sourceSheet: "Multi Trim Sheet",
      trimCount: trimIds.length,
      trims: trimIds.map((trimId, index) => ({
        ...digest.compareGroups[0].trims[index % digest.compareGroups[0].trims.length],
        trimId,
        trimName: ["Basic", "Comfort", "Premium", "Elite", "Flagship"][index],
        fullTrimName: `Multi ${["Basic", "Comfort", "Premium", "Elite", "Flagship"][index]}`,
        modelName: "Multi Trim Model",
        materialNo: index < 2 ? `MAT-${index + 1}` : null,
        salesVersion: `SV-${index + 1}`,
      })),
      rows: digest.compareGroups[0].rows.map((row) => ({
        ...row,
        values: trimIds.map((trimId, index) => ({
          valueId: `${trimId}-${row.featureCode}`,
          rawValue: index % 2 === 0 ? "●" : "-",
          normalizedValue: index % 2 === 0 ? "standard" : "not_available",
          availability: index % 2 === 0 ? "STANDARD" : "NOT_AVAILABLE",
          unit: null,
          displayValue: index % 2 === 0 ? "标配" : "不配备",
        })),
      })),
    };
    const multiTrimDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "multi-trim-source.xlsx",
      summary: {
        ...digest.summary,
        candidateTrimCount: trimIds.length,
        comparableGroupCount: 1,
      },
      compareGroups: [multiTrimGroup],
    };
    const sourceSummary = {
      ...buildSourceSnapshotFixture("multi-source", "multi-trim-source.xlsx", null),
      sourceSearchMatches: ["Model Multi Trim Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: trimIds.length,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 1,
      items: [sourceSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(
      buildSourceSnapshotFixture("multi-source", "multi-trim-source.xlsx", multiTrimDigest),
    );
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "multi-source",
      groupId: "multi-trim-model",
      importBatchId: "draft-multi",
      trimIds: ["draft-basic", "draft-premium"],
      compareTrimIds: ["draft-basic", "draft-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-basic", "version-premium"],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "Multi Trim Model" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Multi Trim Model",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("multi-trim-source.xlsx");
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("multi-source");
    });
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Multi Trim Model/ })).toBeTruthy();
    });

    const trimPicker = within(digestCandidates).getByLabelText("Multi Trim Model 可创建配置列选择");
    expect(within(trimPicker).getByText("选择 2-4 个配置列")).toBeTruthy();
    expect((within(trimPicker).getByLabelText(/Basic/) as HTMLInputElement).checked).toBe(true);
    expect((within(trimPicker).getByLabelText(/Comfort/) as HTMLInputElement).checked).toBe(true);
    expect((within(trimPicker).getByLabelText(/Premium/) as HTMLInputElement).checked).toBe(true);
    expect((within(trimPicker).getByLabelText(/Elite/) as HTMLInputElement).checked).toBe(true);
    expect((within(trimPicker).getByLabelText(/Flagship/) as HTMLInputElement).checked).toBe(false);

    fireEvent.click(within(trimPicker).getByLabelText(/Comfort/));
    fireEvent.click(within(trimPicker).getByLabelText(/Flagship/));
    fireEvent.click(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Multi Trim Model/ }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith(
        "multi-source",
        "multi-trim-model",
        { trimIds: ["multi-basic", "multi-premium", "multi-elite", "multi-flagship"] },
      );
    });
    expect(screen.getByText("Multi Trim Model 已按 2 个配置列创建为可编辑配置列：2 配置列（新建 2，复用 0） · 3 配置项（新建 3，复用 0） · 写入 6 条值（新增 6，更新 0）。")).toBeTruthy();
  });

  it("lets users focus one source hit before creating a digest-backed config column", async () => {
    const sourceAGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-a-model",
      title: "Source A Model",
      modelName: "Source A Model",
      sourceSheet: "Source A Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Source A Model",
        trimId: `source-a-${trim.trimId}`,
      })),
    };
    const sourceBGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-b-model",
      title: "Source B Model",
      modelName: "Source B Model",
      sourceSheet: "Source B Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Source B Model",
        trimId: `source-b-${trim.trimId}`,
      })),
    };
    const sourceADigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "source-a.xlsx",
      compareGroups: [sourceAGroup],
    };
    const sourceBDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "source-b.xlsx",
      compareGroups: [sourceBGroup],
    };
    const sourceASummary = {
      ...buildSourceSnapshotFixture("source-a", "source-a.xlsx", null),
      sourceSearchMatches: ["Model Source A Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    const sourceBSummary = {
      ...buildSourceSnapshotFixture("source-b", "source-b.xlsx", null),
      sourceSearchMatches: ["Model Source B Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 2,
      items: [sourceASummary, sourceBSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockImplementation(async (sourceId: string) => (
      sourceId === "source-a"
        ? buildSourceSnapshotFixture("source-a", "source-a.xlsx", sourceADigest)
        : buildSourceSnapshotFixture("source-b", "source-b.xlsx", sourceBDigest)
    ));

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "Source Model" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Source Model",
      }));
      const sourceHints = within(drawer).getByLabelText("来源库轻量命中");
      expect(sourceHints.textContent).toContain("source-a.xlsx");
      expect(sourceHints.textContent).toContain("source-b.xlsx");
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-a");
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-b");
    });
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-a");
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-b");
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Source A Model/ })).toBeTruthy();
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Source B Model/ })).toBeTruthy();
    });
    const sourceDigestCoverage = within(digestCandidates).getByLabelText("Source Digest 检索覆盖");
    expect(sourceDigestCoverage.textContent).toContain("来源2");
    expect(sourceDigestCoverage.textContent).toContain("Model2");
    expect(sourceDigestCoverage.textContent).toContain("配置列4");

    const sourceAHint = within(digestCandidates).getByRole("button", { name: "聚焦来源 source-a.xlsx" });
    fireEvent.click(sourceAHint);

    expect(within(digestCandidates).getByLabelText("当前 Source Digest 来源聚焦").textContent).toContain("source-a.xlsx");
    expect(sourceDigestCoverage.textContent).toContain("来源1/2");
    expect(sourceDigestCoverage.textContent).toContain("Model1/2");
    expect(sourceDigestCoverage.textContent).toContain("配置列2/4");
    expect(sourceDigestCoverage.textContent).toContain("已按来源 source-a.xlsx 收窄");
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Source A Model/ })).toBeTruthy();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：Source B Model/ })).toBeNull();
    expect((within(digestCandidates).getByRole("button", { name: "已聚焦来源 source-a.xlsx" }) as HTMLButtonElement).getAttribute("aria-pressed")).toBe("true");

    fireEvent.click(within(digestCandidates).getByRole("button", { name: "解除来源聚焦" }));

    expect(sourceDigestCoverage.textContent).toContain("来源2");
    expect(sourceDigestCoverage.textContent).toContain("Model2");
    expect(sourceDigestCoverage.textContent).toContain("配置列4");
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Source A Model/ })).toBeTruthy();
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Source B Model/ })).toBeTruthy();
  });

  it("shows focused source digest groups even when the current page model differs", async () => {
    const runtimeGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "runtime-source-model",
      title: "Runtime Source Model",
      modelName: "Runtime Source Model",
      sourceSheet: "Runtime Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Runtime Source Model",
        trimId: `runtime-${trim.trimId}`,
      })),
    };
    const runtimeDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "runtime-source.xlsx",
      modelName: "Runtime Source Model",
      compareGroups: [runtimeGroup],
      summary: {
        ...digest.summary,
        candidateTrimCount: runtimeGroup.trimCount,
        comparableGroupCount: 1,
        featureCount: runtimeGroup.featureCount,
        differenceCount: runtimeGroup.differenceCount,
      },
    };
    const runtimeContext: EngineeringConfigSourceContext = {
      brand: "RuntimeSmoke",
      model: "Runtime Source Context",
      market: "Germany",
      country: "Germany",
      powertrain: "ICE",
      segment: "SUV C",
      modelYear: "2026",
      trimIds: [],
      salesVersionIds: [],
      contextType: "compare",
    };
    const runtimeSourceSummary = {
      ...buildSourceSnapshotFixture("runtime-source", "runtime-source.xlsx", null),
      relatedContext: runtimeContext,
      sourceSearchMatches: ["File runtime-source.xlsx"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: runtimeGroup.trimCount,
          comparableGroupCount: 1,
          featureCount: runtimeGroup.featureCount,
          differenceCount: runtimeGroup.differenceCount,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 1,
      items: [runtimeSourceSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue({
      ...buildSourceSnapshotFixture("runtime-source", "runtime-source.xlsx", runtimeDigest),
      relatedContext: runtimeContext,
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?market=Germany&model=Target%20C-SUV&powertrain=ICE&segment=SUV%20C"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "runtime-source.xlsx" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        country: "Germany",
        q: "runtime-source.xlsx",
      }));
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("runtime-source.xlsx");
    });

    fireEvent.click(within(drawer).getByRole("button", { name: "聚焦来源 runtime-source.xlsx" }));

    const digestCandidates = within(drawer).getByLabelText("来源组详情浏览");
    await waitFor(() => {
      expect(within(digestCandidates).getByLabelText("当前 Source Digest 来源聚焦").textContent).toContain("runtime-source.xlsx");
      expect(within(digestCandidates).getByLabelText("Source Digest 检索覆盖").textContent).toContain("Model1");
      expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Runtime Source Model/ })).toBeTruthy();
    });
    expect(container.querySelector(".product-config-hero")?.textContent).toContain("筛选 Target C-SUV · Germany · ICE · +1");
  });

  it("keeps all sources visible when focusing the same Source Digest model across sources", async () => {
    const sharedModelName = "Shared Source Model";
    const sourceAGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-a-shared-model",
      title: sharedModelName,
      modelName: sharedModelName,
      sourceSheet: "Source A Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: sharedModelName,
        trimId: `shared-source-a-${trim.trimId}`,
      })),
    };
    const sourceBGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "source-b-shared-model",
      title: sharedModelName,
      modelName: sharedModelName,
      sourceSheet: "Source B Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: sharedModelName,
        trimId: `shared-source-b-${trim.trimId}`,
      })),
    };
    const sourceADigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "shared-source-a.xlsx",
      compareGroups: [sourceAGroup],
    };
    const sourceBDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "shared-source-b.xlsx",
      compareGroups: [sourceBGroup],
    };
    const sourceASummary = {
      ...buildSourceSnapshotFixture("shared-source-a", "shared-source-a.xlsx", null),
      sourceSearchMatches: [`Model ${sharedModelName}`],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    const sourceBSummary = {
      ...buildSourceSnapshotFixture("shared-source-b", "shared-source-b.xlsx", null),
      sourceSearchMatches: [`Model ${sharedModelName}`],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 2,
      items: [sourceASummary, sourceBSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockImplementation(async (sourceId: string) => (
      sourceId === "shared-source-a"
        ? buildSourceSnapshotFixture("shared-source-a", "shared-source-a.xlsx", sourceADigest)
        : buildSourceSnapshotFixture("shared-source-b", "shared-source-b.xlsx", sourceBDigest)
    ));

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: sharedModelName },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("shared-source-a");
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("shared-source-b");
    });

    const directSourcePicker = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_PICKER_COMBOBOX_NAME });
    fireEvent.focus(directSourcePicker);
    const directSourceListbox = screen.getByRole("listbox");
    const crossSourceModelOption = within(directSourceListbox).getByRole("option", {
      name: new RegExp(`聚焦同名车型 · ${sharedModelName}`),
    });
    expect(crossSourceModelOption.textContent).toContain("2 来源");
    expect(crossSourceModelOption.querySelector(".version-comparison-model-option-badge")?.textContent).toBe("同名车型");
    expect(directSourceListbox.textContent).toContain(`聚焦车型 · ${sharedModelName} · shared-source-a.xlsx`);
    expect(directSourceListbox.textContent).toContain(`聚焦车型 · ${sharedModelName} · shared-source-b.xlsx`);
    fireEvent.click(crossSourceModelOption);

    expect((within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value).toBe(sharedModelName);
    await waitFor(() => {
      const sourceScope = within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL);
      expect(sourceScope.textContent).toContain("跨来源车型");
      expect(sourceScope.textContent).not.toContain("只看来源");
    });
    const coverage = within(digestCandidates).getByLabelText("Source Digest 检索覆盖");
    expect(coverage.textContent).toContain("来源2");
    expect(coverage.textContent).toContain("Model1");
    expect(coverage.textContent).toContain("配置列4");
    expect(within(digestCandidates).getAllByRole("button", { name: new RegExp(`选择 Source Digest 可比组：${sharedModelName}`) })).toHaveLength(2);

    fireEvent.focus(directSourcePicker);
    const sourceATrimOption = within(screen.getByRole("listbox"))
      .getAllByRole("option", { name: new RegExp(`暂选配置列 · ${sharedModelName}`) })
      .find((option) => option.textContent?.includes("shared-source-a.xlsx") && option.textContent?.includes("Basic"));
    expect(sourceATrimOption).toBeTruthy();
    fireEvent.click(sourceATrimOption as HTMLElement);

    fireEvent.focus(directSourcePicker);
    const sourceBTrimOption = within(screen.getByRole("listbox"))
      .getAllByRole("option", { name: new RegExp(`暂选配置列 · ${sharedModelName}`) })
      .find((option) => option.textContent?.includes("shared-source-b.xlsx") && option.textContent?.includes("Basic"));
    expect(sourceBTrimOption).toBeTruthy();
    fireEvent.click(sourceBTrimOption as HTMLElement);

    const pendingPanel = await within(drawer).findByLabelText("待生成来源配置列");
    expect(pendingPanel.textContent).toContain("已按来源拆成 2 组暂选");
    expect(pendingPanel.textContent).toContain("shared-source-a.xlsx");
    expect(pendingPanel.textContent).toContain("shared-source-b.xlsx");
    expect(pendingPanel.textContent).toContain("每组至少 2 个同来源配置列才能生成");
    expect(pendingPanel.querySelectorAll(".product-config-direct-pending__group")).toHaveLength(2);
    fireEvent.click(within(pendingPanel).getByRole("button", { name: "清空全部暂选" }));
    await waitFor(() => {
      expect(within(drawer).queryByLabelText("待生成来源配置列")).toBeNull();
      expect(within(drawer).getByText("已清空 2 组 Source Digest 暂选。")).toBeTruthy();
    });
  });

  it("lets the display deck focus missing-source evidence scope", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "查看规则推断差异：推断差异 0" })).toBeNull();

    switchSummaryMode("expert");

    expect(screen.getByRole("button", { name: "显示范围：来源问题 3 项" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "显示范围：合并格 0 项" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "显示范围：来源问题 3 项" }));

    await waitFor(() => {
      expect(screen.getByText("当前表格 3 项证据")).toBeTruthy();
    });
    expect(screen.getByText((_content, element) => element?.textContent === "来源问题6缺源值 6，缺值 0；当前范围 3 项配置需补 evidence")).toBeTruthy();
    expect(screen.getByText("来源问题 摘要")).toBeTruthy();
    expect(screen.getByRole("button", { name: "查看来源问题" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "显示范围：来源问题 3 项" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByText("缺源值 6，缺值 0，推断 0，合并格 0")).toBeTruthy();
  });

  it("lets the summary evidence health chips focus evidence scopes", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.queryByLabelText("证据健康度快捷筛选")).toBeNull();
    switchSummaryMode("expert");
    expect(screen.getByLabelText("证据健康度快捷筛选")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "查看来源问题证据：缺源值 6，缺值 0" }));

    await waitFor(() => {
      expect(screen.getByText("当前表格 3 项证据")).toBeTruthy();
    });
    expect(screen.getByText("来源问题 摘要")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "来源问题6缺源值 6，缺值 0；当前范围 3 项配置需补 evidence")).toBeTruthy();
    expect(screen.getByRole("button", { name: "查看来源问题证据：缺源值 6，缺值 0" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "查看合并格证据：合并格 0" }));

    await waitFor(() => {
      expect(screen.getByText("当前表格 0 项证据")).toBeTruthy();
    });
    expect(screen.getByText("合并格展开 摘要")).toBeTruthy();
  });

  it("separates missing value cells from values that only lack source evidence", async () => {
    const digestWithMissingValue: EngineeringConfigSourceDigest = {
      ...digest,
      summary: {
        ...digest.summary,
        featureCount: 1,
        differenceCount: 1,
      },
      compareGroups: [
        {
          ...digest.compareGroups[0],
          featureCount: 1,
          differenceCount: 1,
          summary: {
            ...digest.compareGroups[0].summary,
            totalFeatures: 1,
            shownFeatures: 1,
            commonSameCount: 0,
            differentValueCount: 0,
            uniqueFeatureCount: 0,
            partialAvailableCount: 0,
            uniqueOrPartialCount: 0,
            missingOrUnknownCount: 1,
            confirmedDifferenceCount: 0,
            rawConfirmedDifferenceCount: 0,
            inferredDifferenceCount: 0,
            availabilityDifferentCount: 0,
            differenceCount: 1,
            categoryCounts: { "Data quality": 1 },
            differenceCategories: ["Data quality"],
          },
          rows: [
            {
              category: "Data quality",
              featureCode: "nullable_value_source",
              featureName: "Nullable value source",
              comparisonType: "MISSING_OR_UNKNOWN",
              uniqueTrimIds: [],
              businessNote: "一个 trim 缺值，另一个 trim 有值但缺来源证据。",
              values: [
                null,
                {
                  valueId: "premium-nullable-source",
                  rawValue: "●",
                  normalizedValue: "standard",
                  availability: "STANDARD",
                  unit: null,
                  displayValue: "标配",
                },
              ],
            },
          ],
        },
      ],
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValue(digestWithMissingValue);

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 1/1 配置行")).toBeTruthy();
    expect(screen.queryByText("空值 / 缺失需核对；推断 / 合并格见更多证据筛选")).toBeNull();
    expect(screen.queryByText("缺源值 1，缺值 1，推断 0，合并格 0")).toBeNull();
    expect(screen.queryByRole("button", { name: "查看来源问题证据：缺源值 1，缺值 1" })).toBeNull();
    switchSummaryMode("expert");
    expect(screen.getByText("缺源值 1，缺值 1，推断 0，合并格 0")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "查看来源问题证据：缺源值 1，缺值 1" }));

    await waitFor(() => {
      expect(screen.getByText("当前表格 1 项证据")).toBeTruthy();
    });
    expect(screen.getByText((_content, element) => element?.textContent === "来源问题2缺源值 1，缺值 1；当前范围 1 项配置需补 evidence")).toBeTruthy();
  });

  it("opens the source panel from business action guidance", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");

    fireEvent.click(await screen.findByRole("button", { name: /打开 Premium 的来源入口/ }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    expect(screen.getByText("当前关联上下文")).toBeTruthy();
    const contextSummary = screen.getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("目标口径");
    expect(contextSummary?.textContent).toContain("身份锚点 物料号 T71607V**MM0001 / Sales version Premium");
    expect(contextSummary?.textContent).toContain("已选 2 配置列");
    expect(screen.getByText((_content, element) => element?.textContent === "候选配置列 0")).toBeTruthy();
    expect(screen.getAllByText((_content, element) => element?.textContent === "已选配置列 2/4").length).toBeGreaterThan(0);
  });

  it("narrows the source context to base and focused target without removing selected trims", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");

    fireEvent.click(await screen.findByRole("button", { name: /打开 Luxury 的来源入口/ }));

    const contextSummary = screen.getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("目标口径");
    expect(contextSummary?.textContent).toContain("身份锚点 物料号 T71607V**MM0001 / Sales version Luxury");
    expect(contextSummary?.textContent).toContain("已选 2 配置列");
    expect(screen.getAllByText((_content, element) => element?.textContent === "已选配置列 3/4").length).toBeGreaterThan(0);
  });

  it("defaults to simple summary mode and keeps mode controls in the floating display deck", async () => {
    localStorage.setItem("jato_product_config_summary_mode", "expert");
    localStorage.setItem("jato_product_config_summary_mode_v2", "expert");
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.getByRole("heading", { name: "车型配置表对比" })).toBeTruthy();
    expect(screen.getByText("像看 xlsx 一样选择 2-4 个配置列，指定基准列后核对全部配置行、差异行和来源证据。")).toBeTruthy();
    const heroModeSwitch = screen.getByLabelText("页面配置对比模式");
    expect(within(heroModeSwitch).getByRole("button", { name: "简易" }).getAttribute("aria-pressed")).toBe("true");
    expect(within(heroModeSwitch).getByRole("button", { name: "专家" }).getAttribute("aria-pressed")).toBe("false");
    expect(localStorage.getItem("jato_product_config_summary_mode")).toBe("expert");
    expect(localStorage.getItem("jato_product_config_summary_mode_v2")).toBe("expert");
    expect(localStorage.getItem("jato_product_config_summary_mode_v3")).toBe("simple");
    const readOnlyDigestControlButton = screen.getByRole("button", { name: /添加配置列 \/ 显示/ });
    expect(readOnlyDigestControlButton.textContent).not.toContain("编辑");
    expect(screen.queryByLabelText("Excel 配置表状态")).toBeNull();
    expect(screen.queryByLabelText("Excel 列对比结果")).toBeNull();
    expect(await screen.findByLabelText("AI 配置对比摘要")).toBeTruthy();
    expect(screen.queryByLabelText("配置摘要模式")).toBeNull();
    expect(screen.queryByLabelText("Excel 首屏速读")).toBeNull();
    expect(screen.queryByLabelText("目标配置列结论抽屉")).toBeNull();
    expect(container.querySelector(".business-summary-panel")?.classList.contains("is-simple")).toBe(true);
    expect(container.querySelector(".comparison-summary")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    const modeSwitch = screen.getByLabelText("显示控制中的当前视图模式");
    const modeOptions = within(modeSwitch).getByLabelText("显示控制中切换配置对比视图模式");
    const simpleButton = within(modeOptions).getAllByRole("button").find((button) => button.textContent?.includes("简易模式"));
    const expertButton = within(modeOptions).getAllByRole("button").find((button) => button.textContent?.includes("专家模式"));
    expect(simpleButton?.getAttribute("aria-pressed")).toBe("true");
    expect(simpleButton?.textContent).toContain("AI 结论");
    expect(simpleButton?.textContent).not.toContain("快速解读");
    expect(modeSwitch.textContent).toContain("AI 结论优先，只保留完整配置行、差异行、目标列和大类导航；高级诊断在专家模式。");
    const configColumnTab = screen.getAllByRole("tab").find((tab) => tab.textContent?.startsWith("配置列"));
    expect(configColumnTab).toBeTruthy();
    fireEvent.click(configColumnTab as HTMLElement);
    expect(screen.getByRole("combobox", { name: "搜索并添加配置列" })).toBeTruthy();
    const unifiedSearchStatus = screen.getByLabelText("统一搜索覆盖状态");
    expect(unifiedSearchStatus.textContent).toContain("下拉统一搜索");
    expect(unifiedSearchStatus.textContent).toContain("库按来源 → 车型 → 配置列组织");
    expect(unifiedSearchStatus.textContent).toContain("上传人");
    expect(unifiedSearchStatus.textContent).toContain("正式库");
    expect(unifiedSearchStatus.textContent).toContain("来源库");
    expect(unifiedSearchStatus.textContent).toContain("同名范围");
    expect(screen.queryByRole("combobox", { name: "配置列 / Configuration" })).toBeNull();
    openSimpleAdvancedSearch();
    expect(screen.getByRole("combobox", { name: "配置列 / Configuration" })).toBeTruthy();
    expect(screen.getByText("同车型配置列组")).toBeTruthy();
    expect(screen.getByText("候选配置列 / 物料号")).toBeTruthy();
    expect(screen.queryByText("候选 Trim")).toBeNull();
    expect(screen.queryByLabelText("Premium 业务解读")).toBeNull();
    expect(screen.queryByLabelText("Premium 业务重点分组")).toBeNull();
    expect(screen.queryByLabelText("Premium 简易业务重点")).toBeNull();
    expect(screen.queryByLabelText("当前基准对比速览")).toBeNull();
    expect(screen.queryByLabelText("相邻版本升级路径")).toBeNull();
    expect(screen.queryByLabelText("Premium 结论草稿")).toBeNull();

    switchSummaryMode("expert");
    await waitFor(() => {
      expect(localStorage.getItem("jato_product_config_summary_mode_v3")).toBe("expert");
      expect(within(screen.getByLabelText("页面配置对比模式")).getByRole("button", { name: "专家" }).getAttribute("aria-pressed")).toBe("true");
    });

    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    const expertModeOptions = within(screen.getByLabelText("显示控制中的当前视图模式")).getByLabelText("显示控制中切换配置对比视图模式");
    expect(screen.getByRole("heading", { name: "车型配置表对比" })).toBeTruthy();
    const activeExpertButton = within(expertModeOptions).getAllByRole("button").find((button) => button.textContent?.includes("专家模式"));
    expect(activeExpertButton?.getAttribute("aria-pressed")).toBe("true");
    const expertConfigColumnTab = screen.getAllByRole("tab").find((tab) => tab.textContent?.startsWith("配置列"));
    expect(expertConfigColumnTab).toBeTruthy();
    fireEvent.click(expertConfigColumnTab as HTMLElement);
    expect(screen.getByRole("combobox", { name: "配置列 / Configuration" })).toBeTruthy();
    expect(screen.getByText("同车型配置列组")).toBeTruthy();
    expect(screen.getByText("候选配置列 / 物料号")).toBeTruthy();
    expect(screen.queryByText("Trim / Configuration")).toBeNull();
    expect(screen.queryByText("同车型 Trim 组")).toBeNull();
    expect(screen.queryByText("候选 Trim / 物料号")).toBeNull();
    const expertInlineModeSwitch = screen.getByLabelText("配置摘要模式");
    expect(expertInlineModeSwitch.textContent).toContain("专家模式");
    expect(expertInlineModeSwitch.textContent).toContain("显示高级诊断、规则推断、升级路径和来源样本。");
    expect(screen.getByLabelText("配置对比摘要").classList.contains("product-config-table-status")).toBe(false);
    const expertBusinessBrief = screen.getByLabelText("Premium 业务解读");
    expect(expertBusinessBrief.textContent).toContain("业务解读");
    expect(expertBusinessBrief.textContent).toContain("Premium 相比 Basic");
    expect(screen.getByLabelText("Premium 业务重点分组")).toBeTruthy();
    expect(screen.getByRole("button", { name: "聚焦 Premium 的 增配重点" })).toBeTruthy();
    expect(screen.getByLabelText("相邻版本升级路径")).toBeTruthy();
    expect(screen.getByLabelText("Premium 结论草稿")).toBeTruthy();
  });

  it("collapses deterministic expert blocks once an AI conclusion is available", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "Premium",
          headline: "Premium 相比 Basic 的主要升级",
          mainUpgrades: ["泊车辅助：新增 Blind spot"],
          replacementsOrReductions: ["轮胎：18 inch 替换为 20 inch"],
          evidenceStatus: ["引用卖点前需要点开 evidence 核对。"],
          recommendedUse: "适合做销售话术初稿。",
        },
      ],
      usage: {
        provider: "astrbot",
        model: "astrbot-llm",
        status: "ok",
        promptTokens: 80,
        completionTokens: 40,
        totalTokens: 120,
      },
    });

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("Premium 相比 Basic 的主要升级", undefined, { timeout: 3500 })).toBeTruthy();

    switchSummaryMode("expert");

    await waitFor(() => {
      expect(screen.getByLabelText("高级规则诊断已收起").textContent).toContain("AI 结论是业务主视图");
    });
    expect(screen.queryByLabelText("Premium 业务解读")).toBeNull();
    expect(screen.queryByLabelText("相邻版本升级路径")).toBeNull();
    expect(screen.queryByRole("button", { name: "复制全部结论" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "查看高级规则诊断" }));

    expect(screen.getByLabelText("Premium 业务解读")).toBeTruthy();
    expect(screen.getByLabelText("相邻版本升级路径")).toBeTruthy();
    expect(screen.getByLabelText("高级规则诊断说明").textContent).toContain("对外话术以 AI 结论为准");
    expect(screen.getByRole("button", { name: "收起高级规则诊断" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "复制全部结论" })).toBeNull();
  });

  it("shows runtime AI summary readiness in the FloatingDeck display panel", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    expect(api.getEngineeringConfigBusinessSummaryReadiness).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    expect(api.getEngineeringConfigBusinessSummaryReadiness).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));

    const aiStatus = await screen.findByLabelText("AI 摘要运行状态") as HTMLDetailsElement;
    expect(aiStatus.open).toBe(false);
    expect(aiStatus.textContent).toContain("展开后检查 runtime");
    expect(api.getEngineeringConfigBusinessSummaryReadiness).not.toHaveBeenCalled();

    fireEvent.click(within(aiStatus).getByText("AI 摘要状态"));
    expect(aiStatus.open).toBe(true);
    await waitFor(() => {
      expect(aiStatus.textContent).toContain("AI 摘要可用");
    });
    expect(aiStatus.textContent).toContain("deepseek");
    expect(aiStatus.textContent).toContain("deepseek-chat");
    expect(aiStatus.textContent).toContain("api.deepseek.com");
    expect(aiStatus.textContent).toContain("未参与摘要");
    expect(aiStatus.textContent).toContain("0/64");
    expect(aiStatus.textContent).toContain("Compare 摘要复用 AstrBot provider 配置，但不走本地 AstrBot runtime");
    expect(aiStatus.textContent).toContain("不是 Source Digest pipeline 持久摘要");
    expect(api.getEngineeringConfigBusinessSummaryReadiness).toHaveBeenCalledTimes(1);
  });

  it("remembers the selected summary mode across page entries", async () => {
    localStorage.setItem("jato_product_config_summary_mode_v3", "expert");
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: "查看本地 xlsx 样例" }));

    expect(await screen.findByLabelText("配置对比摘要")).toBeTruthy();
    expect(screen.getByRole("heading", { name: "车型配置表对比" })).toBeTruthy();
    const inlineModeSwitch = screen.getByLabelText("配置摘要模式");
    expect(inlineModeSwitch.textContent).toContain("专家模式");
    expect(localStorage.getItem("jato_product_config_summary_mode_v3")).toBe("expert");
  });

  it("lets AI summary items focus the matching config table row", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "Premium",
          headline: "Premium 相比 Basic 新增了盲点监测。",
          mainUpgrades: ["安全：新增 Blind spot"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用前需要点开 evidence 核对。"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "premium:ADDED:blind_spot",
              featureCode: "blind_spot",
              category: "Safety",
              reason: "AI 摘要中的 Blind spot 来自 blind_spot 配置差异。",
            },
          ],
          recommendedUse: "适合作为配置差异口径，发布前核对证据。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 80,
        completionTokens: 40,
        totalTokens: 120,
      },
    });

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(await screen.findByText("Premium 相比 Basic 新增了盲点监测。", undefined, { timeout: 3500 })).toBeTruthy();
    const aiPreview = screen.getByLabelText("AI 结论和证据：Premium");
    const aiPreviewDetails = aiPreview.querySelector("details") as HTMLDetailsElement | null;
    expect(aiPreviewDetails?.open).toBe(false);
    expect(within(aiPreview).getByRole("button", { name: "核对 AI 结论证据：Premium" })).toBeTruthy();
    fireEvent.click(within(aiPreview).getByText("展开 AI 要点"));
    expect(aiPreviewDetails?.open).toBe(true);
    expect(within(aiPreview).getByText("安全：新增 Blind spot")).toBeTruthy();
    expect(within(aiPreview).queryByRole("button", { name: "定位 AI 摘要配置行：Premium 安全：新增 Blind spot" })).toBeNull();

    const focusButton = within(aiPreview).getByRole("button", { name: "定位 AI 结论配置行：Premium" });
    fireEvent.click(focusButton);

    await waitFor(() => {
      const focusedRow = document.getElementById("config-feature-blind-spot");
      expect(focusedRow?.classList.contains("compare-row-active")).toBe(true);
      expect(focusedRow?.getAttribute("aria-selected")).toBe("true");
    });
    expect(window.HTMLElement.prototype.scrollIntoView).toHaveBeenCalled();
    expect(screen.getByLabelText("配置表范围状态").textContent).toContain("Safety");
    expect(screen.getByLabelText("配置表范围状态").textContent).toContain("Premium");
  });

  it("opens source evidence from a visible AI summary item", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "Premium",
          headline: "Premium 相比 Basic 新增了盲点监测。",
          mainUpgrades: ["安全：新增 Blind spot"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用前需要点开 evidence 核对。"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "premium:ADDED:blind_spot",
              featureCode: "blind_spot",
              category: "Safety",
              reason: "AI 摘要中的 Blind spot 来自 blind_spot 配置差异。",
            },
          ],
          recommendedUse: "适合作为配置差异口径，发布前核对证据。",
        },
        {
          targetTrimId: "luxury",
          targetLabel: "Luxury",
          headline: "Luxury 相比 Basic 升级了音响配置。",
          mainUpgrades: ["音响：Speaker 升级为 8"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用卖点前需要核对来源坐标。"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "luxury:VALUE_CHANGED:speaker",
              featureCode: "speaker",
              category: "Infotainment",
              reason: "AI 摘要中的 Speaker 来自 speaker 配置差异。",
            },
          ],
          recommendedUse: "适合作为音响升级卖点草稿，发布前核对证据。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 120,
        completionTokens: 80,
        totalTokens: 200,
      },
    });

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("Luxury 相比 Basic 升级了音响配置。")).toBeTruthy();
    const premiumCard = screen.getByLabelText("AI 结论和证据：Premium");
    const premiumDetails = premiumCard.querySelector("details") as HTMLDetailsElement | null;
    expect(premiumDetails?.open).toBe(false);
    expect(within(premiumCard).getByRole("button", { name: "核对 AI 结论证据：Premium" })).toBeTruthy();
    fireEvent.click(within(premiumCard).getByText("展开 AI 要点"));
    expect(premiumDetails?.open).toBe(true);
    expect(within(premiumCard).getByText("安全：新增 Blind spot")).toBeTruthy();
    expect(within(premiumCard).queryByRole("button", { name: "查看 AI 摘要证据：Premium 安全：新增 Blind spot" })).toBeNull();

    const luxuryCard = screen.getByLabelText("AI 结论和证据：Luxury");
    const luxuryDetails = luxuryCard.querySelector("details") as HTMLDetailsElement | null;
    expect(luxuryDetails?.open).toBe(false);
    expect(within(luxuryCard).getByText("展开 AI 要点")).toBeTruthy();
    fireEvent.click(within(luxuryCard).getByRole("button", { name: "核对 AI 结论证据：Luxury" }));

    const drawer = await screen.findByRole("dialog", { name: "配置来源证据" });
    expect(within(drawer).getAllByText("AI 摘要中的 Speaker 来自 speaker 配置差异。").length).toBeGreaterThan(0);
    expect(within(drawer).getAllByText("Speaker").length).toBeGreaterThan(0);
    expect(within(drawer).getAllByText("T19C MY ICE").length).toBeGreaterThan(0);
    expect(within(drawer).getAllByText("E22").length).toBeGreaterThan(0);
  });

  it("keeps full mode controls in the floating display deck and exposes a lightweight page switch", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.queryByLabelText("配置对比视图模式")).toBeNull();
    const pageModeSwitch = screen.getByLabelText("页面配置对比模式");
    expect(within(pageModeSwitch).getByRole("button", { name: "简易" }).getAttribute("aria-pressed")).toBe("true");
    await waitFor(() => {
      expect(container.querySelector(".comparison-container")).toBeTruthy();
    });

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));

    const drawerViewStatus = screen.getByLabelText("显示控制中的当前视图模式");
    expect(drawerViewStatus.textContent).toContain("简易模式");
    expect(drawerViewStatus.textContent).toContain("AI 结论优先，只保留完整配置行、差异行、目标列和大类导航；高级诊断在专家模式。");
    expect(screen.getByLabelText("显示控制中切换配置对比视图模式")).toBeTruthy();
    const displayPanel = drawerViewStatus.closest(".deck-panel-grid") as HTMLElement;
    const advancedScope = displayPanel.querySelector(".comparison-drawer-advanced-scope") as HTMLDetailsElement;
    expect(advancedScope).toBeNull();
    expect(within(displayPanel).queryByText("更多证据筛选")).toBeNull();
    expect(within(displayPanel).getByLabelText("字段映射表导入").textContent).toContain("导入配置字段映射表");
    expect(within(displayPanel).getByText("来源预览只读")).toBeTruthy();
    expect((within(displayPanel).getByRole("button", { name: "预览不可编辑" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByRole("button", { name: "显示范围：全部配置行 3" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.queryByRole("button", { name: /显示范围：规则推断行/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /显示范围：来源问题行/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /显示范围：合并格行/ })).toBeNull();
    expect(within(displayPanel).getByText("简易模式只保留“全部配置行 / 差异行”；规则推断、来源问题、合并格、待确认和共同配置请切到专家模式。")).toBeTruthy();

    switchSummaryMode("expert");
    await waitFor(() => {
      expect(within(screen.getByLabelText("页面配置对比模式")).getByRole("button", { name: "专家" }).getAttribute("aria-pressed")).toBe("true");
    });

    const pageModeOptions = within(screen.getByLabelText("显示控制中的当前视图模式")).getByLabelText("显示控制中切换配置对比视图模式");
    const pageExpertButton = within(pageModeOptions).getAllByRole("button").find((button) => button.textContent?.includes("专家模式"));
    expect(pageExpertButton?.getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByLabelText("显示控制中的当前视图模式").textContent).toContain("专家模式");
    expect(screen.queryByText("更多证据筛选")).toBeNull();
    expect(screen.getByLabelText("字段映射表导入")).toBeTruthy();
    expect(screen.getByRole("button", { name: /显示范围：规则推断/ })).toBeTruthy();
    expect(screen.getByRole("button", { name: /显示范围：来源问题/ })).toBeTruthy();
    expect(screen.getByRole("button", { name: /显示范围：合并格/ })).toBeTruthy();
    expect(screen.getByLabelText("相邻版本升级路径")).toBeTruthy();
    expect(screen.getByText("当前表格 3 项配置")).toBeTruthy();
    expect(screen.getByRole("button", { name: "显示范围：全部配置 3 项" }).getAttribute("aria-pressed")).toBe("true");
  });

  it("keeps simple mode focused on source, AI summary, and the config table", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    const simpleHero = container.querySelector(".product-config-hero");
    const simpleSelectedStrip = container.querySelector(".product-config-selected-strip");
    const simpleSummaryStrip = container.querySelector(".comparison-summary");
    const simpleTable = await waitFor(() => {
      const table = container.querySelector(".comparison-container");
      expect(table).toBeTruthy();
      return table as HTMLElement;
    });
    const simpleDigest = container.querySelector(".product-config-local-digest");
    const simpleIdentityNotes = container.querySelector(".product-config-identity-notes");
    expect(simpleHero?.classList.contains("is-compact")).toBe(true);
    expect(simpleSelectedStrip?.classList.contains("is-compact")).toBe(true);
    expect(simpleSelectedStrip?.classList.contains("is-collapsible")).toBe(true);
    const simpleSelectedDetails = simpleSelectedStrip?.querySelector(".product-config-selected-strip-details") as HTMLDetailsElement | null;
    expect(simpleSelectedDetails).toBeTruthy();
    expect(simpleSelectedDetails?.open).toBe(false);
    expect(simpleSelectedDetails?.textContent).toContain("已选配置列");
    expect(simpleSelectedDetails?.textContent).toContain("3/4");
    expect(simpleSelectedDetails?.textContent).toContain("展开查看配置列和来源");
    expect(simpleSelectedDetails?.querySelector("button")).toBeNull();
    expect(simpleSelectedDetails?.querySelector(".product-config-trim-actions")).toBeNull();
    expect(simpleSelectedDetails?.querySelector(".product-config-trim-card")).toBeNull();
    openSimpleSelectedStrip();
    expect(simpleSelectedDetails?.open).toBe(true);
    const simpleCompactIdentity = simpleSelectedStrip?.querySelector(".product-config-trim-card-compact-meta");
    expect(simpleCompactIdentity?.textContent).toContain("本品");
    expect(simpleCompactIdentity?.textContent).toContain("物料号");
    expect(simpleTable).toBeTruthy();
    expect(simpleSummaryStrip).toBeNull();
    expect(await screen.findByLabelText("AI 配置对比摘要")).toBeTruthy();
    const simpleSummary = container.querySelector(".business-summary-panel");
    expect(simpleSummary?.classList.contains("is-simple")).toBe(true);
    expect(screen.queryByLabelText("Excel 首屏速读")).toBeNull();
    expect(simpleDigest).toBeNull();
    expect(container.querySelector(".product-config-category-nav")).toBeNull();
    const simpleTableControls = simpleTable.querySelector(".comparison-simple-controls") as HTMLDetailsElement | null;
    expect(simpleTableControls).toBeTruthy();
    expect(simpleTableControls?.open).toBe(false);
    expect(simpleTableControls?.textContent).toContain("筛选 / 目标列");
    expect(simpleTableControls?.textContent).toContain("全部配置行 3/3");
    expect(simpleIdentityNotes).toBeNull();

    switchSummaryMode("expert");

    await waitFor(() => {
      const expertSelectedStrip = container.querySelector(".product-config-selected-strip");
      const expertHero = container.querySelector(".product-config-hero");
      const expertSummaryStrip = container.querySelector(".comparison-summary");
      const expertTable = container.querySelector(".comparison-container");
      const expertSummary = container.querySelector(".business-summary-panel");
      const expertDigest = container.querySelector(".product-config-local-digest");
      const expertCategoryNav = container.querySelector(".product-config-category-nav");
      const expertIdentityNotes = container.querySelector(".product-config-identity-notes");
      expect(expertHero?.classList.contains("is-compact")).toBe(false);
      expect(expertSelectedStrip?.classList.contains("is-compact")).toBe(false);
      expect(expertSelectedStrip?.classList.contains("is-collapsible")).toBe(false);
      expect(expertSelectedStrip?.querySelector(".product-config-selected-strip-details")).toBeNull();
      expect(expertSummaryStrip?.classList.contains("is-compact")).toBe(false);
      expect(expertSelectedStrip?.querySelector(".product-config-trim-badges")?.textContent).toContain("本品");
      expect(Boolean(expertTable && expertSummary && (expertSummary.compareDocumentPosition(expertTable) & Node.DOCUMENT_POSITION_FOLLOWING))).toBe(true);
      expect(Boolean(expertTable && expertDigest && (expertDigest.compareDocumentPosition(expertTable) & Node.DOCUMENT_POSITION_FOLLOWING))).toBe(true);
      expect(Boolean(expertTable && expertCategoryNav && (expertCategoryNav.compareDocumentPosition(expertTable) & Node.DOCUMENT_POSITION_FOLLOWING))).toBe(true);
      expect(Boolean(expertTable && expertIdentityNotes && (expertIdentityNotes.compareDocumentPosition(expertTable) & Node.DOCUMENT_POSITION_FOLLOWING))).toBe(true);
    });
  });

  it("uses a compact table legend in simple mode and the full legend in expert mode", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    const simpleLegend = await screen.findByLabelText("配置值与证据图例") as HTMLDetailsElement;
    expect(simpleLegend.tagName.toLowerCase()).toBe("details");
    expect(simpleLegend.open).toBe(false);
    expect(within(simpleLegend).getByText("图例 / 证据说明")).toBeTruthy();
    openSimpleTableControls();
    const simpleFilter = await screen.findByLabelText("差异类型筛选");
    const simplePrimaryFilter = simpleFilter.querySelector(".comparison-type-filter-primary") as HTMLElement;
    expect(within(simpleFilter).getByRole("button", { name: /全部配置行 3/ })).toBeTruthy();
    expect(within(simpleFilter).getByRole("button", { name: /差异行 3/ })).toBeTruthy();
    expect(within(simpleFilter).queryByText("更多行筛选")).toBeNull();
    expect(within(simpleFilter).queryByText("4 个细分行筛选")).toBeNull();
    expect(within(simpleFilter).queryByText("新增、减少、值变化、规则推断和来源证据")).toBeNull();
    expect(simpleFilter.querySelector(".comparison-advanced-filter-panel")).toBeNull();
    expect(within(simplePrimaryFilter).queryByRole("button", { name: /新增配置/ })).toBeNull();
    expect(document.querySelector(".comparison-table")?.classList.contains("comparison-table--matrix")).toBe(true);
    expect(screen.queryByRole("columnheader", { name: "大类" })).toBeNull();
    expect(screen.queryByRole("columnheader", { name: "差异类型" })).toBeNull();
    expect(screen.queryByRole("columnheader", { name: "业务备注" })).toBeNull();
    expect(screen.queryByLabelText("Safety 当前大类差异摘要")).toBeNull();
    const simpleEvidenceCell = screen.getByRole("button", {
      name: /查看 Basic Blind spot 的配置来源/,
    });
    expect(simpleEvidenceCell.textContent).toContain("不配备");
    expect(simpleEvidenceCell.querySelector(".compare-cell-evidence-marker")?.textContent).toBe("!");
    expect(simpleEvidenceCell.classList.contains("compare-cell--evidence-compact")).toBe(true);

    switchSummaryMode("expert");

    await waitFor(() => {
      const expertLegend = screen.getByLabelText("配置值与证据图例");
      expect(expertLegend.tagName.toLowerCase()).toBe("section");
      expect(expertLegend.textContent).toContain("规则推断，不是 Excel 原文");
      const expertFilter = screen.getByLabelText("差异类型筛选");
      expect(within(expertFilter).queryByText("更多行筛选")).toBeNull();
      expect(within(expertFilter).getByRole("button", { name: /新增配置/ })).toBeTruthy();
      expect(document.querySelector(".comparison-table")?.classList.contains("comparison-table--matrix")).toBe(false);
      expect(screen.getByRole("columnheader", { name: "大类" })).toBeTruthy();
      expect(screen.getByRole("columnheader", { name: "差异类型" })).toBeTruthy();
      expect(screen.getByRole("columnheader", { name: "业务备注" })).toBeTruthy();
      expect(screen.getByLabelText("Safety 当前大类差异摘要")).toBeTruthy();
      expect(screen.getByRole("button", { name: /查看 Basic Blind spot 的配置来源/ }).textContent).toBe("缺源");
    });
  });

  it("switches base and target from the adjacent version upgrade path", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    const upgradePath = screen.getByLabelText("相邻版本升级路径");
    expect(upgradePath.textContent).toContain("Basic → Premium");
    expect(upgradePath.textContent).toContain("Premium → Luxury");

    fireEvent.click(screen.getByRole("button", { name: "查看 Premium 到 Luxury 的相邻版本差异" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Luxury · 3 项差异")).toBeTruthy();
    });
    const analysisScope = screen.getByLabelText("当前配置分析口径");
    expect(analysisScope.textContent).toContain("基准Premium");
    expect(analysisScope.textContent).toContain("目标聚焦Luxury");
    expect(screen.getByText("差异项 · 目标 Luxury 摘要")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "清空分析口径" }));

    await waitFor(() => {
      expect(screen.getByLabelText("相邻版本升级路径").textContent).toContain("Basic → Premium");
    });
    const resetUpgradePath = screen.getByLabelText("相邻版本升级路径");
    expect(resetUpgradePath.textContent).toContain("Premium → Luxury");
    expect(resetUpgradePath.textContent).not.toContain("Premium → Basic");
  });

  it("keeps the summary scope aligned with the selected category", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    selectSimpleTableCategory("Wheel");

    await waitFor(() => {
      expectTableRangeStatusParts(["大类 Wheel", "当前展示 1/3 配置行"]);
    });
    expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();
    expect(screen.queryByText("Wheel Excel 对比导读")).toBeNull();
    expect(tableRangeMetricText("当前展示行")).toContain("1配置行");
    expect((screen.getByRole("combobox", { name: "配置大类" }) as HTMLInputElement).value).toBe("Wheel");
    expect(screen.queryByRole("button", { name: /全部大类，当前范围 3 配置行/ })).toBeNull();
  });

  it("lets business summary category chips focus the table category", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    fireEvent.click(await screen.findByRole("button", {
      name: "聚焦 Premium 的 Wheel 差异大类，目标差异 1 项",
    }));

    await waitFor(() => {
      expect(screen.getByText("当前大类 Wheel · 1 项配置")).toBeTruthy();
    });
    expect(screen.getByText("Wheel 业务摘要")).toBeTruthy();
    expect(screen.getByRole("button", { name: /Wheel，当前范围 1 项配置/ }).classList.contains("is-active")).toBe(true);
  });

  it("lets baseline concentrated category chips focus the table category", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    const baselineCategoryButton = await waitFor(() => {
      const button = Array.from(container.querySelectorAll(".business-summary-baseline-action"))
        .find((element) => element.textContent?.includes("Wheel"));
      expect(button).toBeTruthy();
      return button as HTMLElement;
    });

    fireEvent.click(baselineCategoryButton);

    await waitFor(() => {
      expect(screen.getByText("当前大类 Wheel · 1 项配置")).toBeTruthy();
    });
    expect(screen.getByText("Wheel 业务摘要")).toBeTruthy();
    expect(screen.getByRole("button", { name: /Wheel，当前范围 1 项配置/ }).classList.contains("is-active")).toBe(true);
  });

  it("focuses table rows to a single target trim from the business summary", async () => {
    const threeTrimDigest = buildThreeTrimDigest();
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(threeTrimDigest);
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    fireEvent.click(await screen.findByRole("button", { name: "从业务摘要聚焦 Premium 差异" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 2 项差异")).toBeTruthy();
    });
    expect(tableRangeMetricText("当前展示")).toContain("2项差异");
    expect(screen.getByText("差异项 · 目标 Premium 摘要")).toBeTruthy();
    expect(screen.getByText(/Basic 作为基准列，当前聚焦 差异项 · 目标 Premium，当前对比 1 个目标配置列/)).toBeTruthy();
    expect(screen.getAllByText("Blind spot").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Wheel size").length).toBeGreaterThan(0);
    expect(screen.queryByText("Speaker")).toBeNull();
    expect(screen.getByRole("button", { name: "取消业务摘要中 Premium 目标聚焦" })).toBeTruthy();

    fireEvent.click(within(screen.getByLabelText("当前表格口径")).getByRole("button", { name: "恢复全部配置" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 3 项配置")).toBeTruthy();
    });
    expect(screen.getByText(/Basic 作为基准列，当前聚焦 目标 Premium，当前对比 1 个目标配置列/)).toBeTruthy();
    expect(screen.getAllByText("Speaker").length).toBeGreaterThan(0);
  });

  it("shows a page-level analysis scope and lets users clear target focus", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();

    switchSummaryMode("expert");
    fireEvent.click(await screen.findByRole("button", { name: "从业务摘要聚焦 Premium 差异" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 2 项差异")).toBeTruthy();
    });
    let analysisScope = screen.getByLabelText("当前配置分析口径");
    expect(analysisScope.textContent).toContain("基准Basic");
    expect(analysisScope.textContent).toContain("目标聚焦Premium");
    expect(analysisScope.textContent).toContain("范围差异项");
    expect(analysisScope.textContent).toContain("当前2 项差异");
    expect(within(analysisScope).getByRole("button", { name: "恢复全部配置" })).toBeTruthy();
    expect(within(analysisScope).getByRole("button", { name: "显示全部目标列" })).toBeTruthy();
    expect(within(analysisScope).getByRole("button", { name: "清空分析口径" })).toBeTruthy();

    fireEvent.click(within(analysisScope).getByRole("button", { name: "恢复全部配置" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 3 项配置")).toBeTruthy();
    });
    analysisScope = screen.getByLabelText("当前配置分析口径");
    expect(analysisScope.textContent).toContain("目标聚焦Premium");
    expect(analysisScope.textContent).toContain("范围全部配置");
    expect(analysisScope.textContent).toContain("当前3 项配置");
    expect(within(analysisScope).queryByRole("button", { name: "恢复全部配置" })).toBeNull();
    expect(within(analysisScope).getByRole("button", { name: "显示全部目标列" })).toBeTruthy();
    expect(within(analysisScope).queryByRole("button", { name: "清空分析口径" })).toBeNull();

    fireEvent.click(within(analysisScope).getByRole("button", { name: "显示全部目标列" }));

    await waitFor(() => {
      expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();
    });
    expect(screen.getByText("当前表格 3 项配置")).toBeTruthy();
    expect(screen.getAllByText("Speaker").length).toBeGreaterThan(0);
  });

  it("clears a category that has no rows for the newly focused target trim", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");

    fireEvent.click(screen.getByRole("button", { name: /Wheel，当前范围 1 项配置/ }));

    await waitFor(() => {
      expect(screen.getByText("当前大类 Wheel · 1 项配置")).toBeTruthy();
    });

    fireEvent.click(screen.getByRole("button", { name: "从业务摘要聚焦 Luxury 差异" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Luxury · 1 项差异")).toBeTruthy();
    });
    expect(screen.queryByText(/当前大类 Wheel/)).toBeNull();
    expect(screen.getByText("差异项 · 目标 Luxury 摘要")).toBeTruthy();
    expect(screen.getAllByText("Speaker").length).toBeGreaterThan(0);
    expect(screen.queryByText("Wheel size")).toBeNull();
  });

  it("shows own product and external trim identity anchors on selected cards", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    openSimpleSelectedStrip();
    expect(screen.getAllByText("本品").length).toBeGreaterThan(0);
    expect(screen.getAllByText("物料号").length).toBeGreaterThan(0);
    expect(screen.getAllByText("竞品 / 外部").length).toBeGreaterThan(0);
    expect(screen.getAllByText("无物料号").length).toBeGreaterThan(0);
    expect(screen.getByText("身份锚点 物料号 T71607V**MM0001")).toBeTruthy();
  });

  it("focuses a target trim directly from the selected trim card", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    clickSelectedObjectAction("查看 Premium 差异行");

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });
    expect(tableRangeMetricText("当前展示行")).toContain("2差异行");
    expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();
    expect(screen.queryByText("差异行 · 目标 Premium Excel 对比导读")).toBeNull();
    expect(within(openSelectedObjectsPanel()).getByRole("button", { name: "显示全部目标列，取消 Premium 目标列聚焦" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.queryByText("Speaker")).toBeNull();

    fireEvent.click(within(openSelectedObjectsPanel()).getByRole("button", { name: "显示全部目标列，取消 Premium 目标列聚焦" }));

    await waitFor(() => {
      expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    });
    expect(within(openSelectedObjectsPanel()).getByRole("button", { name: "查看 Premium 差异行" }).getAttribute("aria-pressed")).toBe("false");
  });

  it("keeps the focused target when the display drawer switches back to all config", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    await focusPremiumDifferenceFromQuickbar();

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: /显示/ }));

    expect(screen.getByRole("button", { name: "显示范围：全部配置行 3" })).toBeTruthy();
    expect(screen.getByText("完整保留 xlsx 配置行，适合先通读原表。")).toBeTruthy();
    expect(screen.getByText("只看和基准配置列不一致的配置行。")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "显示范围：全部配置行 3" }));

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 3/3 配置行"]);
    });
    expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();
    const drawerScope = screen.getByLabelText("显示控制中的当前分析口径");
    expect(drawerScope.textContent).toContain("基准列Basic");
    expect(drawerScope.textContent).toContain("目标配置列Premium");
    expect(screen.getByRole("button", { name: "显示范围：全部配置行 3" }).getAttribute("aria-pressed")).toBe("true");
  });

  it("summarizes and resets the current analysis scope inside the display drawer", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    await focusPremiumDifferenceFromQuickbar();

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: /显示/ }));

    const drawerScope = screen.getByLabelText("显示控制中的当前分析口径");
    expect(drawerScope.textContent).toContain("目标配置列Premium");
    expect(drawerScope.textContent).toContain("范围差异行");
    expect(drawerScope.textContent).toContain("当前2 差异行");
    expect(drawerScope.textContent).toContain("显示控制、配置大类和配置表正按同一口径联动。");
    expect(within(drawerScope).getByRole("button", { name: "恢复全部配置行" })).toBeTruthy();
    expect(within(drawerScope).getByRole("button", { name: "显示全部目标列" })).toBeTruthy();
    expect(within(drawerScope).getByRole("button", { name: "清空显示口径" })).toBeTruthy();

    fireEvent.click(within(drawerScope).getByRole("button", { name: "恢复全部配置行" }));

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 3/3 配置行"]);
    });
    const targetOnlyDrawerScope = screen.getByLabelText("显示控制中的当前分析口径");
    expect(targetOnlyDrawerScope.textContent).toContain("目标配置列Premium");
    expect(targetOnlyDrawerScope.textContent).toContain("范围全部配置");
    expect(targetOnlyDrawerScope.textContent).toContain("当前3 配置行");
    expect(within(targetOnlyDrawerScope).queryByRole("button", { name: "恢复全部配置行" })).toBeNull();
    expect(within(targetOnlyDrawerScope).getByRole("button", { name: "显示全部目标列" })).toBeTruthy();
    expect(within(targetOnlyDrawerScope).queryByRole("button", { name: "清空显示口径" })).toBeNull();

    fireEvent.click(within(targetOnlyDrawerScope).getByRole("button", { name: "显示全部目标列" }));

    await waitFor(() => {
      expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    });
    const resetDrawerScope = screen.getByLabelText("显示控制中的当前分析口径");
    expect(resetDrawerScope.textContent).toContain("范围全部配置");
    expect(resetDrawerScope.textContent).toContain("当前3 配置行");
    expect(resetDrawerScope.textContent).toContain("当前展示完整 xlsx 配置行；AI 摘要优先，高级诊断已收起，可先通读原表再切换差异行。");
    expect(within(resetDrawerScope).queryByRole("button", { name: "恢复全部配置行" })).toBeNull();
    expect(within(resetDrawerScope).queryByRole("button", { name: "显示全部目标列" })).toBeNull();
    expect(within(resetDrawerScope).queryByRole("button", { name: "清空显示口径" })).toBeNull();
    expect(screen.queryByText(/当前目标配置列 Premium/)).toBeNull();
  });

  it("lets the display drawer focus a target trim with the shared dropdown", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildThreeTrimDigest());

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: /显示/ }));

    const targetTrimInput = screen.getByRole("combobox", { name: "目标配置列" }) as HTMLInputElement;
    expect(targetTrimInput.value).toBe("全部目标列");
    expect(screen.queryByRole("button", { name: "清空 目标配置列" })).toBeNull();
    const defaultTargetAnchor = screen.getByLabelText("目标配置列身份锚点");
    expect(defaultTargetAnchor.textContent).toContain("目标范围全部 2 个目标配置列");
    expect(defaultTargetAnchor.textContent).toContain("物料锚点无物料号 2，需用来源 / sales version");
    expect(defaultTargetAnchor.textContent).toContain("来源同来源 compare-sample.xlsx");

    fireEvent.focus(targetTrimInput);
    const listbox = screen.getByRole("listbox");
    expect(within(listbox).getByText("全部目标列")).toBeTruthy();
    expect(within(listbox).getByText("Luxury")).toBeTruthy();
    expect(within(listbox).queryByText("Basic")).toBeNull();

    fireEvent.click(within(listbox).getByText("Luxury"));

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Luxury", "当前展示 3/3 配置行"]);
    });
    expect(screen.getByText((_content, element) => element?.textContent === "当前范围差异行1")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "当前范围待确认行0")).toBeTruthy();
    expect(screen.queryByText("空值 / 缺失需核对；推断 / 合并格见更多证据筛选")).toBeNull();
    expect((screen.getByRole("combobox", { name: "目标配置列" }) as HTMLInputElement).value).toBe("Luxury");
    expect(screen.getByRole("button", { name: "清空 目标配置列" })).toBeTruthy();
    const focusedTargetAnchor = screen.getByLabelText("目标配置列身份锚点");
    expect(focusedTargetAnchor.textContent).toContain("身份本品 → 竞品 / 外部");
    expect(focusedTargetAnchor.textContent).toContain("目标锚点Sales version Luxury · 无物料号");
    expect(focusedTargetAnchor.textContent).toContain("市场市场待补");
    expect(focusedTargetAnchor.textContent).toContain("来源同来源 compare-sample.xlsx");
    expect(screen.queryByText("目标 Luxury Excel 对比导读")).toBeNull();
    expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();
    expect(screen.getByLabelText("显示控制中的当前分析口径").textContent).toContain("目标配置列Luxury");
    expect(screen.getByRole("button", { name: "顶部显示全部目标列" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
    expect(screen.getAllByText("Speaker").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Wheel size").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "清空 目标配置列" }));

    await waitFor(() => {
      expect((screen.getByRole("combobox", { name: "目标配置列" }) as HTMLInputElement).value).toBe("全部目标列");
    });
    expect(screen.queryByText(/当前目标配置列 Luxury/)).toBeNull();
    expect(screen.queryByText("Excel 配置对比导读")).toBeNull();
  });

  it("uses the shared search dropdown for BOM and keyword filtering", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    openSimpleAdvancedSearch();

    const keywordInput = screen.getByRole("combobox", { name: "BOM / Keyword" });
    expect(screen.queryByPlaceholderText("BOM、配置名、sales version")).toBeNull();

    fireEvent.focus(keywordInput);

    const listbox = screen.getByRole("listbox");
    expect(within(listbox).getByText("T71607V**MM0001")).toBeTruthy();
    expect(within(listbox).getByText("Blind spot")).toBeTruthy();

    fireEvent.click(within(listbox).getByText("T71607V**MM0001"));

    await waitFor(() => {
      expect(vi.mocked(api.listEngineeringConfigTrims).mock.calls.some(([params]) => (
        params?.q === "T71607V**MM0001"
      ))).toBe(true);
    });
  });

  it("lets the floating deck search source digest models before they are formal trims", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildManyGroupDigest(12));

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    openSimpleAdvancedSearch();

    const modelYearInput = screen.getByRole("combobox", { name: "Model Year" });
    fireEvent.focus(modelYearInput);
    const modelYearListbox = screen.getByRole("listbox");
    expect(within(modelYearListbox).getByText("2026")).toBeTruthy();
    fireEvent.click(within(modelYearListbox).getByText("2026"));

    await waitFor(() => {
      expect((screen.getByRole("combobox", { name: "Model Year" }) as HTMLInputElement).value).toBe("2026");
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    let digestCandidates = within(drawer).getByText("Source Digest 可比组").closest(".market-scan-field") as HTMLElement;
    expect(within(digestCandidates).getByRole("button", { name: /选择 Source Digest 可比组：Sample Group 12/ })).toBeTruthy();
    expect(within(digestCandidates).queryByRole("button", { name: /选择 Source Digest 可比组：Sample Group 11/ })).toBeNull();

    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    const sourceDigestSearchInput = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    fireEvent.focus(sourceDigestSearchInput);
    const sourceDigestListbox = screen.getByRole("listbox");
    expect(sourceDigestListbox.textContent).toContain("来源 / 车型路径");
    expect(sourceDigestListbox.textContent).toContain("Sample Group 12");
    expect(sourceDigestListbox.textContent).toContain("整组");
    expect(sourceDigestListbox.textContent).toContain("Germany");
    expect(sourceDigestListbox.textContent).toContain("MY 2026");
    expect(sourceDigestListbox.textContent).toContain("compare-sample.xlsx");
    expect(sourceDigestListbox.textContent).toContain("Sheet 12");

    fireEvent.click(within(sourceDigestListbox).getAllByText("Sample Group 12")[0]);
    await waitFor(() => {
      expect(sourceDigestSearchInput.value).toBe("Sample Group 12");
    });
    const sourceDigestDirectPicker = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_PICKER_COMBOBOX_NAME }) as HTMLInputElement;
    expect(sourceDigestDirectPicker.value).toContain("预览配置列");
    expect(sourceDigestDirectPicker.value).toContain("Sample Group 12");
    expect(within(drawer).getByLabelText(SOURCE_DIGEST_SCOPE_LABEL).textContent).toContain("Sample Group 12");
    const groupConfirm = within(drawer).getByLabelText("当前来源整组确认");
    expect(groupConfirm.textContent).toContain("Sample Group 12");
    fireEvent.click(within(groupConfirm).getByRole("button", { name: "预览当前整组配置列" }));

    await waitFor(() => {
      expect(screen.getByText(/Sample Group 12.*已加载本地预览/)).toBeTruthy();
    });
    expect(container.querySelector(".product-config-local-digest")).toBeNull();
    expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(api.compareEngineeringConfigTrims).not.toHaveBeenCalledWith(["basic-12", "premium-12"], false);
  });

  it("keeps source digest focus inside the config column panel in simple mode", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const configColumnTab = screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME });
    const sourcePanelTab = screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME });
    expect(configColumnTab.getAttribute("aria-selected")).toBe("true");
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" }) as HTMLInputElement;
    fireEvent.focus(directPicker);

    const sourceFocusOption = within(screen.getByRole("listbox")).getByRole("option", {
      name: /聚焦来源 · compare-sample\.xlsx/,
    });
    expect(sourceFocusOption.textContent).toContain("T19C MY ICE");
    fireEvent.click(sourceFocusOption);

    expect(configColumnTab.getAttribute("aria-selected")).toBe("true");
    expect(sourcePanelTab.getAttribute("aria-selected")).toBe("false");
    expect(within(drawer).queryByLabelText("来源组详情浏览")).toBeNull();
    expect(within(drawer).getByLabelText("当前已选配置列").textContent).toContain("来源预览");

    fireEvent.focus(directPicker);
    const focusedListbox = screen.getByRole("listbox");
    expect(focusedListbox.textContent).toContain("预览配置列 · T19C MY ICE");
    expect(focusedListbox.textContent).toContain("compare-sample.xlsx");
    expect(focusedListbox.textContent).toContain("Basic / Premium");
  });

  it("lets users browse the config library by brand and model before adding trims", async () => {
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: 12,
      items: libraryTrimFixtures as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });

    const libraryBrowser = within(drawer).getByText("库内浏览").closest(".market-scan-field") as HTMLElement;
    expect(within(libraryBrowser).getByText("当前筛选命中 12 个配置列，展示 3 个。结果较多时可继续选择车型 / 来源 / 物料号缩小范围。")).toBeTruthy();
    const groupedBrowser = within(libraryBrowser).getByLabelText("配置列库按品牌浏览");
    expect(within(groupedBrowser).getByText("Volvo")).toBeTruthy();
    expect(within(groupedBrowser).getByText("Smart")).toBeTruthy();
    expect(within(groupedBrowser).getAllByText("1 车型")).toHaveLength(2);
    expect(within(groupedBrowser).getByText("2 配置列")).toBeTruthy();
    expect(within(groupedBrowser).getByText("1 配置列")).toBeTruthy();
    expect(within(libraryBrowser).getByText("Volvo EX30")).toBeTruthy();
    expect(within(libraryBrowser).getByText("Smart #1")).toBeTruthy();

    const volvoSummary = within(libraryBrowser).getByText("Volvo EX30").closest("summary") as HTMLElement;
    fireEvent.click(volvoSummary);

    const coreButton = within(libraryBrowser).getByRole("button", { name: /MAT-EX30-CORE.*Core/ });
    const ultraButton = within(libraryBrowser).getByRole("button", { name: /UltraUltra/ });
    expect(coreButton.textContent).toContain("加入");
    expect(ultraButton.textContent).toContain("加入");

    fireEvent.click(coreButton);
    expect(coreButton.textContent).toContain("移除");
    fireEvent.click(ultraButton);

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["library-core", "library-ultra"], false);
    });
    expect(ultraButton.textContent).toContain("移除");
  });

  it("adds multiple config columns from the floating deck search dropdown", async () => {
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: libraryTrimFixtures.length,
      items: libraryTrimFixtures as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const initialUnifiedSearchStatus = within(drawer).getByLabelText("统一搜索覆盖状态");
    expect(initialUnifiedSearchStatus.textContent).toContain("同名范围");
    expect(initialUnifiedSearchStatus.textContent).toContain("暂无当前搜索冲突");
    expect(within(drawer).queryByLabelText("同名车型多来源提示")).toBeNull();
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "EX30" } });
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ q: "EX30", limit: 80 }));
    });
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("Volvo · EX30 · Core");
    });
    let listbox = screen.getByRole("listbox");
    fireEvent.click(within(listbox).getByText("Volvo · EX30 · Core"));

    await waitFor(() => {
      expect(within(drawer).getByLabelText("当前已选配置列").textContent).toContain("Core");
    });

    expect((directPicker as HTMLInputElement).value).toBe("EX30");
    listbox = screen.getByRole("listbox");
    const selectedCoreOption = within(listbox).getByText("Volvo · EX30 · Core").closest("button");
    expect(selectedCoreOption?.className).toContain("is-selected");
    fireEvent.click(within(listbox).getByText("Volvo · EX30 · Ultra"));

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["library-core", "library-ultra"], false);
    });
    const selectedColumns = within(drawer).getByLabelText("当前已选配置列");
    expect(selectedColumns.textContent).toContain("Volvo · EX30 · Germany · MY 2026 · BEV + RWD · 物料号 MAT-EX30-CORE · own-ex30.xlsx");
    expect(selectedColumns.textContent).toContain("Volvo · EX30 · Germany · MY 2026 · BEV + AWD · Sales version Ultra · rival-ex30.html");
    expect(selectedColumns.textContent).toContain("Ultra");
    const selectedPaths = within(selectedColumns).getByLabelText("已选配置列来源路径");
    expect(selectedPaths.textContent).toContain("对比路径分组");
    expect(selectedPaths.textContent).toContain("own-ex30.xlsx");
    expect(selectedPaths.textContent).toContain("rival-ex30.html");
    expect(selectedPaths.textContent).toContain("Volvo");
    expect(selectedPaths.textContent).toContain("Germany");
    expect(selectedPaths.textContent).toContain("MY 2026");
    expect(selectedPaths.textContent).toContain("EX30");
    expect(selectedPaths.textContent).toContain("本品 · 物料号 1");
    expect(selectedPaths.textContent).toContain("竞品 / 外部 · 无物料号 1，Sales version 1");
  });

  it("adds a complete 2-4 column model from the floating deck search dropdown", async () => {
    const singleSourceModelFixtures = libraryTrimFixtures.slice(0, 2).map((trim) => ({
      ...trim,
      sourceUploadId: "source-own",
      sourceFileName: "own-ex30.xlsx",
      sourceFilePath: "/tmp/own-ex30.xlsx",
    }));
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: singleSourceModelFixtures.length,
      items: singleSourceModelFixtures as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "EX30" } });
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ q: "EX30", limit: 80 }));
    });
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("加入车型配置列 · Volvo EX30");
    });

    const listbox = screen.getByRole("listbox");
    expect(listbox.textContent).toContain("加入车型配置列 · Volvo EX30");
    expect(listbox.textContent).toContain("2 个配置列，选择后直接加入对比");
    expect(listbox.textContent).toContain("单一来源 own-ex30.xlsx");
    fireEvent.click(within(listbox).getByRole("option", { name: /加入车型配置列 · Volvo EX30/ }));

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["library-core", "library-ultra"], false);
    });
    const selectedColumns = within(drawer).getByLabelText("当前已选配置列");
    expect(selectedColumns.textContent).toContain("已选配置列 2/4");
    expect(selectedColumns.textContent).toContain("Core");
    expect(selectedColumns.textContent).toContain("Ultra");
    expect((directPicker as HTMLInputElement).value).toBe("");
  });

  it("keeps cross-source formal models in focus mode instead of one-click adding them", async () => {
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: libraryTrimFixtures.length,
      items: libraryTrimFixtures as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "EX30" } });
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ q: "EX30", limit: 80 }));
    });
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("聚焦车型 · Volvo EX30");
    });

    const listbox = screen.getByRole("listbox");
    expect(listbox.textContent).not.toContain("加入车型配置列 · Volvo EX30");
    const focusModelOption = within(listbox).getByRole("option", { name: /聚焦车型 · Volvo EX30/ });
    expect(focusModelOption.textContent).toContain("2 来源，先聚焦核对");
    const unifiedSearchStatus = within(drawer).getByLabelText("统一搜索覆盖状态");
    expect(unifiedSearchStatus.textContent).toContain("同名范围");
    expect(unifiedSearchStatus.textContent).toContain("1 组");
    const ambiguityPanel = within(drawer).getByLabelText("同名车型多来源提示");
    expect(ambiguityPanel.textContent).toContain("EX30 · Germany · MY 2026：2 配置列 / 2 来源");
    expect(ambiguityPanel.textContent).toContain("正式库跨来源车型不会一键加入");
    expect(listbox.textContent).toContain("Volvo · EX30 · Core");
    expect(listbox.textContent).toContain("Volvo · EX30 · Ultra");
    expect(api.compareEngineeringConfigTrims).not.toHaveBeenCalledWith(["library-core", "library-ultra"], false);

    fireEvent.keyDown(directPicker, { key: "Escape" });
    expect((directPicker as HTMLInputElement).value).toBe("");
    expect(within(drawer).queryByLabelText("同名车型多来源提示")).toBeNull();
    const escapeClearedUnifiedSearchStatus = within(drawer).getByLabelText("统一搜索覆盖状态");
    expect(escapeClearedUnifiedSearchStatus.textContent).toContain("同名范围");
    expect(escapeClearedUnifiedSearchStatus.textContent).toContain("暂无当前搜索冲突");

    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "EX30" } });
    expect(within(drawer).getByLabelText("同名车型多来源提示").textContent).toContain("EX30 · Germany · MY 2026");

    fireEvent.click(within(drawer).getByRole("button", { name: "清除直接配置列搜索" }));
    const clearedDirectPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" }) as HTMLInputElement;
    expect(clearedDirectPicker.value).toBe("");
    expect(within(drawer).queryByLabelText("同名车型多来源提示")).toBeNull();
    const clearedUnifiedSearchStatus = within(drawer).getByLabelText("统一搜索覆盖状态");
    expect(clearedUnifiedSearchStatus.textContent).toContain("同名范围");
    expect(clearedUnifiedSearchStatus.textContent).toContain("暂无当前搜索冲突");
  });

  it("disables unselected direct config options after reaching four selected columns", async () => {
    const limitTrimFixtures: VehicleTrimItem[] = Array.from({ length: 5 }, (_, index) => ({
      ...libraryTrimFixtures[0],
      trimId: `limit-${index + 1}`,
      brand: "Limit Brand",
      modelName: "Limit Model",
      trimName: `Trim ${index + 1}`,
      fullTrimName: `Limit Model Trim ${index + 1}`,
      materialNo: `LIMIT-${index + 1}`,
      identityKey: `LIMIT-${index + 1}`,
      salesVersion: `Trim ${index + 1}`,
      sourceFileName: "limit-config.xlsx",
      sourceUploadId: "source-limit",
      sourceFilePath: "/tmp/limit-config.xlsx",
    }));
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: limitTrimFixtures.length,
      items: limitTrimFixtures as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" }) as HTMLInputElement;
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "Limit" } });
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ q: "Limit", limit: 80 }));
    });
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("Limit Brand · Limit Model · Trim 1");
    });

    for (let index = 1; index <= 4; index += 1) {
      const listbox = screen.getByRole("listbox");
      fireEvent.click(within(listbox).getByText(`Limit Brand · Limit Model · Trim ${index}`));
    }

    const selectedColumns = within(drawer).getByLabelText("当前已选配置列");
    await waitFor(() => {
      expect(selectedColumns.textContent).toContain("已选配置列 4/4");
    });
    expect(directPicker.value).toBe("Limit");

    const fullListbox = screen.getByRole("listbox");
    const fifthOption = within(fullListbox)
      .getByText("Limit Brand · Limit Model · Trim 5")
      .closest("button") as HTMLButtonElement | null;
    expect(fifthOption).toBeTruthy();
    expect(fifthOption?.disabled).toBe(true);
    expect(fifthOption?.className).toContain("is-disabled");
    expect(fifthOption?.textContent).toContain("已满");

    fireEvent.click(fifthOption as HTMLButtonElement);
    expect(selectedColumns.textContent).not.toContain("Trim 5");
  });

  it("lets users focus formal config library options by source, brand, and model from the direct dropdown", async () => {
    const brandInModelTrim: VehicleTrimItem = {
      ...libraryTrimFixtures[0],
      trimId: "library-bmw-x7",
      brand: "BMW",
      modelName: "BMW X7",
      trimName: "xDrive40i",
      fullTrimName: "BMW X7 xDrive40i",
      materialNo: null,
      salesVersion: "xDrive40i",
      sourceFileName: "bmw-x7.xlsx",
    };
    const libraryRows = [...libraryTrimFixtures, brandInModelTrim];
    vi.mocked(api.listEngineeringConfigTrims)
      .mockResolvedValueOnce({
        rows: libraryRows.length,
        items: libraryRows as unknown as Record<string, unknown>[],
      })
      .mockResolvedValue({
        rows: 2,
        items: libraryTrimFixtures.slice(0, 2) as unknown as Record<string, unknown>[],
      });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });

    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);

    const listbox = screen.getByRole("listbox");
    expect(listbox.textContent).toContain("聚焦来源 · own-ex30.xlsx");
    expect(listbox.textContent).toContain("聚焦品牌 · Volvo");
    expect(listbox.textContent).toContain("1 车型");
    expect(listbox.textContent).toContain("聚焦车型 · Volvo EX30");
    expect(listbox.textContent).toContain("聚焦车型 · BMW X7");
    expect(listbox.textContent).not.toContain("聚焦车型 · BMW BMW X7");
    expect(listbox.textContent).toContain("2 已建配置列");
    expect(listbox.textContent).toContain("正式库已建范围");
    expect(listbox.textContent).toContain("已建，可直接加入对比");

    const directOptions = within(listbox).getAllByRole("option");
    const sourceOptionIndex = directOptions.findIndex((option) => option.textContent?.includes("聚焦来源 · own-ex30.xlsx"));
    const brandOptionIndex = directOptions.findIndex((option) => option.textContent?.includes("聚焦品牌 · Volvo"));
    const modelOptionIndex = directOptions.findIndex((option) => option.textContent?.includes("聚焦车型 · Volvo EX30"));
    const concreteOptionIndex = directOptions.findIndex((option) => option.textContent?.includes("Volvo · EX30 · Core"));
    expect(sourceOptionIndex).toBeGreaterThanOrEqual(0);
    expect(brandOptionIndex).toBeGreaterThanOrEqual(0);
    expect(modelOptionIndex).toBeGreaterThanOrEqual(0);
    expect(concreteOptionIndex).toBeGreaterThanOrEqual(0);
    expect(sourceOptionIndex).toBeLessThan(brandOptionIndex);
    expect(brandOptionIndex).toBeLessThan(modelOptionIndex);
    expect(modelOptionIndex).toBeLessThan(concreteOptionIndex);

    fireEvent.click(within(listbox).getByRole("option", { name: /聚焦来源 · own-ex30\.xlsx/ }));

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        q: "own-ex30.xlsx",
      }));
    });
    expect((directPicker as HTMLInputElement).value).toBe("聚焦来源 · own-ex30.xlsx");
    expect(within(drawer).getByLabelText("当前已选配置列").textContent).not.toContain("Core");

    fireEvent.focus(directPicker);
    const focusedListbox = screen.getByRole("listbox");
    fireEvent.click(within(focusedListbox).getByRole("option", { name: /聚焦车型 · Volvo EX30/ }));

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        q: "Volvo EX30",
      }));
    });
    expect((directPicker as HTMLInputElement).value).toBe("聚焦车型 · Volvo EX30");
    expect(within(drawer).getByLabelText("当前已选配置列").textContent).not.toContain("Core");
  });

  it("marks formal library options selected when they match the current digest preview identity", async () => {
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: 1,
      items: [{
        ...libraryTrimFixtures[0],
        trimId: "formal-t19c-basic",
        brand: "OMODA",
        modelName: "T19C MY ICE",
        trimName: "Basic",
        fullTrimName: "Basic",
        market: null,
        country: null,
        modelYear: null,
        materialNo: "T71607V**MM0001",
        sourceFileName: "published-t19c.xlsx",
      }] as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    openSimpleSelectedStrip();
    await screen.findByText("身份锚点 物料号 T71607V**MM0001");
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "T19C" } });
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ q: "T19C", limit: 80 }));
    });

    const listbox = await screen.findByRole("listbox");
    const selectedOption = within(listbox).getByText("OMODA · T19C MY ICE · Basic").closest("button");
    expect(selectedOption?.className).toContain("is-selected");
    expect(selectedOption?.textContent).toContain("已选");
  });

  it("creates editable source digest columns directly from the floating deck search dropdown", async () => {
    const directLibraryTrim: VehicleTrimItem = {
      ...libraryTrimFixtures[0],
      trimId: "direct-library-basic",
      brand: "OMODA",
      modelName: "Direct Source Model",
      trimName: "Formal Basic",
      fullTrimName: "Direct Source Model Formal Basic",
      sourceFileName: "direct-source.html",
      sourceCreatedBy: "tester",
    };
    const directDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "tabular",
      sourceFormat: "tabular",
      fileName: "direct-source.html",
      compareGroups: [
        {
          ...digest.compareGroups[0],
          groupId: "direct-source-model",
          title: "Direct Source Model",
          modelName: "Direct Source Model",
          sourceSheet: "Direct Source Sheet",
        },
      ],
    };
    const directSourceSummary = {
      ...buildSourceSnapshotFixture("direct-source", "direct-source.html", null),
      fileType: "html",
      mimeType: "text/html",
      sourceSearchMatches: ["文件 direct-source.html", "Model Direct Source Model"],
      sourceDigestStatus: {
        digestType: "tabular",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 1,
      items: [directSourceSummary],
    });
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValue({
      rows: 1,
      items: [directLibraryTrim] as unknown as Record<string, unknown>[],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(
      {
        ...buildSourceSnapshotFixture("direct-source", "direct-source.html", directDigest),
        fileType: "html",
        mimeType: "text/html",
      },
    );
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "direct-source",
      groupId: "direct-source-model",
      importBatchId: "draft-direct-source",
      trimIds: ["draft-direct-basic", "draft-direct-premium"],
      compareTrimIds: ["draft-direct-basic", "draft-direct-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-direct-basic", "version-direct-premium"],
    });
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare({
      trims: [
        directLibraryTrim,
        { ...directLibraryTrim, trimId: "draft-direct-basic", trimName: "Basic", fullTrimName: "Direct Source Model Basic" },
        { ...directLibraryTrim, trimId: "draft-direct-premium", trimName: "Premium", fullTrimName: "Direct Source Model Premium" },
      ],
      rows: [],
      groups: [],
      totalFeatures: 0,
      shownFeatures: 0,
    }) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=direct-library-basic&baseTrimId=direct-library-basic"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "Direct Source Model" } });

    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Direct Source Model",
      }));
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("direct-source");
      expect(screen.getByRole("listbox").textContent).toContain("Direct Source Model");
      const groupLabels = Array.from(document.querySelectorAll(".comparison-filter-dropdown-group"))
        .map((element) => element.textContent);
      expect(groupLabels).toEqual(expect.arrayContaining([
        "配置列库 · 已发布 / 草稿",
        "来源库 Source Digest · EU · direct-source.html",
      ]));
      expect(screen.getByRole("listbox").textContent).toContain("来源库 Source Digest · EU · direct-source.html");
      expect(screen.getByRole("listbox").textContent).not.toContain("生成在线可编辑配置列");
      expect(screen.getByRole("listbox").textContent).toContain("生成配置列 · Direct Source Model · Basic / Premium");
      expect(screen.getByRole("listbox").textContent).toContain("已选，回车可移除");
      expect(screen.getByRole("listbox").textContent).toContain("可直接生成在线表，生成后加入对比");
      expect(screen.getByRole("listbox").textContent).toContain("生成");
      expect(screen.getByRole("listbox").textContent).toContain("来源库 · EU · direct-source.html · Direct Source Sheet · 上传人 tester");
      expect(screen.getByRole("listbox").textContent).toContain("表格文本");
    });
    expect(within(drawer).getByLabelText("统一搜索覆盖状态").textContent).toContain("可生成在线表选项");

    await waitFor(() => {
      expect(within(drawer).getByLabelText("当前已选配置列").textContent).toContain("Formal Basic");
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", {
      name: /生成配置列 · Direct Source Model · Basic \/ Premium.*direct-source\.html.*Direct Source Sheet.*上传人 tester/,
    }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("direct-source", "direct-source-model");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["direct-library-basic", "draft-direct-basic", "draft-direct-premium"], false, "latest");
    });
    expect(await within(drawer).findByText(/Direct Source Model 已按 2 个配置列创建为可编辑配置列：2 配置列（新建 2，复用 0） · 3 配置项（新建 3，复用 0） · 写入 6 条值（新增 6，更新 0）。 已追加到当前对比。/)).toBeTruthy();
    expect(within(drawer).getByText("新配置列已加入当前对比表")).toBeTruthy();
    expect(within(drawer).getByLabelText("建列结果摘要").textContent).toContain("加入当前2/2");
    const editControl = within(drawer).getByLabelText("在线编辑控制");
    expect(within(editControl).getByText("编辑未开启")).toBeTruthy();
    expect(within(editControl).getByRole("button", { name: "开启在线编辑" })).toBeTruthy();
  });

  it("keeps direct source digest creation additive and reports rows hidden by the four-column limit", async () => {
    const capacityDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "capacity-source.xlsx",
      compareGroups: [
        {
          ...digest.compareGroups[0],
          groupId: "capacity-source-model",
          title: "Capacity Source Model",
          modelName: "Capacity Source Model",
          sourceSheet: "Capacity Sheet",
        },
      ],
    };
    const capacitySummary = {
      ...buildSourceSnapshotFixture("capacity-source", "capacity-source.xlsx", null),
      sourceSearchMatches: ["文件 capacity-source.xlsx", "Model Capacity Source Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValue({
      rows: libraryTrimFixtures.length,
      items: libraryTrimFixtures as unknown as Record<string, unknown>[],
    });
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 1,
      items: [capacitySummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(
      buildSourceSnapshotFixture("capacity-source", "capacity-source.xlsx", capacityDigest),
    );
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "capacity-source",
      groupId: "capacity-source-model",
      importBatchId: "draft-capacity-source",
      trimIds: ["draft-capacity-basic", "draft-capacity-premium"],
      compareTrimIds: ["draft-capacity-basic", "draft-capacity-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-capacity-basic", "version-capacity-premium"],
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=library-core,library-ultra,library-rival&baseTrimId=library-core"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "Capacity Source Model" } });

    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("capacity-source");
      expect(screen.getByRole("listbox").textContent).toContain("生成配置列 · Capacity Source Model · Basic / Premium");
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", {
      name: /生成配置列 · Capacity Source Model · Basic \/ Premium.*capacity-source\.xlsx.*Capacity Sheet/,
    }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("capacity-source", "capacity-source-model");
      expect(vi.mocked(api.compareEngineeringConfigTrims).mock.calls.some(([ids, onlyDifferences]) => (
        Array.isArray(ids)
        && ids.join("|") === "library-core|library-ultra|library-rival|draft-capacity-basic"
        && onlyDifferences === false
      ))).toBe(true);
    });
    expect(await within(drawer).findByText(/已加入当前对比 1\/2 列；1 列已建入库但因最多 4 列暂未显示。/)).toBeTruthy();
    expect(within(drawer).getByText("新配置列已部分加入当前对比表")).toBeTruthy();
    const digestDraftSuccess = within(drawer).getByLabelText("来源建列成功");
    expect(within(digestDraftSuccess).getByLabelText("建列结果摘要").textContent).toContain("加入当前1/2");
    expect(within(digestDraftSuccess).getByLabelText("建列结果摘要").textContent).toContain("暂未显示1");
    const replacementControls = within(digestDraftSuccess).getByLabelText("暂未显示配置列替换入口");
    expect(replacementControls.textContent).toContain("Premium");
    fireEvent.click(within(replacementControls).getByRole("button", { name: "用 Premium 替换 Ultra 进入当前对比" }));

    await waitFor(() => {
      expect(vi.mocked(api.compareEngineeringConfigTrims).mock.calls.some(([ids, onlyDifferences]) => (
        Array.isArray(ids)
        && ids.join("|") === "library-core|draft-capacity-premium|library-rival|draft-capacity-basic"
        && onlyDifferences === false
      ))).toBe(true);
    });
    const updatedDraftSuccess = within(drawer).getByLabelText("来源建列成功");
    expect(updatedDraftSuccess.textContent).toContain("Premium 已替换 Ultra 进入当前对比。");
    expect(within(updatedDraftSuccess).getByLabelText("建列结果摘要").textContent).toContain("加入当前2/2");
    expect(within(updatedDraftSuccess).queryByLabelText("暂未显示配置列替换入口")).toBeNull();
  });

  it("disambiguates same-model source digest options by market and model year in the direct dropdown", async () => {
    const sharedGroupBase: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "shared-model",
      title: "Shared Model",
      modelName: "Shared Model",
      sourceSheet: "Shared Sheet",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        modelName: "Shared Model",
      })),
    };
    const germanyDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "shared-config.xlsx",
      compareGroups: [
        {
          ...sharedGroupBase,
          groupId: "shared-model-de",
          sourceSheet: "Germany Sheet",
          trims: sharedGroupBase.trims.map((trim) => ({
            ...trim,
            trimId: `de-${trim.trimId}`,
            market: "Germany",
            country: "Germany",
            profile: { ...trim.profile, modelYear: "2026" },
          })),
        },
      ],
    };
    const franceDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "shared-config.xlsx",
      compareGroups: [
        {
          ...sharedGroupBase,
          groupId: "shared-model-fr",
          sourceSheet: "France Sheet",
          trims: sharedGroupBase.trims.map((trim) => ({
            ...trim,
            trimId: `fr-${trim.trimId}`,
            market: "France",
            country: "France",
            profile: { ...trim.profile, modelYear: "2025" },
          })),
        },
      ],
    };
    const germanyRefreshDigest: EngineeringConfigSourceDigest = {
      ...digest,
      fileName: "shared-config-refresh.xlsx",
      compareGroups: [
        {
          ...sharedGroupBase,
          groupId: "shared-model-de-refresh",
          sourceSheet: "Germany Refresh Sheet",
          trims: sharedGroupBase.trims.map((trim) => ({
            ...trim,
            trimId: `de-refresh-${trim.trimId}`,
            market: "Germany",
            country: "Germany",
            profile: { ...trim.profile, modelYear: "2026" },
          })),
        },
      ],
    };
    const germanySummary = buildSourceSnapshotFixture("source-germany", "shared-config.xlsx", null);
    germanySummary.relatedContext = {
      ...germanySummary.relatedContext,
      brand: "OMODA",
      model: "Shared Model",
      market: "Germany",
      country: "Germany",
      modelYear: "2026",
    };
    germanySummary.sourceSearchMatches = ["Model Shared Model", "Market Germany"];
    germanySummary.sourceDigestStatus = {
      digestType: "workbook",
      status: "ready",
      summary: {
        candidateTrimCount: 2,
        comparableGroupCount: 1,
        featureCount: 3,
        differenceCount: 2,
      },
    };
    const franceSummary = buildSourceSnapshotFixture("source-france", "shared-config.xlsx", null);
    franceSummary.relatedContext = {
      ...franceSummary.relatedContext,
      brand: "OMODA",
      model: "Shared Model",
      market: "France",
      country: "France",
      modelYear: "2025",
    };
    franceSummary.sourceSearchMatches = ["Model Shared Model", "Market France"];
    franceSummary.sourceDigestStatus = germanySummary.sourceDigestStatus;
    const germanyRefreshSummary = buildSourceSnapshotFixture("source-germany-refresh", "shared-config-refresh.xlsx", null);
    germanyRefreshSummary.createdBy = "bob";
    germanyRefreshSummary.relatedContext = {
      ...germanyRefreshSummary.relatedContext,
      brand: "OMODA",
      model: "Shared Model",
      market: "Germany",
      country: "Germany",
      modelYear: "2026",
    };
    germanyRefreshSummary.sourceSearchMatches = ["Model Shared Model", "Market Germany", "Source refresh"];
    germanyRefreshSummary.sourceDigestStatus = germanySummary.sourceDigestStatus;
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 3,
      items: [germanySummary, franceSummary, germanyRefreshSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockImplementation(async (sourceId: string) => {
      const snapshot = sourceId === "source-germany"
        ? buildSourceSnapshotFixture("source-germany", "shared-config.xlsx", germanyDigest)
        : sourceId === "source-germany-refresh"
          ? buildSourceSnapshotFixture("source-germany-refresh", "shared-config-refresh.xlsx", germanyRefreshDigest)
          : buildSourceSnapshotFixture("source-france", "shared-config.xlsx", franceDigest);
      if (sourceId === "source-germany") {
        snapshot.relatedContext = germanySummary.relatedContext;
        snapshot.sourceSearchMatches = germanySummary.sourceSearchMatches;
      } else if (sourceId === "source-germany-refresh") {
        snapshot.createdBy = "bob";
        snapshot.relatedContext = germanyRefreshSummary.relatedContext;
        snapshot.sourceSearchMatches = germanyRefreshSummary.sourceSearchMatches;
      } else {
        snapshot.relatedContext = franceSummary.relatedContext;
        snapshot.sourceSearchMatches = franceSummary.sourceSearchMatches;
      }
      return snapshot;
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-germany",
      groupId: "shared-model-de",
      importBatchId: "draft-shared-de",
      trimIds: ["draft-de-basic", "draft-de-premium"],
      compareTrimIds: ["draft-de-basic", "draft-de-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-de-basic", "version-de-premium"],
    });

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.change(screen.getByRole("combobox", { name: "搜索并添加配置列" }), {
      target: { value: "Shared Model" },
    });

    await waitFor(() => {
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-germany");
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-france");
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-germany-refresh");
    });
    const listbox = screen.getByRole("listbox");
    expect(listbox.textContent).toContain("来源库 Source Digest · Germany · MY 2026 · shared-config.xlsx");
    expect(listbox.textContent).toContain("来源库 Source Digest · Germany · MY 2026 · shared-config-refresh.xlsx");
    expect(listbox.textContent).toContain("来源库 Source Digest · France · MY 2025 · shared-config.xlsx");
    expect(listbox.textContent).toContain("生成配置列 · Shared Model · Basic / Premium");
    expect(listbox.textContent).toContain("来源库 · Germany · MY 2026 · shared-config.xlsx · Germany Sheet · 上传人 tester");
    expect(listbox.textContent).toContain("来源库 · Germany · MY 2026 · shared-config-refresh.xlsx · Germany Refresh Sheet · 上传人 bob");
    expect(listbox.textContent).toContain("来源库 · France · MY 2025 · shared-config.xlsx · France Sheet · 上传人 tester");
    expect(listbox.textContent).not.toContain("上传人 上传人");
    const crossSourceModelOption = within(listbox).getByRole("option", { name: /聚焦同名车型 · Shared Model/ });
    expect(crossSourceModelOption.textContent).toContain("跨来源核对：Shared Model · Germany · MY 2026 同国家同年款存在 2 来源 / 2 sheet / 2 上传人");
    expect(screen.getByLabelText("同名车型多来源提示").textContent).toContain("Shared Model · Germany · MY 2026：2 组 / 2 来源 / 2 表格页 / 2 上传人");
    expect(screen.getByLabelText("同名车型多来源提示").textContent).toContain("同国家同年款但来源、表格页或上传人不同");
    fireEvent.click(screen.getByRole("button", { name: "查看全部同名范围" }));
    expect((screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value).toBe("Shared Model");
    expect((screen.getByLabelText("来源组详情浏览") as HTMLDetailsElement).open).toBe(true);
    expect(screen.getByLabelText("Source Digest 命中路径预览").textContent).toContain("shared-config-refresh.xlsx");

    fireEvent.click(within(screen.getByRole("tablist", { name: "配置列对比控制" })).getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: "搜索并添加配置列" }), {
      target: { value: "Shared Model" },
    });
    fireEvent.click(screen.getByRole("button", { name: "按此范围核对配置来源：Shared Model · Germany · MY 2026" }));
    expect((screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value).toBe("Shared Model Germany 2026");
    expect(screen.getByLabelText("Source Digest 命中路径预览").textContent).toContain("shared-config.xlsx");

    fireEvent.click(within(screen.getByRole("tablist", { name: "配置列对比控制" })).getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: "搜索并添加配置列" }), {
      target: { value: "Shared Model" },
    });

    const refreshedListbox = screen.getByRole("listbox");
    fireEvent.click(within(refreshedListbox).getByRole("option", {
      name: /生成配置列 · Shared Model · Basic \/ Premium.*shared-config\.xlsx.*Germany Sheet.*上传人 tester/,
    }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-germany", "shared-model-de");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-de-basic", "draft-de-premium"], false, "latest");
    });
  });

  it("prioritizes exact source digest matches and limits direct dropdown detail expansion", async () => {
    const makeDigestForModel = (modelName: string, groupId: string): EngineeringConfigSourceDigest => ({
      ...digest,
      fileName: `${groupId}.xlsx`,
      compareGroups: [
        {
          ...digest.compareGroups[0],
          groupId,
          title: modelName,
          modelName,
          sourceSheet: `${modelName} Sheet`,
          trims: digest.compareGroups[0].trims.map((trim) => ({
            ...trim,
            trimId: `${groupId}-${trim.trimId}`,
            modelName,
          })),
        },
      ],
    });
    const exactDigest = makeDigestForModel("Exact Model", "exact-model");
    const otherDigests = Array.from({ length: 5 }, (_, index) => makeDigestForModel(`Other Model ${index + 1}`, `other-model-${index + 1}`));
    const makeSummary = (
      sourceId: string,
      fileName: string,
      modelName: string,
      sourceSearchMatches: string[],
    ): EngineeringConfigSourceSnapshot => ({
      ...buildSourceSnapshotFixture(sourceId, fileName, null),
      relatedContext: {
        brand: "OMODA",
        model: modelName,
        market: "Germany",
        country: "Germany",
        modelYear: "2026",
        trimIds: [],
        salesVersionIds: [],
        contextType: "compare",
      },
      sourceSearchMatches,
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    });
    const summaries = [
      ...otherDigests.map((item, index) => makeSummary(
        `source-other-${index + 1}`,
        `other-${index + 1}.xlsx`,
        item.compareGroups[0].modelName,
        [`Model ${item.compareGroups[0].modelName}`],
      )),
      makeSummary("source-exact", "exact-source.xlsx", "Exact Model", ["Model Exact Model", "Source exact-source.xlsx"]),
    ];
    const detailBySourceId = new Map<string, EngineeringConfigSourceSnapshot>([
      ...otherDigests.map((item, index) => [
        `source-other-${index + 1}`,
        buildSourceSnapshotFixture(`source-other-${index + 1}`, `other-${index + 1}.xlsx`, item),
      ] as const),
      ["source-exact", buildSourceSnapshotFixture("source-exact", "exact-source.xlsx", exactDigest)],
    ]);
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: summaries.length,
      items: summaries,
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockImplementation(async (sourceId: string) => {
      const detail = detailBySourceId.get(sourceId);
      if (!detail) throw new Error(`Missing test detail for ${sourceId}`);
      return detail;
    });

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.change(screen.getByRole("combobox", { name: "搜索并添加配置列" }), {
      target: { value: "Exact Model" },
    });

    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "Exact Model",
      }));
      expect(api.getEngineeringConfigSourceSnapshot).toHaveBeenCalledWith("source-exact");
    });
    const requestedSourceIds = vi.mocked(api.getEngineeringConfigSourceSnapshot).mock.calls.map(([sourceId]) => sourceId);
    expect(requestedSourceIds).toHaveLength(4);
    expect(requestedSourceIds[0]).toBe("source-exact");
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("生成配置列 · Exact Model · Basic / Premium");
    });
    openSimpleAdvancedSearch();
    const sourceHints = await screen.findByLabelText("来源库轻量命中");
    expect(sourceHints.querySelectorAll(".product-config-source-snapshot-hint")).toHaveLength(6);
    expect(sourceHints.querySelector(".product-config-source-snapshot-hint")?.textContent).toContain("exact-source.xlsx");
  });

  it("creates editable source digest columns from directly selected digest trims after confirmation", async () => {
    const directThreeDigest = buildThreeTrimDigest();
    const trimIds = ["direct-three-basic", "direct-three-premium", "direct-three-luxury"];
    const directThreeGroup: EngineeringConfigSourceDigestGroup = {
      ...directThreeDigest.compareGroups[0],
      groupId: "direct-three-model",
      title: "Direct Three Model",
      modelName: "Direct Three Model",
      sourceSheet: "Direct Three Sheet",
      trimCount: 3,
      trims: directThreeDigest.compareGroups[0].trims.map((trim, index) => ({
        ...trim,
        trimId: trimIds[index],
        trimName: ["Basic", "Premium", "Luxury"][index],
        fullTrimName: `Direct Three ${["Basic", "Premium", "Luxury"][index]}`,
        modelName: "Direct Three Model",
      })),
      rows: directThreeDigest.compareGroups[0].rows.map((row) => ({
        ...row,
        values: row.values.map((value, index) => (
          value == null
            ? null
            : {
              ...value,
              valueId: `${trimIds[index]}-${row.featureCode}`,
            }
        )),
      })),
    };
    const sourceDigest: EngineeringConfigSourceDigest = {
      ...directThreeDigest,
      compareGroups: [directThreeGroup],
    };
    const directThreeSummary = {
      ...buildSourceSnapshotFixture("direct-three-source", "direct-three-source.xlsx", null),
      sourceSearchMatches: ["文件 direct-three-source.xlsx", "Model Direct Three Model"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 3,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 3,
        },
      },
    };
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockResolvedValue({
      rows: 1,
      items: [directThreeSummary],
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(
      buildSourceSnapshotFixture("direct-three-source", "direct-three-source.xlsx", sourceDigest),
    );
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "direct-three-source",
      groupId: "direct-three-model",
      importBatchId: "draft-direct-three",
      trimIds: ["draft-direct-three-basic", "draft-direct-three-luxury"],
      compareTrimIds: ["draft-direct-three-basic", "draft-direct-three-luxury"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 3,
      createdFeatureCount: 3,
      reusedFeatureCount: 0,
      valueRecordCount: 6,
      insertedValueCount: 6,
      updatedValueCount: 0,
      createdVersionIds: ["version-direct-three-basic", "version-direct-three-luxury"],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "Direct Three Model" } });

    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("Direct Three Model");
      expect(screen.getByRole("listbox").textContent).not.toContain("选择 2-4 个同组配置列后在已选区域确认生成");
      expect(screen.getByRole("listbox").textContent).toContain("暂选配置列 · Direct Three Model");
      expect(screen.getByRole("listbox").textContent).toContain("暂选");
      expect(screen.getByRole("listbox").textContent).toContain("Excel");
      expect(screen.getByRole("listbox").textContent).toContain("来源库来源 / 品牌 OMODA / 市场 EU / direct-three-source.xlsx / Direct Three Sheet / Direct Three Model / 配置列 Basic");
      expect(screen.getByRole("listbox").textContent).toContain("来源库 · EU · direct-three-source.xlsx · Direct Three Sheet · 上传人 tester");
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", {
      name: /暂选配置列 · Direct Three Model · Basic.*direct-three-source\.xlsx.*Direct Three Sheet.*上传人 tester/,
    }));
    expect(api.createEngineeringConfigDraftFromSourceDigest).not.toHaveBeenCalled();
    const pendingPanel = await within(drawer).findByLabelText("待生成来源配置列");
    expect(pendingPanel.textContent).toContain("Direct Three Model");
    expect(pendingPanel.textContent).toContain("1/4");
    expect(pendingPanel.textContent).toContain("EU · direct-three-source.xlsx · Direct Three Sheet · 上传人 tester");
    const pendingPath = within(pendingPanel).getByLabelText("Direct Three Model 待生成来源路径");
    expect(pendingPath.textContent).toContain("品牌OMODA");
    expect(pendingPath.textContent).toContain("车型Direct Three Model");
    expect(pendingPath.textContent).toContain("市场EU");
    expect(pendingPath.textContent).toContain("来源direct-three-source.xlsx / Direct Three Sheet");
    expect(pendingPanel.textContent).toContain("还需再选择 1 个同来源配置列，才能生成可编辑配置列。");
    expect(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Basic" })).toBeTruthy();

    fireEvent.change(directPicker, { target: { value: "Direct Three Model" } });
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("同组已暂选 1/4");
      expect(screen.getByRole("listbox").textContent).toContain("已暂选");
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", {
      name: /暂选配置列 · Direct Three Model · Premium.*direct-three-source\.xlsx.*Direct Three Sheet.*上传人 tester/,
    }));
    expect(api.createEngineeringConfigDraftFromSourceDigest).not.toHaveBeenCalled();
    await waitFor(() => {
      expect(pendingPanel.textContent).toContain("2/4");
      expect(pendingPanel.textContent).toContain("已满足生成条件，点击生成后会进入正式配置列库并加入当前对比。");
      expect(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Basic" })).toBeTruthy();
      expect(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Premium" })).toBeTruthy();
    });
    const directDiagnostics = within(drawer).getByLabelText("直接搜索配置列诊断") as HTMLDetailsElement;
    expect(within(drawer).queryByLabelText("直接搜索配置列结果拆解")).toBeNull();
    fireEvent.click(within(directDiagnostics).getByText("搜索诊断"));
    expect(directDiagnostics.open).toBe(true);
    const directSummary = within(drawer).getByLabelText("直接搜索配置列结果拆解");
    expect(directSummary.textContent).toContain("待生成2 列 / 1 组确认后写入配置列库");

    fireEvent.change(directPicker, { target: { value: "Direct Three Model" } });
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("同组已暂选 2/4，可生成");
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", {
      name: /暂选配置列 · Direct Three Model · Luxury.*direct-three-source\.xlsx.*Direct Three Sheet.*上传人 tester/,
    }));
    expect(api.createEngineeringConfigDraftFromSourceDigest).not.toHaveBeenCalled();
    await waitFor(() => {
      expect(pendingPanel.textContent).toContain("3/4");
      expect(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Luxury" })).toBeTruthy();
    });

    expect(pendingPanel.textContent).toContain("3/4");
    expect(pendingPanel.textContent).toContain("Excel");
    expect(pendingPanel.textContent).toContain("EU · direct-three-source.xlsx · Direct Three Sheet · 上传人 tester");
    expect(pendingPanel.textContent).toContain("当前暂选按同一个来源 / 车型生成；生成后进入正式配置列库，可在线编辑、导出，并继续和其他国家 / 车型 / 网站来源一起对比。");
    fireEvent.click(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Premium" }));
    await waitFor(() => {
      expect(pendingPanel.textContent).toContain("2/4");
      expect(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Basic" })).toBeTruthy();
      expect(within(pendingPanel).getByRole("button", { name: "移除待处理配置列 Luxury" })).toBeTruthy();
      expect(within(pendingPanel).queryByRole("button", { name: "移除待处理配置列 Premium" })).toBeNull();
    });

    fireEvent.click(within(pendingPanel).getByRole("button", { name: "生成 Direct Three Model · EU · direct-three-source.xlsx · Direct Three Sheet · 上传人 tester 可编辑配置列" }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith(
        "direct-three-source",
        "direct-three-model",
        { trimIds: ["direct-three-basic", "direct-three-luxury"] },
      );
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-direct-three-basic", "draft-direct-three-luxury"], false, "latest");
    });
    expect(await within(drawer).findByText("Direct Three Model 已按 2 个配置列创建为可编辑配置列：2 配置列（新建 2，复用 0） · 3 配置项（新建 3，复用 0） · 写入 6 条值（新增 6，更新 0）。")).toBeTruthy();
  });

  it("moves a selected formal config column to the library trash from the floating deck", async () => {
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: libraryTrimFixtures.length,
      items: libraryTrimFixtures as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    const listbox = screen.getByRole("listbox");
    fireEvent.click(within(listbox).getByText("Volvo · EX30 · Core"));

    await waitFor(() => {
      expect(within(drawer).getByLabelText("当前已选配置列").textContent).toContain("Core");
    });

    fireEvent.click(screen.getByRole("tab", { name: /已选对象/ }));
    fireEvent.click(within(drawer).getByRole("button", { name: "移入库垃圾桶 Core" }));

    await waitFor(() => {
      expect(api.updateEngineeringConfigTrim).toHaveBeenCalledWith("library-core", {
        status: "trashed",
        comment: "配置核对更正",
      });
    });
    expect(await within(drawer).findByText("Core 已移入配置列库垃圾桶。")).toBeTruthy();
    expect(within(drawer).queryByText("MAT-EX30-CORE")).toBeNull();
  });

  it("restores and clears the current-country config column trash from the floating deck", async () => {
    const trashedCore: VehicleTrimItem = {
      ...libraryTrimFixtures[0],
      trimId: "trashed-core",
      trimName: "Trashed Core",
      fullTrimName: "Volvo EX30 Trashed Core",
      status: "trashed",
    };
    const trashedUltra: VehicleTrimItem = {
      ...libraryTrimFixtures[1],
      trimId: "trashed-ultra",
      trimName: "Trashed Ultra",
      fullTrimName: "Volvo EX30 Trashed Ultra",
      status: "trashed",
    };
    vi.mocked(api.listEngineeringConfigTrims).mockImplementation(async (params) => {
      if (params?.status === "trashed") {
        return {
          rows: 2,
          items: [trashedCore, trashedUltra] as unknown as Record<string, unknown>[],
        };
      }
      return {
        rows: libraryTrimFixtures.length,
        items: libraryTrimFixtures as unknown as Record<string, unknown>[],
      };
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?market=Germany"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    fireEvent.click(screen.getByRole("tab", { name: /已选对象/ }));
    const trashPanel = within(drawer).getByLabelText("配置列库垃圾桶");

    fireEvent.click(within(trashPanel).getByRole("button", { name: "查看 Germany 配置列垃圾桶" }));

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        market: "Germany",
        status: "trashed",
        limit: 100,
      }));
    });
    expect(await within(trashPanel).findByText("Trashed Core")).toBeTruthy();
    expect(within(trashPanel).getByText("Trashed Ultra")).toBeTruthy();

    fireEvent.click(within(trashPanel).getByRole("button", { name: "恢复 Trashed Core 为 Draft" }));
    await waitFor(() => {
      expect(api.updateEngineeringConfigTrim).toHaveBeenCalledWith("trashed-core", {
        status: "draft",
        comment: "配置核对更正",
      });
    });
    expect(await within(drawer).findByText("Trashed Core 已恢复为 Draft 配置列。")).toBeTruthy();

    fireEvent.click(within(trashPanel).getByRole("button", { name: "清空 Germany 配置列垃圾桶（1 项）" }));
    expect(api.clearEngineeringConfigTrimTrash).not.toHaveBeenCalled();
    expect(await within(trashPanel).findByText("再次点击确认清空 Germany 配置列垃圾桶，才会永久清空 1 项。")).toBeTruthy();
    fireEvent.click(within(trashPanel).getByRole("button", { name: "确认清空 Germany 配置列垃圾桶" }));
    await waitFor(() => {
      expect(api.clearEngineeringConfigTrimTrash).toHaveBeenCalledWith("Germany");
    });
    expect(api.updateEngineeringConfigTrim).not.toHaveBeenCalledWith("trashed-ultra", { status: "purged" });
    expect(await within(drawer).findByText("已清空 Germany 配置列库垃圾桶 1 项。")).toBeTruthy();
  });

  it("searches remote trim library from the direct floating deck dropdown", async () => {
    vi.mocked(api.listEngineeringConfigTrims)
      .mockResolvedValueOnce({
        rows: 1,
        items: [libraryTrimFixtures[0]] as unknown as Record<string, unknown>[],
      })
      .mockResolvedValueOnce({
        rows: 9,
        items: [{ ...libraryTrimFixtures[2], brand: "Unknown" }] as unknown as Record<string, unknown>[],
      });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    let directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Volvo · EX30 · Core"));

    directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "smart-config.pdf" } });

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        q: "smart-config.pdf",
        limit: 80,
      }));
    });
    await waitFor(() => {
      expect(within(screen.getByRole("listbox")).getByText("#1 · Premium")).toBeTruthy();
    });
    expect(screen.getByRole("listbox").textContent).not.toContain("Unknown · #1");
    expect(screen.getByRole("listbox").textContent).not.toContain("来源人 alice");
    expect(within(drawer).getByText("配置列库命中 9 个已建配置列，当前拉取 1 个。继续输入品牌 / 车型 / 物料号 / 来源缩小范围。")).toBeTruthy();
    fireEvent.blur(directPicker);
    await waitFor(() => {
      expect(screen.queryByRole("listbox")).toBeNull();
    });
    openSimpleAdvancedSearch();
    const sourcePicker = within(drawer).getByRole("combobox", { name: "Source / File" });
    fireEvent.focus(sourcePicker);
    await waitFor(() => {
      expect(screen.getByRole("listbox").textContent).toContain("alice");
    });
    expect(screen.getByRole("listbox").textContent).toContain("上传人 · smart-config.pdf");
    fireEvent.blur(sourcePicker);
    await waitFor(() => {
      expect(screen.queryByRole("listbox")).toBeNull();
    });
    directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.click(within(screen.getByRole("listbox")).getByText("#1 · Premium"));

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["library-core", "library-rival"], false);
    });
    expect(within(drawer).getByLabelText("当前已选配置列").textContent).toContain("Premium");
  });

  it("opens source digest upload from a direct config column search miss", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "Missing Upload Model" } });

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        q: "Missing Upload Model",
        limit: 80,
      }));
    });
    const sourceEntry = await within(drawer).findByLabelText("直接搜索未命中来源入口");
    expect(sourceEntry.textContent).toContain("库内暂未命中");
    expect(sourceEntry.textContent).toContain("Missing Upload Model");
    expect(sourceEntry.textContent).toContain("上传 xlsx / PDF / 图片 / CSV / HTML / 价格单");

    fireEvent.click(within(sourceEntry).getByRole("button", { name: "搜索 / 上传这个资料" }));

    expect(await within(drawer).findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    const digestSearchInput = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(digestSearchInput.value).toBe("Missing Upload Model");
    expect((within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement).open).toBe(true);
    expect(within(drawer).queryByText(/当前只看来源/)).toBeNull();
    const contextSummary = within(drawer).getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("Missing Upload Model");
    expect(contextSummary?.textContent).toContain("场景 config_library_search_miss");
    expect(contextSummary?.textContent).toContain("身份锚点 品牌 / 车型 / 市场");
  });

  it("uses the direct miss query instead of the existing model filter for upload context", async () => {
    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?brand=Own%20Brand&model=Existing%20Model&market=Germany&powertrain=ICE&segment=SUV%20C&modelYear=2026"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.change(directPicker, { target: { value: "Missing Rival Model" } });

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        q: "Missing Rival Model",
        limit: 80,
      }));
    });
    const sourceEntry = await within(drawer).findByLabelText("直接搜索未命中来源入口");
    fireEvent.click(within(sourceEntry).getByRole("button", { name: "搜索 / 上传这个资料" }));

    expect(await within(drawer).findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    const contextSummary = within(drawer).getByText("当前关联上下文").closest(".config-source-context-summary");
    expect(contextSummary?.textContent).toContain("Own Brand");
    expect(contextSummary?.textContent).toContain("Missing Rival Model");
    expect(contextSummary?.textContent).not.toContain("Existing Model");
    expect(contextSummary?.textContent).toContain("Germany");
    expect(contextSummary?.textContent).toContain("ICE");
    expect(contextSummary?.textContent).toContain("SUV C");
    expect(contextSummary?.textContent).toContain("2026");
    expect(contextSummary?.textContent).toContain("场景 config_library_search_miss");
  });

  it("adds cross-country and cross-model config columns from one direct dropdown without mode selection", async () => {
    const smartFranceTrim: VehicleTrimItem = {
      ...libraryTrimFixtures[2],
      trimId: "library-smart-france",
      market: "France",
      country: "France",
      identityKey: "SMART-1-PREMIUM-FR",
      sourceFileName: "smart-france.pdf",
      sourceFilePath: "/tmp/smart-france.pdf",
    };
    vi.mocked(api.listEngineeringConfigTrims)
      .mockResolvedValueOnce({
        rows: 1,
        items: [libraryTrimFixtures[0]] as unknown as Record<string, unknown>[],
      })
      .mockResolvedValueOnce({
        rows: 1,
        items: [smartFranceTrim] as unknown as Record<string, unknown>[],
      });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    let directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Volvo · EX30 · Core"));

    directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "smart-france.pdf" } });

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        q: "smart-france.pdf",
        limit: 80,
      }));
    });
    await waitFor(() => {
      expect(within(screen.getByRole("listbox")).getByText("Smart · #1 · Premium")).toBeTruthy();
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Smart · #1 · Premium"));

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["library-core", "library-smart-france"], false);
    });
    const selectedColumns = within(drawer).getByLabelText("当前已选配置列");
    expect(selectedColumns.textContent).toContain("Volvo · EX30 · Germany · MY 2026 · BEV + RWD · 物料号 MAT-EX30-CORE · own-ex30.xlsx");
    expect(selectedColumns.textContent).toContain("Smart · #1 · France · MY 2026 · BEV + RWD · Sales version Premium · smart-france.pdf · 来源人 alice");
    expect(drawer.textContent).not.toContain("本品对竞品模式");
    expect(drawer.textContent).not.toContain("竞品对本品模式");
  });

  it("uses sales version as the source upload identity anchor when selected config columns have no material number", async () => {
    const rivalBasicTrim: VehicleTrimItem = {
      ...libraryTrimFixtures[2],
      trimId: "rival-basic",
      trimName: "Basic",
      fullTrimName: "Smart #1 Basic",
      materialNo: null,
      vehicleCode: null,
      identityKey: "SMART-1-BASIC-DE",
      salesVersion: "Basic",
      hasMaterialNo: false,
      dataOrigin: "external_or_scraped",
    };
    const rivalPremiumTrim: VehicleTrimItem = {
      ...libraryTrimFixtures[2],
      trimId: "rival-premium",
      trimName: "Premium",
      fullTrimName: "Smart #1 Premium",
      materialNo: null,
      vehicleCode: null,
      identityKey: "SMART-1-PREMIUM-DE",
      salesVersion: "Premium",
      hasMaterialNo: false,
      dataOrigin: "external_or_scraped",
    };
    vi.mocked(api.listEngineeringConfigTrims).mockResolvedValueOnce({
      rows: 2,
      items: [rivalBasicTrim, rivalPremiumTrim] as unknown as Record<string, unknown>[],
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    openSimpleAdvancedSearch();
    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({ limit: 200 }));
    });
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;

    let directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Smart · #1 · Basic"));

    directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Smart · #1 · Premium"));

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["rival-basic", "rival-premium"], false);
    });

    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    expect(await screen.findByText("拖放配置表或来源文件")).toBeTruthy();
    const fileInput = await waitFor(() => {
      const input = container.querySelector<HTMLInputElement>("input[type='file']");
      expect(input).toBeTruthy();
      return input;
    });
    const file = new File([new Uint8Array([37, 80, 68, 70, 45, 49, 46, 52])], "smart-config.pdf", {
      type: "application/pdf",
    });
    fireEvent.change(fileInput as HTMLInputElement, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));

    await waitFor(() => {
      expect(api.completeEngineeringConfigSourceUpload).toHaveBeenCalledWith("upload-1", expect.objectContaining({
        brand: "Smart",
        model: "#1",
        market: "Germany",
        country: "Germany",
        identityAnchor: "sales_version",
        salesVersionIds: expect.arrayContaining(["Sales version Basic", "Sales version Premium"]),
        contextType: "model_trim_compare",
      }));
    });
  });

  it("scopes direct config column search with the current floating deck filters", async () => {
    vi.mocked(api.listEngineeringConfigTrims).mockImplementation(async (params) => {
      if (params?.q === "MAT-EX30") {
        return {
          rows: 1,
          items: [libraryTrimFixtures[0]] as unknown as Record<string, unknown>[],
        };
      }
      return { rows: 0, items: [] };
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?brand=Volvo&model=EX30&market=Germany&modelYear=2026&powertrain=BEV&segment=SUV%20C&source=own-ex30.xlsx"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        brand: "Volvo",
        model_name: "EX30",
        market: "Germany",
        model_year: "2026",
        energy_type: "BEV",
        source: "own-ex30.xlsx",
        limit: 200,
      }));
    });
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        brand: "Volvo",
        country: "Germany",
        modelYear: "2026",
        powertrain: "BEV",
        segment: "SUV C",
        q: "EX30",
      }));
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const directPicker = within(drawer).getByRole("combobox", { name: "搜索并添加配置列" });
    fireEvent.focus(directPicker);
    fireEvent.change(directPicker, { target: { value: "MAT-EX30" } });

    await waitFor(() => {
      expect(api.listEngineeringConfigTrims).toHaveBeenCalledWith(expect.objectContaining({
        brand: "Volvo",
        model_name: "EX30",
        market: "Germany",
        model_year: "2026",
        energy_type: "BEV",
        source: "own-ex30.xlsx",
        q: "MAT-EX30",
        limit: 80,
      }));
    });
    await waitFor(() => {
      expect(within(screen.getByRole("listbox")).getByText("Volvo · EX30 · Core")).toBeTruthy();
    });
    expect(screen.getByRole("listbox").textContent).toContain(
      "配置列库 / 本品 / Volvo / 市场 Germany / MY 2026 / EX30 / Core / 来源 own-ex30.xlsx",
    );
    expect(within(drawer).getByText("当前筛选范围 Volvo · EX30 · Germany · +3；配置列库命中 1 个已建配置列，当前拉取 1 个。可直接从下拉加入对比。")).toBeTruthy();
  });

  it("keeps target focus when the display drawer switches to common config", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    await focusPremiumDifferenceFromQuickbar();

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });

    switchSummaryMode("expert");

    expect(screen.getByRole("button", { name: "显示范围：共同配置 1 项" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "显示范围：共同配置 1 项" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 1 项配置")).toBeTruthy();
    });
    expect(screen.getByText("共同配置 · 目标 Premium 摘要")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "共同配置1当前范围为一致配置，差异项 0 项")).toBeTruthy();
    expect(screen.getAllByText("Speaker").length).toBeGreaterThan(0);
    expect(screen.queryByText("Blind spot")).toBeNull();
    expect(screen.getByRole("button", { name: "顶部恢复全部配置" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "顶部恢复全部配置" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 3 项配置")).toBeTruthy();
    });
    expect(screen.getByRole("button", { name: "顶部显示全部目标列" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "显示范围：全部配置 3 项" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: "顶部查看差异项" })).toBeTruthy();
  });

  it("keeps target focus when the table scope switches to common config", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    await focusPremiumDifferenceFromQuickbar();

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });

    switchSummaryMode("expert");
    const commonScopeButton = Array.from(container.querySelectorAll<HTMLButtonElement>(".comparison-type-filter .comparison-filter-chip"))
      .find((button) => button.textContent === "共同配置 1");
    expect(commonScopeButton).toBeTruthy();

    fireEvent.click(commonScopeButton as HTMLButtonElement);

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 1 项配置")).toBeTruthy();
    });
    expect(screen.getByText("共同配置 · 目标 Premium 摘要")).toBeTruthy();
    expect(screen.getAllByText("Speaker").length).toBeGreaterThan(0);
    expect(screen.queryByText("Blind spot")).toBeNull();
  });

  it("clears target focus when the focused target becomes the base trim", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    await focusPremiumDifferenceFromQuickbar();

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });

    clickSelectedObjectAction("设 Premium 为基准列");

    await waitFor(() => {
      expect(within(openSelectedObjectsPanel()).getByRole("button", { name: "当前基准列 Premium" })).toBeTruthy();
    });
    expect(screen.queryByText(/当前目标配置列 Premium/)).toBeNull();
    expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    expect(within(openSelectedObjectsPanel()).getByRole("button", { name: "查看 Basic 差异行" })).toBeTruthy();
  });

  it("clears scoped state when the local digest sample is closed", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    await focusPremiumDifferenceFromQuickbar();

    await waitFor(() => {
      expectTableRangeStatusParts(["目标配置列 Premium", "当前展示 2/3 差异行"]);
    });

    closePremiumSampleFromSelectedPanel();

    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    expect(screen.getByText("可以查看本地 xlsx 样例里的同车型不同配置列，或上传 xlsx / PDF / CSV / HTML / 图片作为来源文件快照。")).toBeTruthy();
    expect(screen.getByText("先用下拉搜索库内品牌 / 车型 / 配置列；不要手填配置列，库内未命中再上传来源文件。")).toBeTruthy();
    const emptyState = screen.getByLabelText("配置对比空状态入口");
    const emptyPrimary = emptyState.querySelector(".product-config-empty-actions__primary") as HTMLElement | null;
    const emptySecondary = within(emptyState).getByLabelText("Source Digest 补充入口");
    expect(emptyPrimary).toBeTruthy();
    expect(within(emptyPrimary as HTMLElement).getByRole("button", { name: "搜索配置列" }).classList.contains("btn-primary")).toBe(true);
    expect(screen.getAllByRole("button", { name: "搜索配置列" })).toHaveLength(1);
    expect(emptySecondary.textContent).toContain("库内未命中时再上传来源或打开样例。");
    expect(within(emptySecondary).getByRole("button", { name: "上传配置表 / 价格单" })).toBeTruthy();
    expect(within(emptySecondary).getByRole("button", { name: "查看本地 xlsx 样例" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: "顶部查看差异项" })).toBeNull();
    expect(screen.queryByText(/当前目标配置列 Premium/)).toBeNull();
    expect(screen.queryByText(/当前表格/)).toBeNull();
  });

  it("opens the source upload panel from the empty compare state", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    fireEvent.click(floatingDeckTrigger());
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    const seededDrawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const staleSourceSearchInput = within(seededDrawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    fireEvent.change(staleSourceSearchInput, { target: { value: "stale-source.csv" } });
    expect(staleSourceSearchInput.value).toBe("stale-source.csv");

    closePremiumSampleFromSelectedPanel();

    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    expect(emptyState).toBeTruthy();

    const secondarySourceEntry = within(emptyState as HTMLElement).getByLabelText("Source Digest 补充入口");
    expect(secondarySourceEntry.textContent).toContain("库内未命中时再上传来源或打开样例。");
    fireEvent.click(within(secondarySourceEntry).getByRole("button", { name: "上传配置表 / 价格单" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    expect(screen.getByText("来源归档")).toBeTruthy();
    const libraryScope = screen.getByLabelText("来源库共享范围") as HTMLDetailsElement;
    expect(libraryScope.open).toBe(false);
    expect(screen.queryByText("上传后进入团队共享来源库")).toBeNull();
    expect(screen.getByText("上传诊断")).toBeTruthy();
    expect(screen.getByText("展开后检查 OCR / AI runtime 状态")).toBeTruthy();
    expect(screen.queryByText("PDF / 图片 OCR 已就绪")).toBeNull();
    const diagnostics = screen.getByLabelText("上传诊断") as HTMLDetailsElement;
    expect(diagnostics.open).toBe(false);
    fireEvent.click(within(diagnostics).getByText("上传诊断"));
    expect(await screen.findByText("PDF / 图片 OCR 已就绪")).toBeTruthy();
    expect(screen.getByText("默认 paddleocr，可处理图片和扫描 PDF；Excel、CSV、HTML 仍走结构化解析。")).toBeTruthy();
    expect(screen.getByText("拖放配置表或来源文件")).toBeTruthy();
    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const sourceSearchInputAfterOpen = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(sourceSearchInputAfterOpen.value).toBe("");
    const uploadPanel = within(drawer).getByText("配置表 / 价格单上传（推荐）").closest(".config-source-upload-panel");
    let digestCandidates = within(drawer).queryByText("Source Digest 可比组")?.closest(".market-scan-field") ?? null;
    expect(digestCandidates).toBeNull();
    expect(within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_PICKER_COMBOBOX_NAME })).toBeTruthy();
    const digestDetailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(digestDetailBrowser.open).toBe(false);
    digestCandidates = await openSourceDigestDetailBrowser(drawer);
    expect(Boolean(uploadPanel && digestCandidates && (uploadPanel.compareDocumentPosition(digestCandidates) & Node.DOCUMENT_POSITION_FOLLOWING))).toBe(true);
    expect(within(drawer).queryByText(/Country Source Library/)).toBeNull();
    fireEvent.click(within(drawer).getByRole("button", { name: "去选择 Market / Country" }));
    expect(within(drawer).getByRole("combobox", { name: "搜索并添加配置列" })).toBeTruthy();
    expect(within(drawer).getByRole("combobox", { name: "Market" })).toBeTruthy();
    expect(within(drawer).getByLabelText("来源上传国家绑定提示")).toBeTruthy();
    expect(within(drawer).getByText("先选择 Market / Country")).toBeTruthy();
    expect(within(drawer).queryByText("拖放配置表或来源文件")).toBeNull();

    fireEvent.click(within(drawer).getByRole("button", { name: "回到上传 / 关联来源" }));
    expect(await screen.findByText("拖放配置表或来源文件")).toBeTruthy();
  });

  it("does not auto-bind source uploads to a joined country when selected config columns span markets", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(buildCrossMarketDigest());

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();

    fireEvent.click(floatingDeckTrigger());
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    const libraryScope = screen.getByLabelText("来源库共享范围") as HTMLDetailsElement;
    expect(libraryScope.open).toBe(false);
    expect(within(libraryScope).getByText("团队共享 · 国家待绑定")).toBeTruthy();
    expect(screen.getByRole("button", { name: "去选择 Market / Country" })).toBeTruthy();

    fireEvent.click(within(libraryScope).getByText("来源归档"));

    expect(within(libraryScope).getByText("未选择 Market / Country，建议先用 FloatingDeck 绑定国家")).toBeTruthy();
    expect(within(libraryScope).queryByText("Germany / France")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "去选择 Market / Country" }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    expect(within(drawer).getByRole("combobox", { name: "搜索并添加配置列" })).toBeTruthy();
    const marketInput = within(drawer).getByRole("combobox", { name: "Market" });
    expect(marketInput).toBeTruthy();
    expect(within(drawer).getByLabelText("来源上传国家绑定提示")).toBeTruthy();
    expect(within(drawer).getByRole("button", { name: "回到上传 / 关联来源" })).toBeTruthy();

    fireEvent.click(within(drawer).getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.click(within(drawer).getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    expect(within(drawer).queryByLabelText("来源上传国家绑定提示")).toBeNull();

    fireEvent.click(within(drawer).getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.click(screen.getByRole("button", { name: "去选择 Market / Country" }));

    const reboundMarketInput = within(drawer).getByRole("combobox", { name: "Market" });
    fireEvent.change(reboundMarketInput, { target: { value: "Germany" } });
    fireEvent.click(within(drawer).getByRole("option", { name: "Germany" }));

    expect(within(drawer).getByText("已选择 Germany")).toBeTruthy();

    fireEvent.click(within(drawer).getByRole("button", { name: "回到上传 / 关联来源" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    const reboundLibraryScope = screen.getByLabelText("来源库共享范围") as HTMLDetailsElement;
    expect(within(reboundLibraryScope).getByText("Germany")).toBeTruthy();
    expect(within(reboundLibraryScope).queryByText("团队共享 · 国家待绑定")).toBeNull();
  });

  it("focuses the direct config column picker when adding from the empty state", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    expect(emptyState).toBeTruthy();

    const primaryEntry = (emptyState as HTMLElement).querySelector(".product-config-empty-actions__primary") as HTMLElement | null;
    expect(primaryEntry).toBeTruthy();
    fireEvent.click(within(primaryEntry as HTMLElement).getByRole("button", { name: "搜索配置列" }));

    const directPicker = await screen.findByRole("combobox", { name: "搜索并添加配置列" });
    await waitFor(() => {
      expect(document.activeElement).toBe(directPicker);
      expect(directPicker.getAttribute("aria-expanded")).toBe("true");
    });
    expect(screen.getByRole("listbox").textContent).toContain("当前库内没有匹配配置列");
  });

  it("lets the source panel add digest trims through a searchable Source Model Trim picker", async () => {
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    const pathPreview = within(drawer).getByLabelText("Source Digest 命中路径预览");
    expect(pathPreview.textContent).toContain("compare-sample.xlsx");
    expect(pathPreview.textContent).toContain("T19C MY ICE");
    expect(pathPreview.textContent).toContain("聚焦来源");
    expect(pathPreview.textContent).toContain("聚焦车型");
    expect(pathPreview.textContent).toContain("2 可比配置列");
    const sampleFlow = within(pathPreview).getByLabelText("compare-sample.xlsx 来源车型配置列路径");
    expect(within(sampleFlow).getByText("来源")).toBeTruthy();
    expect(within(sampleFlow).getByText("compare-sample.xlsx")).toBeTruthy();
    expect(within(sampleFlow).getByText("车型")).toBeTruthy();
    expect(within(sampleFlow).getByText("T19C MY ICE")).toBeTruthy();
    expect(within(sampleFlow).getByText("配置列")).toBeTruthy();
    expect(sampleFlow.textContent).toContain("Basic");
    expect(sampleFlow.textContent).toContain("Premium");
    const sourcePicker = within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_PICKER_COMBOBOX_NAME });
    fireEvent.focus(sourcePicker);

    const listbox = await screen.findByRole("listbox");
    expect(listbox.textContent).toContain("本地样例来源 / 品牌待补 / 动力 ICE / compare-sample.xlsx / T19C MY ICE / 配置列 Basic");
    expect(listbox.textContent).toContain("聚焦来源 · compare-sample.xlsx");
    expect(listbox.textContent).toContain("聚焦车型 · T19C MY ICE");

    fireEvent.change(sourcePicker, { target: { value: "Premium" } });
    expect(within(drawer).getByLabelText("来源当前搜索范围").textContent).toContain("Premium");
    fireEvent.keyDown(sourcePicker, { key: "Escape" });
    expect((sourcePicker as HTMLInputElement).value).toBe("");
    expect(within(drawer).queryByLabelText("来源当前搜索范围")).toBeNull();

    fireEvent.focus(sourcePicker);

    const basicOption = await screen.findByRole("option", {
      name: /暂选预览列 · T19C MY ICE · Basic/,
    });
    fireEvent.click(basicOption);

    const pendingPanel = await within(drawer).findByLabelText("待生成来源配置列");
    expect(pendingPanel.textContent).toContain("T19C MY ICE");
    expect(pendingPanel.textContent).toContain("Basic");
    expect(pendingPanel.textContent).toContain("1/4");
    expect(pendingPanel.textContent).toContain("当前暂选按同一个来源 / 车型生成");
    expect(within(pendingPanel).getByLabelText("T19C MY ICE 待生成来源路径").textContent).toContain("来源compare-sample.xlsx / T19C MY ICE");
    expect(within(drawer).queryByText(/已暂选 1\/4 个配置列/)).toBeNull();
  });

  it("refreshes floating source digest candidates after uploading a source snapshot", async () => {
    const uploadedSource = {
      ...buildSourceSnapshotFixture("source-1", "compare-sample.xlsx", digest),
      sourceSearchMatches: ["Model T19C"],
      sourceDigestStatus: {
        digestType: "workbook",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    };
    let sourceSearchCalls = 0;
    vi.mocked(api.listEngineeringConfigSourceSnapshots).mockImplementation(async (options) => {
      if (typeof options !== "number" && options?.q === "T19C") {
        sourceSearchCalls += 1;
        return sourceSearchCalls === 1
          ? { rows: 0, items: [] }
          : { rows: 1, items: [uploadedSource] };
      }
      return { rows: 0, items: [] };
    });
    vi.mocked(api.getEngineeringConfigSourceSnapshot).mockResolvedValue(uploadedSource);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: SOURCE_PANEL_TAB_NAME }));
    fireEvent.change(screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }), {
      target: { value: "T19C" },
    });

    const drawer = container.querySelector(".deck-floating-panel") as HTMLElement;
    await waitFor(() => {
      expect(api.listEngineeringConfigSourceSnapshots).toHaveBeenCalledWith(expect.objectContaining({
        q: "T19C",
      }));
    });
    const detailBrowser = within(drawer).getByLabelText("来源组详情浏览") as HTMLDetailsElement;
    expect(detailBrowser.open).toBe(false);
    expect(within(drawer).queryByLabelText("来源库轻量命中")).toBeNull();

    const fileInput = await waitFor(() => {
      const input = container.querySelector<HTMLInputElement>(".deck-floating-panel input[type='file']");
      if (!input) throw new Error("Source upload input not mounted");
      return input;
    });
    const file = new File([new Uint8Array([80, 75, 3, 4])], "compare-sample.xlsx", {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    fireEvent.change(fileInput, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));

    await waitFor(() => {
      expect(api.completeEngineeringConfigSourceUpload).toHaveBeenCalled();
    });
    await waitFor(() => {
      expect(within(drawer).getByLabelText("来源库轻量命中").textContent).toContain("compare-sample.xlsx");
    });
    expect(detailBrowser.open).toBe(false);
    const digestCandidates = await openSourceDigestDetailBrowser(drawer);
    await waitFor(() => {
      const candidateButtons = within(digestCandidates).getAllByRole("button", { name: /选择 Source Digest 可比组：T19C MY ICE/ });
      expect(candidateButtons.some((button) => (
        button.textContent?.includes("来源库")
        && button.textContent.includes("Excel")
        && button.textContent.includes("上传人 tester")
      ))).toBe(true);
    });
    expect(within(digestCandidates).getByText("来源库命中 1 个来源，当前显示 1/1 个可转配置列组。")).toBeTruthy();
    const sourceSearchBridge = within(drawer).getByLabelText("上传来源回到 FloatingDeck 搜索");
    fireEvent.click(within(sourceSearchBridge).getByRole("button", { name: "在 FloatingDeck 搜索这个来源" }));

    await waitFor(() => {
      expect((within(drawer).getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement).value)
        .toBe("compare-sample.xlsx");
    });
  });

  it("creates editable compare data from a workbook digest and switches to formal compare", async () => {
    const formalDraftCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-basic",
          fullTrimName: "Draft Basic",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Basic",
          market: "EU",
          modelYear: "2026",
          materialNo: "MM001",
          salesVersion: "Basic",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-premium",
          fullTrimName: "Draft Premium",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Premium",
          market: "EU",
          modelYear: "2026",
          materialNo: "MM002",
          salesVersion: "Premium",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 1,
        uniqueFeatureCount: 0,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Comfort"],
      },
      rows: [
        {
          category: "Comfort",
          featureId: "feature-seat-heat",
          featureCode: "seat_heat",
          featureName: "Seat heating",
          comparisonType: "DIFFERENT_VALUE",
          uniqueTrimIds: [],
          businessNote: "配置值不同",
          values: [
            {
              valueId: "value-basic-seat",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "compare-sample.xlsx",
                rowNumber: 12,
                columnNumber: 0,
                columnLetter: "D",
                cell: "D12",
                sourceCell: "D12",
                mergedRange: null,
              },
            },
            {
              valueId: "value-premium-seat",
              rawValue: "O",
              normalizedValue: "optional",
              availability: "OPTIONAL",
              unit: null,
              displayValue: "选装",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "compare-sample.xlsx",
                rowNumber: 12,
                columnNumber: 0,
                columnLetter: "E",
                cell: "E12",
                sourceCell: "E12",
                mergedRange: null,
              },
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalDraftCompare) as unknown as Record<string, unknown>);
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    closePremiumSampleFromSelectedPanel();
    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    fireEvent.click(within(emptyState as HTMLElement).getByRole("button", { name: "上传配置表 / 价格单" }));

    const fileInput = await waitFor(() => {
      const input = container.querySelector<HTMLInputElement>("input[type='file']");
      expect(input).toBeTruthy();
      return input as HTMLInputElement;
    });
    const file = new File([new Uint8Array([80, 75, 3, 4])], "compare-sample.xlsx", {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    fireEvent.change(fileInput, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));
    fireEvent.click(await screen.findByRole("button", { name: "创建可编辑配置列" }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-1", "t19c");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-basic", "draft-premium"], false, "latest");
    });
    const uploadDraftSuccess = screen.getByLabelText("来源建列成功");
    expect(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }).getAttribute("aria-selected")).toBe("true");
    const uploadDraftPath = within(uploadDraftSuccess).getByLabelText("建列来源路径");
    expect(uploadDraftPath.textContent).toContain("compare-sample.xlsx");
    expect(uploadDraftPath.textContent).toContain("T19C MY ICE");
    expect(uploadDraftPath.textContent).toContain("Basic");
    expect(uploadDraftPath.textContent).toContain("Premium");
    expect(within(uploadDraftSuccess).getByLabelText("建列结果摘要").textContent).toContain("配置值6");
    expect(within(uploadDraftSuccess).getByLabelText("建列后工作区").textContent).toContain("在线编辑和导出当前表格");
    expect(within(uploadDraftSuccess).getByLabelText("建列后 AI 摘要边界").textContent).toContain("可随 XLSX / PDF 导出");
    expect(uploadDraftSuccess.textContent).toContain("不写回来源解析记录");
    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 1/1 配置行")).toBeTruthy();
    expect(container.querySelectorAll(".compare-cell--editable")).toHaveLength(0);
    const drawerTrigger = screen.getByRole("button", { name: /添加配置列 \/ 显示/ });
    expect(drawerTrigger.textContent).not.toContain("编辑");
    if (drawerTrigger.getAttribute("aria-expanded") !== "true") fireEvent.click(drawerTrigger);
    const editControl = screen.getByLabelText("在线编辑控制");
    expect(within(editControl).getByText("编辑未开启")).toBeTruthy();
    fireEvent.click(within(editControl).getByRole("button", { name: "开启在线编辑" }));
    await waitFor(() => {
      expect(container.querySelectorAll(".compare-cell--editable").length).toBeGreaterThan(0);
    });
    expect(within(editControl).getByText("编辑已开启")).toBeTruthy();
    expect(screen.getByRole("button", { name: /编辑已开启 \/ 显示/ })).toBeTruthy();
    expect(screen.getByLabelText(/在线编辑状态：编辑已开启/)).toBeTruthy();
    expect(screen.queryByLabelText("在线编辑安全提示")).toBeNull();
    const tableEditStatus = screen.getByLabelText("配置表在线编辑状态");
    expect(tableEditStatus.textContent).toContain("在线编辑已开启");
    expect(tableEditStatus.textContent).toContain("点击配置值进入编辑");
    fireEvent.click(screen.getByRole("button", { name: /编辑已开启 \/ 显示/ }));
    expect(screen.getByRole("button", { name: /打开编辑控制/ })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /编辑已开启 \/ 显示/ }));
    expect(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }).getAttribute("aria-selected")).toBe("true");
    vi.mocked(api.updateEngineeringConfigTrim).mockResolvedValueOnce({
      trimId: "draft-basic",
      fullTrimName: "Draft Basic",
      brand: "OMODA",
      modelName: "T19C",
      trimName: "Basic",
      market: "Germany",
      country: "Germany",
      modelYear: "2026",
      energyType: null,
      drivetrain: null,
      engine: null,
      vehicleCode: "BASIC-SV",
      materialNo: "T71607V**MM0001",
      identityKey: "T19C-BASIC-DE",
      salesVersion: "BASIC-SV",
      status: "draft",
      hasMaterialNo: true,
      dataOrigin: "own_catalog",
    });
    const identityEditor = screen.getByLabelText("配置列身份编辑");
    expect(identityEditor.textContent).toContain("配置列身份");
    fireEvent.change(within(identityEditor).getByLabelText("Market / Country"), { target: { value: "Germany" } });
    fireEvent.change(within(identityEditor).getByLabelText("物料号"), { target: { value: "T71607V**MM0001" } });
    fireEvent.change(within(identityEditor).getByLabelText("Sales version"), { target: { value: "BASIC-SV" } });
    fireEvent.change(within(identityEditor).getByLabelText("Identity key"), { target: { value: "T19C-BASIC-DE" } });
    fireEvent.click(within(identityEditor).getByRole("button", { name: "保存配置列身份" }));
    await waitFor(() => {
      expect(api.updateEngineeringConfigTrim).toHaveBeenCalledWith("draft-basic", expect.objectContaining({
        market: "Germany",
        material_no: "T71607V**MM0001",
        vehicle_code: "BASIC-SV",
        identity_key: "T19C-BASIC-DE",
        comment: "配置核对更正",
      }));
    });
    expect(await within(identityEditor).findByText("配置列身份已保存。")).toBeTruthy();
    const currentEditControl = screen.getByLabelText("在线编辑控制");
    expect(currentEditControl).toBeTruthy();
    fireEvent.click(within(currentEditControl).getByRole("button", { name: "关闭在线编辑" }));
    await waitFor(() => {
      expect(container.querySelectorAll(".compare-cell--editable")).toHaveLength(0);
    });
    expect(within(currentEditControl).getByText("编辑未开启")).toBeTruthy();
  });

  it("creates editable compare data from a text PDF source digest and switches to formal compare", async () => {
    const pdfGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "pdf-source-model",
      title: "Rival PDF Model",
      sourceSheet: "PDF Page 1",
      modelName: "Rival PDF Model",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        trimId: `pdf-${trim.trimId}`,
        modelName: "Rival PDF Model",
        materialNo: null,
        hasMaterialNo: false,
        dataOrigin: "external_or_scraped",
        sourceSheet: "PDF Page 1",
      })),
    };
    const pdfDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "pdf_text",
      sourceFormat: "pdf_text",
      fileName: "rival-config.pdf",
      modelName: "Rival PDF Model",
      summary: {
        ...digest.summary,
        comparableGroupCount: 1,
        candidateTrimCount: 2,
      },
      compareGroups: [pdfGroup],
    };
    vi.mocked(api.completeEngineeringConfigSourceUpload).mockResolvedValueOnce({
      ...buildSourceSnapshotFixture("source-pdf", "rival-config.pdf", pdfDigest),
      fileType: "pdf",
      mimeType: "application/pdf",
      parseMode: "stored_source",
      message: "Source snapshot registered.",
      sourceDigestStatus: {
        digestType: "pdf_text",
        status: "ready",
        sourceFormat: "pdf_text",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-pdf",
      groupId: "pdf-source-model",
      importBatchId: "draft-pdf",
      sourceFileName: "rival-config.pdf",
      groupTitle: "Rival PDF Model",
      sourceDigestType: "pdf_text",
      sourceFormat: "pdf_text",
      sourceKind: "config_matrix",
      trimIds: ["draft-pdf-basic", "draft-pdf-premium"],
      compareTrimIds: ["draft-pdf-basic", "draft-pdf-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 1,
      createdFeatureCount: 1,
      reusedFeatureCount: 0,
      valueRecordCount: 2,
      insertedValueCount: 2,
      updatedValueCount: 0,
      createdVersionIds: ["version-pdf-basic", "version-pdf-premium"],
    });
    const formalPdfCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-pdf-basic",
          fullTrimName: "PDF Basic",
          brand: "Rival",
          modelName: "Rival PDF Model",
          trimName: "Basic",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Basic",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-pdf-premium",
          fullTrimName: "PDF Premium",
          brand: "Rival",
          modelName: "Rival PDF Model",
          trimName: "Premium",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Premium",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 0,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Safety"],
      },
      rows: [
        {
          category: "Safety",
          featureId: "feature-pdf-blind",
          featureCode: "blind_spot",
          featureName: "Blind spot",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["draft-pdf-premium"],
          businessNote: "PDF 来源配置差异",
          values: [
            {
              valueId: "value-pdf-basic-blind",
              rawValue: "-",
              normalizedValue: "not_available",
              availability: "NOT_AVAILABLE",
              unit: null,
              displayValue: "不配备",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "PDF Page 1",
                rowNumber: 18,
                columnNumber: 4,
                columnLetter: "D",
                cell: "D18",
                sourceCell: "D18",
                mergedRange: null,
              },
            },
            {
              valueId: "value-pdf-premium-blind",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "PDF Page 1",
                rowNumber: 18,
                columnNumber: 5,
                columnLetter: "E",
                cell: "E18",
                sourceCell: "E18",
                mergedRange: null,
              },
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalPdfCompare) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    closePremiumSampleFromSelectedPanel();
    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    fireEvent.click(within(emptyState as HTMLElement).getByRole("button", { name: "上传配置表 / 价格单" }));

    const fileInput = container.querySelector<HTMLInputElement>("input[type='file']");
    const file = new File([new Uint8Array([37, 80, 68, 70, 45, 49, 46, 52])], "rival-config.pdf", {
      type: "application/pdf",
    });
    fireEvent.change(fileInput as HTMLInputElement, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));
    fireEvent.click(await screen.findByRole("button", { name: "创建可编辑配置列" }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-pdf", "pdf-source-model");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-pdf-basic", "draft-pdf-premium"], false, "latest");
    });
    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 1/1 配置行")).toBeTruthy();
    expect(screen.getAllByText("PDF Premium").length).toBeGreaterThan(0);
  });

  it("creates editable compare data from a scanned PDF OCR source digest and switches to formal compare", async () => {
    const pdfOcrGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "pdf-ocr-source-model",
      title: "Scanned PDF Model",
      sourceSheet: "PDF OCR Page 1",
      modelName: "Scanned PDF Model",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        trimId: `pdf-ocr-${trim.trimId}`,
        fullTrimName: trim.fullTrimName.replace("两驱", "PDF OCR "),
        trimName: trim.trimName.replace("两驱", "PDF OCR "),
        modelName: "Scanned PDF Model",
        materialNo: null,
        hasMaterialNo: false,
        dataOrigin: "external_or_scraped",
        sourceSheet: "PDF OCR Page 1",
      })),
      rows: digest.compareGroups[0].rows.map((row) => ({
        ...row,
        values: row.values.map((value) =>
          value
            ? {
                ...value,
                source: {
                  sheetName: "PDF OCR Page 1",
                  rowNumber: value.source?.rowNumber ?? 18,
                  columnNumber: value.source?.columnNumber ?? 4,
                  columnLetter: value.source?.columnLetter ?? "D",
                  cell: value.source?.cell ?? "D18",
                  sourceCell: value.source?.sourceCell ?? value.source?.cell ?? "D18",
                  mergedRange: value.source?.mergedRange ?? null,
                  sourceType: "pdf_ocr",
                  pageNumber: 1,
                  ocrEngine: "paddleocr",
                },
              }
            : null,
        ),
      })),
    };
    const pdfOcrDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "pdf_ocr",
      sourceFormat: "pdf_ocr",
      ocrEngine: "paddleocr",
      fileName: "scanned-config.pdf",
      modelName: "Scanned PDF Model",
      ocrEvaluation: {
        strategy: "highest_config_semantic_score",
        reason: "highest_config_semantic_score",
        candidateCount: 2,
        comparableCandidateCount: 2,
        selectedCandidateCount: 1,
        selectedEngine: "paddleocr",
        selectedEngines: ["paddleocr"],
        selectedScore: {
          semanticScore: 1,
          comparableGroupCount: 1,
          featureCount: 1,
          differenceCount: 1,
          candidateTrimCount: 2,
          totalFeatureCount: 1,
          totalDifferenceCount: 1,
          totalCandidateTrimCount: 2,
          tableShapeScore: 18,
          rowCount: 4,
          columnCount: 5,
          nonEmptyCount: 16,
        },
        selectedSheetName: "PDF OCR Page 1",
        selectedPageNumber: 1,
      },
      ocrEngineCandidates: [
        {
          engine: "legacy_pdf_ocr",
          sourceType: "pdf_ocr",
          sheetName: "PDF OCR Page 1",
          pageNumber: 1,
          selected: false,
          comparableTableDetected: true,
          score: {
            semanticScore: 1,
            comparableGroupCount: 1,
            featureCount: 1,
            differenceCount: 1,
            candidateTrimCount: 2,
            totalFeatureCount: 1,
            totalDifferenceCount: 1,
            totalCandidateTrimCount: 2,
            tableShapeScore: 8,
            rowCount: 3,
            columnCount: 3,
            nonEmptyCount: 9,
          },
        },
        {
          engine: "paddleocr",
          sourceType: "pdf_ocr",
          sheetName: "PDF OCR Page 1",
          pageNumber: 1,
          selected: true,
          comparableTableDetected: true,
          score: {
            semanticScore: 1,
            comparableGroupCount: 1,
            featureCount: 1,
            differenceCount: 1,
            candidateTrimCount: 2,
            totalFeatureCount: 1,
            totalDifferenceCount: 1,
            totalCandidateTrimCount: 2,
            tableShapeScore: 18,
            rowCount: 4,
            columnCount: 5,
            nonEmptyCount: 16,
          },
        },
      ],
      summary: {
        ...digest.summary,
        comparableGroupCount: 1,
        candidateTrimCount: 2,
      },
      compareGroups: [pdfOcrGroup],
    };
    vi.mocked(api.completeEngineeringConfigSourceUpload).mockResolvedValueOnce({
      ...buildSourceSnapshotFixture("source-pdf-ocr", "scanned-config.pdf", pdfOcrDigest),
      fileType: "pdf",
      mimeType: "application/pdf",
      parseMode: "stored_source",
      message: "Source snapshot registered.",
      sourceDigestStatus: {
        digestType: "pdf_ocr",
        status: "ready",
        sourceFormat: "pdf_ocr",
        ocrEngine: "paddleocr",
        ocrEvaluation: pdfOcrDigest.ocrEvaluation ?? null,
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-pdf-ocr",
      groupId: "pdf-ocr-source-model",
      importBatchId: "draft-pdf-ocr",
      sourceFileName: "scanned-config.pdf",
      groupTitle: "Scanned PDF Model",
      sourceDigestType: "pdf_ocr",
      sourceFormat: "pdf_ocr",
      sourceKind: "config_matrix",
      ocrEngine: "paddleocr",
      ocrEvaluation: pdfOcrDigest.ocrEvaluation ?? null,
      ocrEngineCandidates: pdfOcrDigest.ocrEngineCandidates ?? [],
      trimIds: ["draft-pdf-ocr-basic", "draft-pdf-ocr-premium"],
      compareTrimIds: ["draft-pdf-ocr-basic", "draft-pdf-ocr-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 1,
      createdFeatureCount: 1,
      reusedFeatureCount: 0,
      valueRecordCount: 2,
      insertedValueCount: 2,
      updatedValueCount: 0,
      createdVersionIds: ["version-pdf-ocr-basic", "version-pdf-ocr-premium"],
    });
    const formalPdfOcrCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-pdf-ocr-basic",
          fullTrimName: "PDF OCR Basic",
          brand: "Rival",
          modelName: "Scanned PDF Model",
          trimName: "Basic",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Basic",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-pdf-ocr-premium",
          fullTrimName: "PDF OCR Premium",
          brand: "Rival",
          modelName: "Scanned PDF Model",
          trimName: "Premium",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Premium",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 0,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Safety"],
      },
      rows: [
        {
          category: "Safety",
          featureId: "feature-pdf-ocr-camera",
          featureCode: "pdf_ocr_camera",
          featureName: "PDF OCR 360 camera",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["draft-pdf-ocr-premium"],
          businessNote: "扫描 PDF OCR 来源配置差异",
          values: [
            {
              valueId: "value-pdf-ocr-basic-camera",
              rawValue: "-",
              normalizedValue: "not_available",
              availability: "NOT_AVAILABLE",
              unit: null,
              displayValue: "不配备",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "PDF OCR Page 1",
                rowNumber: 20,
                columnNumber: 4,
                columnLetter: "D",
                cell: "D20",
                sourceCell: "D20",
                mergedRange: null,
                sourceType: "pdf_ocr",
                pageNumber: 1,
                ocrEngine: "paddleocr",
              },
            },
            {
              valueId: "value-pdf-ocr-premium-camera",
              rawValue: "O",
              normalizedValue: "optional",
              availability: "OPTIONAL",
              unit: null,
              displayValue: "选装",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "PDF OCR Page 1",
                rowNumber: 20,
                columnNumber: 5,
                columnLetter: "E",
                cell: "E20",
                sourceCell: "E20",
                mergedRange: null,
                sourceType: "pdf_ocr",
                pageNumber: 1,
                ocrEngine: "paddleocr",
              },
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalPdfOcrCompare) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    closePremiumSampleFromSelectedPanel();
    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    fireEvent.click(within(emptyState as HTMLElement).getByRole("button", { name: "上传配置表 / 价格单" }));
    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();

    const fileInput = container.querySelector<HTMLInputElement>("input[type='file']");
    const file = new File([new Uint8Array([37, 80, 68, 70, 45, 49, 46, 52])], "scanned-config.pdf", {
      type: "application/pdf",
    });
    fireEvent.change(fileInput as HTMLInputElement, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));

    expect(await screen.findByText("扫描 PDF OCR digest")).toBeTruthy();
    expect(screen.getAllByText("OCR 选择 paddleocr").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByLabelText("来源上传下一步").textContent).toContain("OCR 透明度：OCR 已比较 2 个候选");
    expect(screen.getByLabelText("来源上传下一步").textContent).toContain("采用 paddleocr");
    expect(screen.getByText("PDF OCR Page 1 · 第 1 页 · 识别到可比表 · 配置项 1 · 差异 1 · 配置列 2 · 候选组 1 · 表格 3 x 3 · 非空 9 · 语义分 1")).toBeTruthy();
    expect(screen.getByText("PDF OCR Page 1 · 第 1 页 · 识别到可比表 · 配置项 1 · 差异 1 · 配置列 2 · 候选组 1 · 表格 4 x 5 · 非空 16 · 语义分 1")).toBeTruthy();
    expect(screen.getAllByText("OCR 对比：paddleocr 胜出；相对 legacy_pdf_ocr 多识别 7 个非空单元，表格结构分 +10。").length).toBeGreaterThanOrEqual(1);
    fireEvent.click(screen.getByRole("button", { name: "创建可编辑配置列" }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith("source-pdf-ocr", "pdf-ocr-source-model");
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-pdf-ocr-basic", "draft-pdf-ocr-premium"], false, "latest");
    });
    const pdfOcrDraftSuccess = screen.getByLabelText("来源建列成功");
    const pdfOcrTransparency = within(pdfOcrDraftSuccess).getByLabelText("来源建列 OCR 透明度");
    expect(pdfOcrTransparency.textContent).toContain("OCR 采用 paddleocr");
    expect(pdfOcrTransparency.textContent).toContain("候选 2");
    expect(pdfOcrTransparency.textContent).toContain("可比候选 2/2");
    expect(pdfOcrTransparency.textContent).toContain("按配置表语义选优");
    expect(pdfOcrTransparency.textContent).toContain("选中表格 4 x 5");
    expect(pdfOcrTransparency.textContent).toContain("OCR 对比：paddleocr 胜出");
    expect(pdfOcrTransparency.textContent).toContain("引用卖点前仍建议点开 evidence 核对原 PDF / 图片");
    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 1/1 配置行")).toBeTruthy();
    expect(screen.getAllByText("PDF OCR Premium").length).toBeGreaterThan(0);
  });

  it("creates editable compare data from a price list source digest and switches to formal compare", async () => {
    const priceListGroup: EngineeringConfigSourceDigestGroup = {
      groupId: "price-list-competitor-price-list-1",
      title: "OMODA / T19C / Germany / 2026 / ICE / 价格单",
      sourceSheet: "competitor-price-list",
      modelName: "T19C",
      trimCount: 2,
      featureCount: 2,
      differenceCount: 1,
      sourceKind: "price_list",
      summary: {
        totalFeatures: 2,
        shownFeatures: 2,
        commonSameCount: 1,
        differentValueCount: 1,
        uniqueFeatureCount: 0,
        partialAvailableCount: 0,
        uniqueOrPartialCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        availabilityDifferentCount: 0,
        differenceCount: 1,
        differenceCategories: ["价格 Pricing"],
        categoryCounts: {
          "价格 Pricing": 2,
        },
      },
      trims: [
        {
          trimId: "price-basic",
          trimName: "Basic",
          fullTrimName: "OMODA / T19C / Basic",
          modelName: "T19C",
          sourceSheet: "competitor-price-list",
          market: "Germany",
          country: "Germany",
          materialNo: null,
          salesVersion: null,
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          profile: {
            country: "Germany",
            modelYear: "2026",
          },
        },
        {
          trimId: "price-premium",
          trimName: "Premium",
          fullTrimName: "OMODA / T19C / Premium",
          modelName: "T19C",
          sourceSheet: "competitor-price-list",
          market: "Germany",
          country: "Germany",
          materialNo: null,
          salesVersion: null,
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          profile: {
            country: "Germany",
            modelYear: "2026",
          },
        },
      ],
      rows: [
        {
          category: "价格 Pricing",
          featureCode: "price_list_MSRP",
          featureName: "MSRP",
          comparisonType: "DIFFERENT_VALUE",
          uniqueTrimIds: [],
          businessNote: "价格单字段，可与配置项一起进入在线编辑、导出和来源证据核对。",
          values: [
            {
              valueId: "price-basic-msrp",
              rawValue: "23000",
              normalizedValue: "23000",
              availability: "VALUE",
              unit: null,
              valueState: "numeric_value",
              displayValue: "23000",
              inferred: false,
              source: {
                sheetName: "competitor-price-list",
                rowNumber: 2,
                columnNumber: 7,
                columnLetter: "G",
                cell: "G2",
                sourceCell: "G2",
                mergedRange: null,
              },
            },
            {
              valueId: "price-premium-msrp",
              rawValue: "28000",
              normalizedValue: "28000",
              availability: "VALUE",
              unit: null,
              valueState: "numeric_value",
              displayValue: "28000",
              inferred: false,
              source: {
                sheetName: "competitor-price-list",
                rowNumber: 3,
                columnNumber: 7,
                columnLetter: "G",
                cell: "G3",
                sourceCell: "G3",
                mergedRange: null,
              },
            },
          ],
        },
        {
          category: "价格 Pricing",
          featureCode: "price_list_Currency",
          featureName: "Currency",
          comparisonType: "COMMON_SAME",
          uniqueTrimIds: [],
          businessNote: "价格单字段，可与配置项一起进入在线编辑、导出和来源证据核对。",
          values: [
            {
              valueId: "price-basic-currency",
              rawValue: "EUR",
              normalizedValue: "EUR",
              availability: "VALUE",
              unit: null,
              valueState: "text_value",
              displayValue: "EUR",
              inferred: false,
              source: {
                sheetName: "competitor-price-list",
                rowNumber: 2,
                columnNumber: 8,
                columnLetter: "H",
                cell: "H2",
                sourceCell: "H2",
                mergedRange: null,
              },
            },
            {
              valueId: "price-premium-currency",
              rawValue: "EUR",
              normalizedValue: "EUR",
              availability: "VALUE",
              unit: null,
              valueState: "text_value",
              displayValue: "EUR",
              inferred: false,
              source: {
                sheetName: "competitor-price-list",
                rowNumber: 3,
                columnNumber: 8,
                columnLetter: "H",
                cell: "H3",
                sourceCell: "H3",
                mergedRange: null,
              },
            },
          ],
        },
      ],
    };
    const priceListDigest: EngineeringConfigSourceDigest = {
      digestType: "tabular",
      status: "ready",
      fileName: "competitor-price-list.csv",
      modelName: "T19C",
      summary: {
        sheetCount: 1,
        tableCount: 1,
        candidateTrimCount: 2,
        comparableGroupCount: 1,
        featureCount: 2,
        differenceCount: 1,
      },
      sheets: [
        {
          name: "competitor-price-list",
          rowCount: 3,
          columnCount: 8,
          nonEmptyCellCount: 24,
          sampleRows: [
            ["Brand", "Model", "Trim", "Market", "Model Year", "Powertrain", "MSRP", "Currency"],
            ["OMODA", "T19C", "Basic", "Germany", "2026", "ICE", "23000", "EUR"],
            ["OMODA", "T19C", "Premium", "Germany", "2026", "ICE", "28000", "EUR"],
          ],
        },
      ],
      compareGroups: [priceListGroup],
    };
    vi.mocked(api.completeEngineeringConfigSourceUpload).mockResolvedValueOnce({
      ...buildSourceSnapshotFixture("source-price-list", "competitor-price-list.csv", priceListDigest),
      fileType: "csv",
      mimeType: "text/csv",
      parseMode: "stored_source",
      message: "Source snapshot registered.",
      sourceDigestStatus: {
        digestType: "tabular",
        status: "ready",
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 2,
          differenceCount: 1,
        },
      },
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-price-list",
      groupId: "price-list-competitor-price-list-1",
      importBatchId: "draft-price-list",
      sourceFileName: "competitor-price-list.csv",
      groupTitle: "OMODA / T19C / Germany / 2026 / ICE / 价格单",
      sourceDigestType: "tabular",
      sourceKind: "price_list",
      trimIds: ["draft-price-basic", "draft-price-premium"],
      compareTrimIds: ["draft-price-basic", "draft-price-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 2,
      createdFeatureCount: 2,
      reusedFeatureCount: 0,
      valueRecordCount: 4,
      insertedValueCount: 4,
      updatedValueCount: 0,
      createdVersionIds: ["version-price-basic", "version-price-premium"],
    });
    const formalPriceCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-price-basic",
          fullTrimName: "OMODA / T19C / Basic",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Basic",
          market: "Germany",
          country: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: null,
          msrp: 23000,
          targetPrice: null,
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
        },
        {
          trimId: "draft-price-premium",
          fullTrimName: "OMODA / T19C / Premium",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Premium",
          market: "Germany",
          country: "Germany",
          modelYear: "2026",
          materialNo: null,
          salesVersion: null,
          msrp: 28000,
          targetPrice: null,
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
        },
      ],
      summary: {
        totalFeatures: 2,
        shownFeatures: 2,
        commonSameCount: 1,
        differentValueCount: 1,
        uniqueFeatureCount: 0,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["价格 Pricing"],
      },
      rows: [
        {
          category: "价格 Pricing",
          featureId: "feature-price-msrp",
          featureCode: "price_list_MSRP",
          featureName: "MSRP",
          comparisonType: "DIFFERENT_VALUE",
          uniqueTrimIds: [],
          businessNote: "价格单字段",
          values: [
            {
              valueId: "value-price-basic-msrp",
              rawValue: "23000",
              normalizedValue: "23000",
              availability: "VALUE",
              unit: null,
              displayValue: "23000",
              valueState: "numeric_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "competitor-price-list",
                rowNumber: 2,
                columnNumber: 7,
                columnLetter: "G",
                cell: "G2",
                sourceCell: "G2",
                mergedRange: null,
              },
            },
            {
              valueId: "value-price-premium-msrp",
              rawValue: "28000",
              normalizedValue: "28000",
              availability: "VALUE",
              unit: null,
              displayValue: "28000",
              valueState: "numeric_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "competitor-price-list",
                rowNumber: 3,
                columnNumber: 7,
                columnLetter: "G",
                cell: "G3",
                sourceCell: "G3",
                mergedRange: null,
              },
            },
          ],
        },
        {
          category: "价格 Pricing",
          featureId: "feature-price-currency",
          featureCode: "price_list_Currency",
          featureName: "Currency",
          comparisonType: "COMMON_SAME",
          uniqueTrimIds: [],
          businessNote: "价格单字段",
          values: [
            {
              valueId: "value-price-basic-currency",
              rawValue: "EUR",
              normalizedValue: "EUR",
              availability: "VALUE",
              unit: null,
              displayValue: "EUR",
              valueState: "text_value",
              version: 1,
              inferred: false,
              source: null,
            },
            {
              valueId: "value-price-premium-currency",
              rawValue: "EUR",
              normalizedValue: "EUR",
              availability: "VALUE",
              unit: null,
              displayValue: "EUR",
              valueState: "text_value",
              version: 1,
              inferred: false,
              source: null,
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 2,
      shownFeatures: 2,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalPriceCompare) as unknown as Record<string, unknown>);
    vi.mocked(api.updateEngineeringConfigFeatureValue).mockResolvedValueOnce({
      valueId: "value-price-premium-msrp",
      rawValue: "28500",
      normalizedValue: "28500",
      availability: "VALUE",
      displayValue: "28500",
      valueState: "numeric_value",
      version: 2,
    });

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    closePremiumSampleFromSelectedPanel();
    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    fireEvent.click(within(emptyState as HTMLElement).getByRole("button", { name: "上传配置表 / 价格单" }));
    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();

    const fileInput = container.querySelector<HTMLInputElement>("input[type='file']");
    const file = new File(
      [
        [
          "Brand,Model,Trim,Market,Model Year,Powertrain,MSRP,Currency",
          "OMODA,T19C,Basic,Germany,2026,ICE,23000,EUR",
          "OMODA,T19C,Premium,Germany,2026,ICE,28000,EUR",
        ].join("\n"),
      ],
      "competitor-price-list.csv",
      { type: "text/csv" },
    );
    fireEvent.change(fileInput as HTMLInputElement, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));

    expect(await screen.findByText("价格单 digest")).toBeTruthy();
    expect(screen.getByText("OMODA / T19C / Germany / 2026 / ICE / 价格单")).toBeTruthy();
    expect(screen.getAllByText("价格单已识别到可对比的车型 / 版型和价格字段，可以先创建可编辑配置列，再加入 FloatingDeck 对比。").length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "创建可编辑配置列" }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith(
        "source-price-list",
        "price-list-competitor-price-list-1",
      );
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-price-basic", "draft-price-premium"], false, "latest");
    });
    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 2/2 配置行")).toBeTruthy();
    expect(screen.getAllByText("MSRP").length).toBeGreaterThan(0);
    expect(screen.getAllByText("28000").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    fireEvent.click(within(screen.getByLabelText("在线编辑控制")).getByRole("button", { name: "开启在线编辑" }));
    const editableCells = await waitFor(() => {
      const cells = container.querySelectorAll<HTMLElement>(".compare-cell--editable");
      expect(cells.length).toBeGreaterThanOrEqual(4);
      return cells;
    });
    fireEvent.click(editableCells[1]);
    const priceInput = await screen.findByLabelText("OMODA / T19C / Premium MSRP 配置值，修改后 1.2 秒自动保存");
    fireEvent.change(priceInput, { target: { value: "28500" } });
    fireEvent.click(container.querySelector<HTMLButtonElement>(".compare-cell-edit-save") as HTMLButtonElement);

    await waitFor(() => {
      expect(api.updateEngineeringConfigFeatureValue).toHaveBeenCalledWith("value-price-premium-msrp", {
        raw_value: "28500",
        expected_version: 1,
        comment: "配置核对更正",
      });
    });
    await waitFor(() => {
      expect(editableCells[1].textContent).toContain("28500");
    });

    await exportCurrentRangeFromFloatingDeck("xlsx");
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const xlsxPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(xlsxPayload).toEqual(expect.objectContaining({
      trimIds: ["draft-price-basic", "draft-price-premium"],
      baseTrimId: "draft-price-basic",
      versionScope: "latest",
    }));
    expect(xlsxPayload).not.toHaveProperty("rows");

    await exportCurrentRangeFromFloatingDeck("pdf");
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const pdfPayload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0][0];
    expect(pdfPayload).toEqual(xlsxPayload);
  });

  it("creates editable compare data from an image OCR source digest and switches to formal compare", async () => {
    const imageGroup: EngineeringConfigSourceDigestGroup = {
      ...digest.compareGroups[0],
      groupId: "image-ocr-source-model",
      title: "Rival Image Model",
      sourceSheet: "OCR Image 1",
      modelName: "Rival Image Model",
      trims: digest.compareGroups[0].trims.map((trim) => ({
        ...trim,
        trimId: `image-${trim.trimId}`,
        fullTrimName: trim.fullTrimName.replace("两驱", "Image "),
        trimName: trim.trimName.replace("两驱", "Image "),
        modelName: "Rival Image Model",
        materialNo: null,
        hasMaterialNo: false,
        dataOrigin: "external_or_scraped",
        sourceSheet: "OCR Image 1",
      })),
      rows: digest.compareGroups[0].rows.map((row, index) => ({
        ...row,
        ...(index === 0
          ? {
              category: "Safety",
              featureCode: "image_blind_spot",
              featureName: "Image OCR Blind spot",
              reviewFlags: ["ocr_possible_feature_text_in_value_cell"],
              reviewNotes: ["OCR 值单元格像配置项文本（spot），可能是特征名换行或单位被切入值列。"],
            }
          : {}),
        values: row.values.map((value) =>
          value
            ? {
                ...value,
                source: {
                  sheetName: "OCR Image 1",
                  rowNumber: value.source?.rowNumber ?? 18,
                  columnNumber: value.source?.columnNumber ?? 4,
                  columnLetter: value.source?.columnLetter ?? "D",
                  cell: value.source?.cell ?? "D18",
                  sourceCell: value.source?.sourceCell ?? value.source?.cell ?? "D18",
                  mergedRange: value.source?.mergedRange ?? null,
                  sourceType: "image_ocr",
                  pageNumber: 1,
                  ocrEngine: "paddleocr",
                },
              }
            : null,
        ),
      })),
    };
    const imageDigest: EngineeringConfigSourceDigest = {
      ...digest,
      digestType: "image_ocr",
      sourceFormat: "image_ocr",
      ocrEngine: "paddleocr",
      fileName: "rival-config.jpg",
      modelName: "Rival Image Model",
      ocrEvaluation: {
        strategy: "highest_config_semantic_score",
        reason: "highest_config_semantic_score",
        candidateCount: 2,
        comparableCandidateCount: 1,
        selectedCandidateCount: 1,
        selectedEngine: "paddleocr",
        selectedEngines: ["paddleocr"],
        selectedScore: {
          semanticScore: 1,
          comparableGroupCount: 1,
          featureCount: 1,
          differenceCount: 1,
          candidateTrimCount: 2,
          totalFeatureCount: 1,
          totalDifferenceCount: 1,
          totalCandidateTrimCount: 2,
          tableShapeScore: 16,
          rowCount: 4,
          columnCount: 4,
          nonEmptyCount: 12,
        },
        selectedSheetName: "OCR Image 1",
        selectedPageNumber: 1,
      },
      ocrEngineCandidates: [
        {
          engine: "legacy_ocr",
          sourceType: "image_ocr",
          sheetName: "OCR Image 1",
          selected: false,
          comparableTableDetected: false,
          score: {
            semanticScore: 0,
            comparableGroupCount: 0,
            featureCount: 0,
            differenceCount: 0,
            candidateTrimCount: 0,
            totalFeatureCount: 0,
            totalDifferenceCount: 0,
            totalCandidateTrimCount: 0,
            tableShapeScore: 0,
            rowCount: 0,
            columnCount: 0,
            nonEmptyCount: 0,
          },
          message: "legacy_ocr OCR text did not contain comparable table rows.",
        },
        {
          engine: "paddleocr",
          sourceType: "image_ocr",
          sheetName: "OCR Image 1",
          selected: true,
          comparableTableDetected: true,
          score: {
            semanticScore: 1,
            comparableGroupCount: 1,
            featureCount: 1,
            differenceCount: 1,
            candidateTrimCount: 2,
            totalFeatureCount: 1,
            totalDifferenceCount: 1,
            totalCandidateTrimCount: 2,
            tableShapeScore: 16,
            rowCount: 4,
            columnCount: 4,
            nonEmptyCount: 12,
          },
        },
      ],
      summary: {
        ...digest.summary,
        comparableGroupCount: 1,
        candidateTrimCount: 2,
      },
      compareGroups: [imageGroup],
    };
    vi.mocked(api.completeEngineeringConfigSourceUpload).mockResolvedValueOnce({
      ...buildSourceSnapshotFixture("source-image-ocr", "rival-config.jpg", imageDigest),
      fileType: "image",
      mimeType: "image/jpeg",
      parseMode: "stored_source",
      message: "Source snapshot registered.",
      sourceDigestStatus: {
        digestType: "image_ocr",
        status: "ready",
        sourceFormat: "image_ocr",
        ocrEngine: "paddleocr",
        ocrEvaluation: imageDigest.ocrEvaluation ?? null,
        summary: {
          candidateTrimCount: 2,
          comparableGroupCount: 1,
          featureCount: 3,
          differenceCount: 2,
        },
      },
    });
    vi.mocked(api.createEngineeringConfigDraftFromSourceDigest).mockResolvedValueOnce({
      sourceId: "source-image-ocr",
      groupId: "image-ocr-source-model",
      importBatchId: "draft-image-ocr",
      sourceFileName: "rival-config.jpg",
      groupTitle: "Rival Image Model",
      sourceDigestType: "image_ocr",
      sourceFormat: "image_ocr",
      sourceKind: "config_matrix",
      ocrEngine: "paddleocr",
      ocrEvaluation: imageDigest.ocrEvaluation ?? null,
      trimIds: ["draft-image-basic", "draft-image-premium"],
      compareTrimIds: ["draft-image-basic", "draft-image-premium"],
      trimCount: 2,
      createdTrimCount: 2,
      reusedTrimCount: 0,
      featureCount: 1,
      createdFeatureCount: 1,
      reusedFeatureCount: 0,
      valueRecordCount: 2,
      insertedValueCount: 2,
      updatedValueCount: 0,
      createdVersionIds: ["version-image-basic", "version-image-premium"],
    });
    const formalImageCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-image-basic",
          fullTrimName: "Image Basic",
          brand: "Rival",
          modelName: "Rival Image Model",
          trimName: "Basic",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Basic",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-image-premium",
          fullTrimName: "Image Premium",
          brand: "Rival",
          modelName: "Rival Image Model",
          trimName: "Premium",
          market: "EU",
          modelYear: "2026",
          materialNo: null,
          salesVersion: "Premium",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 0,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Safety"],
      },
      rows: [
        {
          category: "Safety",
          featureId: "feature-image-blind",
          featureCode: "image_blind_spot",
          featureName: "Image OCR Blind spot",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["draft-image-premium"],
          businessNote: "图片 OCR 来源配置差异",
          values: [
            {
              valueId: "value-image-basic-blind",
              rawValue: "-",
              normalizedValue: "not_available",
              availability: "NOT_AVAILABLE",
              unit: null,
              displayValue: "不配备",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "OCR Image 1",
                rowNumber: 18,
                columnNumber: 4,
                columnLetter: "D",
                cell: "D18",
                sourceCell: "D18",
                mergedRange: null,
                sourceType: "image_ocr",
                pageNumber: 1,
                ocrEngine: "paddleocr",
              },
            },
            {
              valueId: "value-image-premium-blind",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: {
                sheetName: "OCR Image 1",
                rowNumber: 18,
                columnNumber: 5,
                columnLetter: "E",
                cell: "E18",
                sourceCell: "E18",
                mergedRange: null,
                sourceType: "image_ocr",
                pageNumber: 1,
                ocrEngine: "paddleocr",
              },
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalImageCompare) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    closePremiumSampleFromSelectedPanel();
    await waitFor(() => {
      expect(screen.getByText("请选择至少 2 个配置列开始配置对比。")).toBeTruthy();
    });
    const emptyState = container.querySelector(".product-config-empty");
    fireEvent.click(within(emptyState as HTMLElement).getByRole("button", { name: "上传配置表 / 价格单" }));
    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();

    const fileInput = container.querySelector<HTMLInputElement>("input[type='file']");
    const file = new File([new Uint8Array([255, 216, 255, 224, 0, 16, 74, 70, 73, 70])], "rival-config.jpg", {
      type: "image/jpeg",
    });
    fireEvent.change(fileInput as HTMLInputElement, { target: { files: [file] } });
    fireEvent.click(screen.getByRole("button", { name: "上传并生成 Source Digest" }));

    expect(await screen.findByText("图片 OCR digest")).toBeTruthy();
    expect(screen.getAllByText("OCR 选择 paddleocr").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByLabelText("来源上传下一步").textContent).toContain("OCR 透明度：OCR 已比较 2 个候选");
    expect(screen.getByLabelText("来源上传下一步").textContent).toContain("采用 paddleocr");
    expect(screen.getByText("OCR Image 1 · 未识别可比表 · 非空 0 · 语义分 0")).toBeTruthy();
    expect(screen.getByText("OCR Image 1 · 识别到可比表 · 配置项 1 · 差异 1 · 配置列 2 · 候选组 1 · 表格 4 x 4 · 非空 12 · 语义分 1")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "建列后定位此行" }));
    expect(screen.getByRole("button", { name: "已设为建列后定位" }).getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(screen.getByRole("button", { name: "创建可编辑配置列" }));

    await waitFor(() => {
      expect(api.createEngineeringConfigDraftFromSourceDigest).toHaveBeenCalledWith(
        "source-image-ocr",
        "image-ocr-source-model",
      );
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["draft-image-basic", "draft-image-premium"], false, "latest");
    });
    expect(screen.getAllByText("Image Premium").length).toBeGreaterThan(0);
    await waitFor(() => {
      const focusedRow = document.getElementById("config-feature-image-blind-spot");
      expect(focusedRow).toBeTruthy();
      expect(focusedRow?.classList.contains("compare-row-active")).toBe(true);
      expect(focusedRow?.getAttribute("aria-selected")).toBe("true");
    });
    expect(window.HTMLElement.prototype.scrollIntoView).toHaveBeenCalled();
    expect(screen.getByText(/定位到需核对行：Image OCR Blind spot/)).toBeTruthy();
  });

  it("saves an edited formal compare cell and refreshes the cell display", async () => {
    const formalCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-basic",
          fullTrimName: "Draft Basic",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Basic",
          market: "EU",
          modelYear: "2026",
          materialNo: "MM001",
          salesVersion: "Basic",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-premium",
          fullTrimName: "Draft Premium",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Premium",
          market: "EU",
          modelYear: "2026",
          materialNo: "MM002",
          salesVersion: "Premium",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 1,
        differentValueCount: 0,
        uniqueFeatureCount: 0,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        confirmedDifferenceCount: 0,
        rawConfirmedDifferenceCount: 0,
        inferredDifferenceCount: 0,
        differenceCount: 0,
        differenceCategories: [],
      },
      rows: [
        {
          category: "Comfort",
          featureId: "feature-seat-heat",
          featureCode: "seat_heat",
          featureName: "Seat heating",
          comparisonType: "COMMON_SAME",
          uniqueTrimIds: [],
          businessNote: "共同配置",
          values: [
            {
              valueId: "value-basic-seat",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: null,
            },
            {
              valueId: "value-premium-seat",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: null,
            },
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalCompare) as unknown as Record<string, unknown>);
    vi.mocked(api.updateEngineeringConfigFeatureValue).mockResolvedValueOnce({
      valueId: "value-basic-seat",
      rawValue: "O",
      normalizedValue: "optional",
      availability: "OPTIONAL",
      displayValue: "选装",
      valueState: "marker_value",
      version: 2,
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=draft-basic,draft-premium&baseTrimId=draft-basic&versionScope=latest"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 1/1 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    fireEvent.click(within(screen.getByLabelText("在线编辑控制")).getByRole("button", { name: "开启在线编辑" }));

    const editableCells = await waitFor(() => {
      const cells = container.querySelectorAll<HTMLElement>(".compare-cell--editable");
      expect(cells.length).toBeGreaterThan(0);
      return cells;
    });
    fireEvent.click(editableCells[0]);
    const input = await screen.findByLabelText("Draft Basic Seat heating 配置值，修改后 1.2 秒自动保存");
    fireEvent.change(input, { target: { value: "O" } });
    fireEvent.click(container.querySelector<HTMLButtonElement>(".compare-cell-edit-save") as HTMLButtonElement);

    await waitFor(() => {
      expect(api.updateEngineeringConfigFeatureValue).toHaveBeenCalledWith("value-basic-seat", {
        raw_value: "O",
        expected_version: 1,
        comment: "配置核对更正",
      });
    });
    await waitFor(() => {
      expect(editableCells[0].textContent).toContain("选装");
    });
    await exportCurrentRangeFromFloatingDeck("xlsx");
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const exportPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(exportPayload).toEqual(expect.objectContaining({
      trimIds: ["draft-basic", "draft-premium"],
      baseTrimId: "draft-basic",
      versionScope: "latest",
    }));
    expect(exportPayload).not.toHaveProperty("rows");
    await exportCurrentRangeFromFloatingDeck("pdf");
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const pdfExportPayload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0][0];
    expect(pdfExportPayload).toEqual(exportPayload);
  });

  it("creates a missing formal compare cell from the page edit deck and exports the created value", async () => {
    const formalCompare: CompareResponse = {
      trims: [
        {
          trimId: "draft-basic",
          fullTrimName: "Draft Basic",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Basic",
          market: "EU",
          modelYear: "2026",
          materialNo: "MM001",
          salesVersion: "Basic",
          msrp: null,
          targetPrice: null,
        },
        {
          trimId: "draft-premium",
          fullTrimName: "Draft Premium",
          brand: "OMODA",
          modelName: "T19C",
          trimName: "Premium",
          market: "EU",
          modelYear: "2026",
          materialNo: "MM002",
          salesVersion: "Premium",
          msrp: null,
          targetPrice: null,
        },
      ],
      summary: {
        totalFeatures: 1,
        shownFeatures: 1,
        commonSameCount: 0,
        differentValueCount: 0,
        uniqueFeatureCount: 1,
        partialAvailableCount: 0,
        missingOrUnknownCount: 1,
        confirmedDifferenceCount: 1,
        rawConfirmedDifferenceCount: 1,
        inferredDifferenceCount: 0,
        differenceCount: 1,
        differenceCategories: ["Comfort"],
      },
      rows: [
        {
          category: "Comfort",
          featureId: "feature-seat-heat",
          featureCode: "seat_heat",
          featureName: "Seat heating",
          comparisonType: "UNIQUE_TO_TRIM",
          uniqueTrimIds: ["draft-basic"],
          businessNote: "Premium 缺少原始配置值",
          values: [
            {
              valueId: "value-basic-seat",
              rawValue: "●",
              normalizedValue: "standard",
              availability: "STANDARD",
              unit: null,
              displayValue: "标配",
              valueState: "marker_value",
              version: 1,
              inferred: false,
              source: null,
            },
            null,
          ],
        },
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalCompare) as unknown as Record<string, unknown>);
    vi.mocked(api.createEngineeringConfigFeatureValue).mockResolvedValueOnce({
      valueId: "value-premium-seat-created",
      rawValue: "O",
      normalizedValue: "optional",
      availability: "OPTIONAL",
      displayValue: "选装",
      valueState: "marker_value",
      version: 1,
    });

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=draft-basic,draft-premium&baseTrimId=draft-basic&versionScope=latest"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 1/1 配置行")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    fireEvent.click(within(screen.getByLabelText("在线编辑控制")).getByRole("button", { name: "开启在线编辑" }));

    const editableCells = await waitFor(() => {
      const cells = container.querySelectorAll<HTMLElement>(".compare-cell--editable");
      expect(cells.length).toBe(2);
      return cells;
    });
    fireEvent.click(editableCells[1]);
    const input = await screen.findByLabelText("Draft Premium Seat heating 配置值，修改后 1.2 秒自动保存");
    fireEvent.change(input, { target: { value: "O" } });
    fireEvent.click(container.querySelector<HTMLButtonElement>(".compare-cell-edit-save") as HTMLButtonElement);

    await waitFor(() => {
      expect(api.createEngineeringConfigFeatureValue).toHaveBeenCalledWith({
        trim_id: "draft-premium",
        feature_id: "feature-seat-heat",
        raw_value: "O",
      });
    });
    expect(api.updateEngineeringConfigFeatureValue).not.toHaveBeenCalled();
    await waitFor(() => {
      expect(editableCells[1].textContent).toContain("选装");
    });
    await exportCurrentRangeFromFloatingDeck("xlsx");
    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const exportPayload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0][0];
    expect(exportPayload).toEqual(expect.objectContaining({
      trimIds: ["draft-basic", "draft-premium"],
      baseTrimId: "draft-basic",
      versionScope: "latest",
    }));
    expect(exportPayload).not.toHaveProperty("rows");
    await exportCurrentRangeFromFloatingDeck("pdf");
    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const pdfExportPayload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0][0];
    expect(pdfExportPayload).toEqual(exportPayload);
  });

  it("keeps a source upload action visible when the local digest sample fails", async () => {
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockRejectedValueOnce(new Error("Failed to fetch"));
    const { container } = render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: "查看本地 xlsx 样例" }));

    expect(await screen.findByText("来源样例暂不可用")).toBeTruthy();
    expect(screen.getByText("本地 xlsx 样例暂不可用：Failed to fetch")).toBeTruthy();

    const digestEmpty = container.querySelector(".product-config-local-digest--empty");
    expect(digestEmpty).toBeTruthy();
    fireEvent.click(within(digestEmpty as HTMLElement).getByRole("button", { name: "上传配置表 / 价格单" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    expect(screen.getByText("拖放配置表或来源文件")).toBeTruthy();
  });

  it("labels and scopes the page clearly in difference-only mode", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    const hero = document.querySelector(".product-config-hero") as HTMLElement;
    expect(screen.getByLabelText("顶部当前表格范围：全部配置行")).toBeTruthy();
    expect(hero.textContent).toContain("范围 全部配置行");
    expect(hero.textContent).not.toContain("当前展示 3/3 配置行");

    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
    fireEvent.click(tableDeltaFilterButton(/差异行 2/));

    await waitFor(() => {
      expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    });
    expect(screen.getByLabelText("顶部当前表格范围：差异行")).toBeTruthy();
    expect(hero.textContent).toContain("范围 差异行");
    expect(hero.textContent).not.toContain("当前展示 2/3 差异行");
    await screen.findByLabelText("差异类型筛选");
    expect(screen.queryByText("差异行 Excel 对比导读")).toBeNull();
    expect(tableDeltaFilterButton(/差异行 2/).classList.contains("is-active")).toBe(true);
    expect(screen.getByRole("button", { name: "顶部恢复全部配置行" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: /Infotainment/ })).toBeNull();

    selectSimpleTableCategory("Wheel");

    await waitFor(() => {
      const status = screen.getByLabelText("配置表范围状态");
      expect(status.textContent).toContain("当前展示 1/3 差异行");
      expect(status.textContent).toContain("大类 Wheel");
    });
    expect(tableRangeMetricText("当前展示行")).toContain("1差异行");

    fireEvent.click(screen.getByRole("button", { name: "顶部恢复全部配置行" }));

    await waitFor(() => {
      expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    });
    expect(tableDeltaFilterButton(/全部配置行 3/).classList.contains("is-active")).toBe(true);
    expect((screen.getByRole("combobox", { name: "配置大类" }) as HTMLInputElement).value).toBe("");
    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
  });

  it("keeps the default table complete and lets the simple controls focus differences", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.getByLabelText("顶部当前表格范围：全部配置行")).toBeTruthy();
    const tableScopeStatus = await screen.findByLabelText("配置表范围状态");
    expect(tableScopeStatus.textContent).toContain("当前展示 3/3 行");
    expect(tableScopeStatus.textContent).toContain("未隐藏共同项");
    expect(tableRangeMetricText("当前展示行")).toContain("3配置行");
    expect(screen.getByText("Speaker")).toBeTruthy();

    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: "定位首个差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: "上一个差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: "下一个差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: "复制选中行" })).toBeNull();
    fireEvent.click(tableDeltaFilterButton(/差异行 2/));

    await waitFor(() => {
      expect(tableRangeMetricText("当前展示行")).toContain("2差异行");
    });
    expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    expect(screen.getByLabelText("配置表范围状态").textContent).toContain("当前展示 2/3 行");
    expect(tableDeltaFilterButton(/差异行 2/).classList.contains("is-active")).toBe(true);
    expect(screen.queryByRole("button", { name: /Infotainment/ })).toBeNull();
    expect(screen.queryByText("Speaker")).toBeNull();
    expect(screen.getAllByText("Blind spot").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Wheel size").length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: "定位首个差异行" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "上一个差异行" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "下一个差异行" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "复制选中行" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "定位首个差异行" }));

    expect(screen.getByText("差异行 1/2")).toBeTruthy();
    expect(screen.getByRole("button", { name: "复制选中行" })).toBeTruthy();
  });

  it("keeps the business summary aligned with the selected delta type", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");

    fireEvent.click(tableDeltaFilterButton(/新增配置 1/));

    await waitFor(() => {
      expect(screen.getByText("当前表格 1 项差异")).toBeTruthy();
    });
    expect(screen.getByText("新增配置 差异摘要")).toBeTruthy();
    expect(screen.getByText(/Basic 作为基准列，当前聚焦 新增配置，当前对比 1 个目标配置列，累计 1 个目标差异/)).toBeTruthy();
    expect(screen.getByText(/Premium 相比 Basic 在 新增配置：新增 1 项/)).toBeTruthy();
    expect(screen.queryByText("Wheel size")).toBeNull();
    expect(screen.queryByText("Speaker")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "查看当前范围差异" }));

    expect(tableDeltaFilterButton(/新增配置 1/).classList.contains("is-active")).toBe(true);
    expect(screen.getByText("当前表格 1 项差异")).toBeTruthy();
  });

  it("focuses table rows from a business insight chip", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");

    fireEvent.click(await screen.findByRole("button", { name: "聚焦 Premium 的 增配重点" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 1 项差异")).toBeTruthy();
    });
    expect(screen.getByText("新增配置 · 目标 Premium 差异摘要")).toBeTruthy();
    expect(tableDeltaFilterButton(/新增配置 1/).classList.contains("is-active")).toBe(true);
    expect(screen.getAllByText("Blind spot").length).toBeGreaterThan(0);
    expect(screen.queryByText("Wheel size")).toBeNull();
    expect(screen.queryByText("Speaker")).toBeNull();
  });

  it("keeps the common-config scope when the business summary action is clicked", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");

    fireEvent.click(tableDeltaFilterButton(/共同配置 1/));

    await waitFor(() => {
      expect(screen.getByText("当前表格 1 项配置")).toBeTruthy();
    });
    expect(screen.getByText("共同配置 摘要")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "共同配置1当前范围为一致配置，差异项 0 项")).toBeTruthy();
    expect(screen.getByText(/Basic 作为基准列，当前聚焦 共同配置，当前对比 1 个目标配置列，当前范围包含 1 行共同配置/)).toBeTruthy();
    expect(screen.getByText(/Premium 与 Basic 在 共同配置：共同配置 1 项/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "查看当前范围配置" }));

    expect(tableDeltaFilterButton(/共同配置 1/).classList.contains("is-active")).toBe(true);
    expect(screen.getByText("当前表格 1 项配置")).toBeTruthy();
  });

  it("switches from common config to differences when the summary focuses a target trim", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    fireEvent.click(tableDeltaFilterButton(/共同配置 1/));

    await waitFor(() => {
      expect(screen.getByText("共同配置 摘要")).toBeTruthy();
    });

    switchSummaryMode("expert");
    fireEvent.click(await screen.findByRole("button", { name: "查看 Premium 相对基准的差异" }));

    await waitFor(() => {
      expect(screen.getByText("当前目标 Premium · 2 项差异")).toBeTruthy();
    });
    expect(tableDeltaFilterButton(/差异项 2/).classList.contains("is-active")).toBe(true);
    expect(screen.getAllByText("Blind spot").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Wheel size").length).toBeGreaterThan(0);
    expect(screen.queryByText("Speaker")).toBeNull();
  });

  it("keeps summary metrics aligned with table search", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    openSimpleTableControls();
    const searchInput = screen.getByRole("combobox", { name: "搜索配置" });
    fireEvent.focus(searchInput);
    fireEvent.change(searchInput, {
      target: { value: "20 inch" },
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByText("20 inch"));

    await waitFor(() => {
      expectTableRangeStatusParts(["搜索 20 inch", "当前展示 1/3 配置行"]);
    });
    expect(tableRangeMetricText("当前展示行")).toContain("1配置行");
    expect(screen.queryByText("搜索：20 inch Excel 对比导读")).toBeNull();
    expect(screen.queryByLabelText("当前配置分析口径")).toBeNull();
    expect(screen.queryByText("Speaker")).toBeNull();
    expect((screen.getByRole("combobox", { name: "配置大类" }) as HTMLInputElement).value).toBe("");
    expect(screen.queryByRole("button", { name: /Wheel，当前范围 1 配置行/ })).toBeNull();
    expect(screen.getByRole("button", { name: "顶部恢复全部配置行" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "顶部恢复全部配置行" }));

    await waitFor(() => {
      expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    });
    expect((screen.getByRole("combobox", { name: "搜索配置" }) as HTMLInputElement).value).toBe("");
    expect(tableRangeMetricText("当前展示行")).toContain("3配置行");
    expect(screen.queryByText("Excel 配置对比导读")).toBeNull();
    expect(screen.getByText("Speaker")).toBeTruthy();
    expect(screen.getByText("Blind spot")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "清空 搜索配置" })).toBeNull();
    expect(screen.queryByRole("button", { name: "顶部查看差异行" })).toBeNull();
  });

  it("labels inferred filtering as a difference scope", async () => {
    const inferredDigest: EngineeringConfigSourceDigest = {
      ...digest,
      compareGroups: [
        {
          ...digest.compareGroups[0],
          summary: {
            ...digest.compareGroups[0].summary,
            inferredDifferenceCount: 1,
          },
          rows: digest.compareGroups[0].rows.map((row) => {
            if (row.featureCode !== "blind_spot") return row;
            const basicValue = row.values[0];
            const premiumValue = row.values[1];
            if (!basicValue || !premiumValue) return row;
            return {
              ...row,
              values: [
                {
                  ...basicValue,
                  displayValue: "不配备*",
                  inferred: true,
                  inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
                  confidence: 0.7,
                },
                premiumValue,
              ],
            };
          }),
        },
      ],
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(inferredDigest);

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "查看规则推断差异：推断差异 1" })).toBeNull();
    switchSummaryMode("expert");
    expect(await screen.findByRole("button", { name: "查看规则推断差异：推断差异 1" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "查看规则推断差异：推断差异 1" }));

    await waitFor(() => {
      expect(screen.getByText("当前表格 1 项差异")).toBeTruthy();
    });
    expect(screen.getByText("规则推断 摘要")).toBeTruthy();
    expect(screen.getByText("当前只统计规则推断差异，推断值需回看来源证据。")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "规则推断1当前范围差异项 1 项")).toBeTruthy();
    expect(screen.getByRole("button", { name: "查看规则推断项" })).toBeTruthy();
    expect(screen.getAllByText("Blind spot").length).toBeGreaterThan(0);
    expect(screen.queryByText("Speaker")).toBeNull();
  });

  it("returns to all config rows when simple mode leaves an expert-only evidence scope", async () => {
    const inferredDigest: EngineeringConfigSourceDigest = {
      ...digest,
      compareGroups: [
        {
          ...digest.compareGroups[0],
          summary: {
            ...digest.compareGroups[0].summary,
            inferredDifferenceCount: 1,
          },
          rows: digest.compareGroups[0].rows.map((row) => {
            if (row.featureCode !== "blind_spot") return row;
            const basicValue = row.values[0];
            const premiumValue = row.values[1];
            if (!basicValue || !premiumValue) return row;
            return {
              ...row,
              values: [
                {
                  ...basicValue,
                  displayValue: "不配备*",
                  inferred: true,
                  inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
                  confidence: 0.7,
                },
                premiumValue,
              ],
            };
          }),
        },
      ],
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(inferredDigest);

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    switchSummaryMode("expert");
    fireEvent.click(await screen.findByRole("button", { name: "查看规则推断差异：推断差异 1" }));

    await waitFor(() => {
      expect(screen.getByText("当前表格 1 项差异")).toBeTruthy();
    });
    expect(screen.queryByText("Speaker")).toBeNull();

    switchSummaryMode("simple");

    await waitFor(() => {
      expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    });
    expect(tableRangeMetricText("当前展示行")).toContain("3配置行");
    expect(screen.getByText("Speaker")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /显示范围：规则推断行/ })).toBeNull();
    expect(screen.getByRole("button", { name: "显示范围：全部配置行 3" }).getAttribute("aria-pressed")).toBe("true");
  });

  it("labels pending scope with pending metrics instead of confirmed differences", async () => {
    const pendingDigest: EngineeringConfigSourceDigest = {
      ...digest,
      summary: {
        ...digest.summary,
        featureCount: 4,
        differenceCount: 3,
      },
      compareGroups: [
        {
          ...digest.compareGroups[0],
          featureCount: 4,
          differenceCount: 3,
          summary: {
            ...digest.compareGroups[0].summary,
            totalFeatures: 4,
            shownFeatures: 4,
            missingOrUnknownCount: 1,
            differenceCount: 3,
          },
          rows: [
            ...digest.compareGroups[0].rows,
            {
              category: "Data quality",
              featureCode: "source_pending",
              featureName: "Pending source config",
              comparisonType: "MISSING_OR_UNKNOWN",
              uniqueTrimIds: [],
              businessNote: "来源缺失，需要回看原表。",
              values: [
                null,
                {
                  valueId: "premium-source-pending",
                  rawValue: "",
                  normalizedValue: null,
                  availability: "UNKNOWN",
                  unit: null,
                  displayValue: "待确认",
                },
              ],
            },
          ],
        },
      ],
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(pendingDigest);

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 4/4 配置行")).toBeTruthy();

    fireEvent.click(tableDeltaFilterButton(/待确认 1/));

    await waitFor(() => {
      expect(screen.getByText("当前表格 1 项待确认")).toBeTruthy();
    });
    expect(screen.getByText("待确认 摘要")).toBeTruthy();
    expect(screen.getByText(/Basic 作为基准列，当前聚焦 待确认，当前对比 1 个目标配置列，当前范围包含 1 个待确认项，涉及 1 行配置/)).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "待确认1空值 / 缺失需回看来源，不直接等于无配置")).toBeTruthy();
    expect(screen.getByRole("button", { name: "查看待确认项" })).toBeTruthy();
    expect(screen.getAllByText("Pending source config").length).toBeGreaterThan(0);
    expect(screen.queryByText((_content, element) => element?.textContent === "差异项0当前范围含规则推断 0 项")).toBeNull();
  });

  it("uses base-trim deltas for summary metrics when row comparison type is stale", async () => {
    const staleComparisonDigest: EngineeringConfigSourceDigest = {
      ...digest,
      summary: {
        ...digest.summary,
        featureCount: 4,
        differenceCount: 3,
      },
      compareGroups: [
        {
          ...digest.compareGroups[0],
          featureCount: 4,
          differenceCount: 3,
          summary: {
            ...digest.compareGroups[0].summary,
            totalFeatures: 4,
            shownFeatures: 4,
            commonSameCount: 2,
            confirmedDifferenceCount: 2,
            differenceCount: 2,
          },
          rows: [
            ...digest.compareGroups[0].rows,
            {
              category: "Exterior",
              featureCode: "panoramic_roof",
              featureName: "Panoramic roof",
              comparisonType: "COMMON_SAME",
              uniqueTrimIds: [],
              businessNote: "旧摘要字段误标为共同配置",
              values: [
                {
                  valueId: "basic-roof",
                  rawValue: "",
                  normalizedValue: null,
                  availability: "NOT_AVAILABLE",
                  unit: null,
                  displayValue: "不配备",
                },
                {
                  valueId: "premium-roof",
                  rawValue: "●",
                  normalizedValue: "standard",
                  availability: "STANDARD",
                  unit: null,
                  displayValue: "标配",
                },
              ],
            },
          ],
        },
      ],
    };
    vi.mocked(api.getEngineeringConfigLocalWorkbookDigest).mockResolvedValueOnce(staleComparisonDigest);

    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 4/4 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    expect(screen.getByText((_content, element) => element?.textContent === "差异项3当前范围含规则推断 0 项")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "可用性差异2共同配置 1，值不同 1")).toBeTruthy();

    fireEvent.click(tableDeltaFilterButton(/新增配置 2/));

    await waitFor(() => {
      expect(screen.getByText("当前表格 2 项差异")).toBeTruthy();
    });
    expect(screen.getByText((_content, element) => element?.textContent === "新增配置2当前范围含规则推断 0 项")).toBeTruthy();
    expect(screen.getAllByText("Panoramic roof").length).toBeGreaterThan(0);
    expect(screen.queryByText("Wheel size")).toBeNull();
  });

  it("restores the full config scope from a searched difference view", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();

    fireEvent.click(tableDeltaFilterButton(/差异行 2/));
    await waitFor(() => {
      expect(screen.getAllByText("当前展示 2/3 差异行").length).toBeGreaterThan(0);
    });

    const differenceSearchInput = screen.getByRole("combobox", { name: "搜索配置" });
    fireEvent.focus(differenceSearchInput);
    fireEvent.change(differenceSearchInput, {
      target: { value: "wheel" },
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: /Wheel.*配置大类/ }));

    await waitFor(() => {
      expectTableRangeStatusParts(["搜索 Wheel", "当前展示 1/3 差异行"]);
    });
    expect(screen.queryByText("差异行 · 搜索：Wheel Excel 对比导读")).toBeNull();
    expect(screen.queryByText("Speaker")).toBeNull();

    fireEvent.click(within(screen.getByLabelText("当前表格口径")).getByRole("button", { name: "恢复全部配置行" }));

    await waitFor(() => {
      expect(screen.getByText("当前展示 3/3 配置行")).toBeTruthy();
    });
    expect((screen.getByRole("combobox", { name: "搜索配置" }) as HTMLInputElement).value).toBe("");
    expect((screen.getByRole("combobox", { name: "配置大类" }) as HTMLInputElement).value).toBe("");
    expect(screen.queryByText("Excel 配置对比导读")).toBeNull();
    expect(screen.getByText("Speaker")).toBeTruthy();
    expect(screen.getByText("Blind spot")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "恢复全部配置行" })).toBeNull();
  });

  it("keeps formal compare requests full while using table filters for difference scope", async () => {
    render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(api.compareEngineeringConfigTrims).toHaveBeenCalledWith(["basic", "premium"], false);
    });
    expect(api.getEngineeringConfigLocalWorkbookDigest).not.toHaveBeenCalled();
    const compareCallCount = vi.mocked(api.compareEngineeringConfigTrims).mock.calls.length;

    fireEvent.click(tableDeltaFilterButton(/差异行/));

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "顶部恢复全部配置行" })).toBeTruthy();
    });
    expect(vi.mocked(api.compareEngineeringConfigTrims).mock.calls.length).toBe(compareCallCount);
  });

  it("warns when a formal compare trim has no source anchor", async () => {
    const formalCompare: CompareResponse = {
      trims: [
        {
          trimId: "basic",
          brand: "OMODA",
          modelName: "T19C MY ICE",
          fullTrimName: "Basic",
          trimName: "Basic",
          materialNo: "T71607V**MM0001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: "own-catalog.xlsx",
        },
        {
          trimId: "premium",
          brand: "Competitor",
          modelName: "T19C MY ICE",
          fullTrimName: "Premium",
          trimName: "Premium",
          salesVersion: "Premium",
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
        },
      ],
      rows: digest.compareGroups[0].rows,
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(formalCompare as unknown as Record<string, unknown>);

    render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    openSimpleSelectedStrip();
    expect(await screen.findByText("来源 own-catalog.xlsx")).toBeTruthy();
    expect(screen.getByText("来源 来源待补")).toBeTruthy();

    switchSummaryMode("expert");

    expect(screen.getAllByText("来源待补 1").length).toBeGreaterThan(0);
    expect(screen.getByText("同车型不同网站或来源问题时，配置差异需要优先回看来源证据。")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "补充来源 / 上传 Source Digest" }));

    expect(await screen.findByText("配置表 / 价格单上传（推荐）")).toBeTruthy();
    expect(await screen.findByText("目标配置列 Premium")).toBeTruthy();
    const sourceDigestSearch = screen.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_COMBOBOX_NAME }) as HTMLInputElement;
    expect(sourceDigestSearch.value).toBe("Competitor T19C MY ICE Premium");
    expect(screen.queryByText(/当前只看来源/)).toBeNull();
  });

  it("keeps same-name external trims source-scoped after formal compare loads", async () => {
    const rows = digest.compareGroups[0].rows.map((row) => ({
      ...row,
      values: [
        row.values[0],
        row.values[1] ? { ...row.values[1], valueId: `site-a-${row.values[1].valueId}` } : null,
        row.values[1] ? { ...row.values[1], valueId: `site-b-${row.values[1].valueId}` } : null,
      ],
    }));
    const formalCompare: CompareResponse = {
      trims: [
        {
          trimId: "basic",
          brand: "OMODA",
          modelName: "T19C MY ICE",
          fullTrimName: "Basic",
          trimName: "Basic",
          materialNo: "T71607V**MM0001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          market: "EU",
          modelYear: "2026",
          sourceFileName: "own-catalog.xlsx",
        },
        {
          trimId: "premium-site-a",
          brand: "RivalBrand",
          modelName: "T19C MY ICE",
          fullTrimName: "RivalBrand / T19C MY ICE / Premium / partner-config.xlsx / source-a",
          trimName: "Premium",
          salesVersion: "Premium",
          identityKey: "RivalBrand / T19C MY ICE / Premium / source:site-a",
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          market: "EU",
          modelYear: "2026",
          sourceFileName: "partner-config.xlsx",
          sourceUploadId: "source-a",
          sourceCreatedBy: "alice",
          sourceCreatedAt: "2026-06-01T10:30:00Z",
        },
        {
          trimId: "premium-site-b",
          brand: "RivalBrand",
          modelName: "T19C MY ICE",
          fullTrimName: "RivalBrand / T19C MY ICE / Premium / partner-config.xlsx / source-b",
          trimName: "Premium",
          salesVersion: "Premium",
          identityKey: "RivalBrand / T19C MY ICE / Premium / source:site-b",
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          market: "EU",
          modelYear: "2026",
          sourceFileName: "partner-config.xlsx",
          sourceUploadId: "source-b",
          sourceCreatedBy: "bob",
          sourceCreatedAt: "2026-07-01T09:15:00Z",
        },
      ],
      rows,
      groups: [],
      totalFeatures: rows.length,
      shownFeatures: rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(formalCompare as unknown as Record<string, unknown>);

    render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium-site-a,premium-site-b&baseTrimId=basic"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    const selectedStrip = openSimpleSelectedStrip();
    expect(selectedStrip.textContent).toContain("partner-config.xlsx");

    fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
    const selectedColumns = screen.getByLabelText("当前已选配置列");
    expect(selectedColumns.textContent).toContain("partner-config.xlsx · 快照 source-a");
    expect(selectedColumns.textContent).toContain("partner-config.xlsx · 快照 source-b");
    expect(selectedColumns.textContent).toContain("来源人 alice");
    expect(selectedColumns.textContent).toContain("来源人 bob");
    expect(selectedColumns.textContent).toContain("上传 2026-06-01");
    expect(selectedColumns.textContent).toContain("上传 2026-07-01");
    expect(selectedColumns.querySelectorAll(".product-config-direct-selected-path")).toHaveLength(3);

    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    const targetTrimInput = screen.getByRole("combobox", { name: "目标配置列" });
    fireEvent.focus(targetTrimInput);
    const listbox = screen.getByRole("listbox");
    expect(listbox.textContent).toContain("partner-config.xlsx · 快照 source-a");
    expect(listbox.textContent).toContain("partner-config.xlsx · 快照 source-b");
  });

  it("keeps formal compare cells read-only below editor role", async () => {
    localStorage.setItem("jato_user_role", "viewer");
    const formalCompare: CompareResponse = {
      trims: digest.compareGroups[0].trims.map((trim) => ({ ...trim, brand: "OMODA" })),
      rows: digest.compareGroups[0].rows,
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(formalCompare as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    const readOnlyControlButton = screen.getByRole("button", { name: /添加配置列 \/ 显示/ });
    expect(readOnlyControlButton.textContent).not.toContain("编辑");
    fireEvent.click(readOnlyControlButton);
    expect(screen.queryByLabelText(/在线编辑状态/)).toBeNull();
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    expect(screen.getByText("当前权限只读")).toBeTruthy();
    expect((screen.getByRole("button", { name: "权限只读" }) as HTMLButtonElement).disabled).toBe(true);
    expect(container.querySelectorAll(".compare-cell--editable")).toHaveLength(0);
  });

  it.each(["editor", "admin", "developer"] as const)("allows %s role to enable formal compare cell editing from the floating deck", async (role) => {
    localStorage.setItem("jato_user_role", role);
    const formalCompare: CompareResponse = {
      trims: digest.compareGroups[0].trims.map((trim) => ({ ...trim, brand: "OMODA" })),
      rows: digest.compareGroups[0].rows.map((row, rowIndex) => ({
        ...row,
        featureId: `feature-${rowIndex}`,
        values: row.values.map((value, valueIndex) => value ? {
          ...value,
          valueId: `value-${rowIndex}-${valueIndex}`,
          version: 1,
        } : value),
      })),
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalCompare) as unknown as Record<string, unknown>);

    const { container } = render(
      <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic&versionScope=latest"]}>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    const editableControlButton = screen.getByRole("button", { name: /添加配置列 \/ 显示/ });
    expect(editableControlButton.textContent).not.toContain("编辑");
    expect(screen.queryByLabelText("在线编辑控制")).toBeNull();
    expect(screen.queryByRole("button", { name: "开启在线编辑" })).toBeNull();
    expect(screen.queryByLabelText("配置表在线编辑状态")).toBeNull();
    expect(container.querySelectorAll(".compare-cell--editable")).toHaveLength(0);
    expect(container.querySelectorAll(".compare-cell-edit-marker")).toHaveLength(0);
    fireEvent.click(editableControlButton);
    const editClosedStatus = screen.getByLabelText(/在线编辑状态：编辑关闭/);
    expect(editClosedStatus.textContent).toContain("编辑关闭");
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    const editControl = screen.getByLabelText("在线编辑控制");
    expect(within(editControl).getByText("编辑未开启")).toBeTruthy();
    const editButton = screen.getByRole("button", { name: "开启在线编辑" }) as HTMLButtonElement;
    expect(editButton.disabled).toBe(false);
    fireEvent.click(editButton);

    await waitFor(() => {
      expect(container.querySelectorAll(".compare-cell--editable").length).toBeGreaterThan(0);
    });
    const editStatus = screen.getByLabelText(/在线编辑状态：编辑已开启/);
    expect(editStatus.textContent).toContain("编辑已开启");
    fireEvent.click(screen.getByRole("tab", { name: CONFIG_COLUMN_TAB_NAME }));
    expect(screen.queryByLabelText("在线编辑控制")).toBeNull();
    fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));
    expect(screen.getByLabelText("在线编辑控制")).toBeTruthy();
    expect(container.querySelectorAll(".compare-cell--editable").length).toBeGreaterThan(0);
  });

  it("uses the refreshed auth context role to enable formal compare editing", async () => {
    localStorage.setItem("jato_auth_token", "editor-session-token");
    localStorage.setItem("jato_user_role", "viewer");
    const originalFetch = globalThis.fetch;
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        username: "editor-user",
        role: "editor",
        email: null,
        oauthProvider: null,
        avatarUrl: null,
        displayName: "Editor User",
        primaryCountry: "Germany",
        secondaryCountries: [],
        preferredLandingPage: null,
        profileComplete: true,
      }),
    } as Response);
    Object.defineProperty(globalThis, "fetch", {
      configurable: true,
      writable: true,
      value: fetchMock,
    });
    const formalCompare: CompareResponse = {
      trims: digest.compareGroups[0].trims.map((trim) => ({ ...trim, brand: "OMODA" })),
      rows: digest.compareGroups[0].rows.map((row, rowIndex) => ({
        ...row,
        featureId: `feature-${rowIndex}`,
        values: row.values.map((value, valueIndex) => value ? {
          ...value,
          valueId: `auth-value-${rowIndex}-${valueIndex}`,
          version: 1,
        } : value),
      })),
      groups: [],
      totalFeatures: digest.compareGroups[0].rows.length,
      shownFeatures: digest.compareGroups[0].rows.length,
      summary: digest.compareGroups[0].summary,
    };
    vi.mocked(api.compareEngineeringConfigTrims).mockResolvedValueOnce(latestDraftCompare(formalCompare) as unknown as Record<string, unknown>);

    try {
      const { container } = render(
        <AuthProvider>
          <MemoryRouter initialEntries={["/product/compare/config?trimIds=basic,premium&baseTrimId=basic&versionScope=latest"]}>
            <ProductConfigComparePage />
          </MemoryRouter>
        </AuthProvider>,
      );

      await openLocalSampleIfAvailable();
      expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
      expect(fetchMock).not.toHaveBeenCalled();
      fireEvent.click(screen.getByRole("button", { name: /添加配置列 \/ 显示/ }));
      fireEvent.click(screen.getByRole("tab", { name: DISPLAY_PANEL_TAB_NAME }));

      const editControl = screen.getByLabelText("在线编辑控制");
      await waitFor(() => {
        expect(fetchMock).toHaveBeenCalled();
        expect(within(editControl).getByText("编辑未开启")).toBeTruthy();
      });
      fireEvent.click(within(editControl).getByRole("button", { name: "开启在线编辑" }));

      await waitFor(() => {
        expect(container.querySelectorAll(".compare-cell--editable").length).toBeGreaterThan(0);
      });
    } finally {
      Object.defineProperty(globalThis, "fetch", {
        configurable: true,
        writable: true,
        value: originalFetch,
      });
    }
  });

  it("marks the active base trim and flips the business direction when base changes", async () => {
    render(
      <MemoryRouter>
        <ProductConfigComparePage />
      </MemoryRouter>,
    );

    await openLocalSampleIfAvailable();
    expect(await screen.findByText("当前展示 3/3 配置行")).toBeTruthy();
    switchSummaryMode("expert");
    expect((screen.getByRole("button", { name: "当前基准列 Basic" }) as HTMLButtonElement).disabled).toBe(true);
    expect(await screen.findByText(/Basic 作为基准列，当前对比 1 个目标配置列，累计 2 个目标差异/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "设 Premium 为基准列" }));

    await waitFor(() => {
      expect(screen.getByText("基准：Premium")).toBeTruthy();
    });
    expect((screen.getByRole("button", { name: "当前基准列 Premium" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText(/Premium 作为基准列，当前对比 1 个目标配置列，累计 2 个目标差异/)).toBeTruthy();
    expect(screen.getByText(/Basic 相比 Premium：减少 1 项，值变化 1 项/)).toBeTruthy();
  });
});
