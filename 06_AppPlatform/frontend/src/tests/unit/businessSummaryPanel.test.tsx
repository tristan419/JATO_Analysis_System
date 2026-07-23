// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { BusinessSummaryPanel, clearEngineeringConfigBusinessSummaryCache } from "../../components/BusinessSummaryPanel";
import type { CompareResponse } from "../../types/engineeringConfig";

vi.mock("../../api/client", () => ({
  api: {
    composeEngineeringConfigBusinessSummary: vi.fn(),
  },
}));

const compareData: CompareResponse = {
  trims: [
    {
      trimId: "comfort",
      fullTrimName: "两驱舒适型 Comfort-FWD",
      brand: "OMODA",
      modelName: "T19C MY ICE",
      trimName: "Comfort-FWD",
    },
    {
      trimId: "premium",
      fullTrimName: "两驱尊贵型 Premium-FWD",
      brand: "OMODA",
      modelName: "T19C MY ICE",
      trimName: "Premium-FWD",
    },
  ],
  rows: [
    {
      category: "驾驶辅助 Drive assist",
      featureCode: "camera_360",
      featureName: "360 round view camera / 360度高清全景影像",
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: [],
      businessNote: "部分版本具备",
      values: [
        {
          valueId: "comfort-camera",
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          displayValue: "不配备*",
          inferred: true,
          inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
          confidence: 0.7,
          source: null,
        },
        {
          valueId: "premium-camera",
          rawValue: "●",
          normalizedValue: "standard",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
          inferred: false,
          source: null,
        },
      ],
    },
    {
      category: "信息娱乐 Information&Entertainment",
      featureCode: "speaker_count",
      featureName: "Speaker count / 扬声器数量",
      comparisonType: "DIFFERENT_VALUE",
      uniqueTrimIds: [],
      businessNote: "配置值不同",
      values: [
        {
          valueId: "comfort-speaker",
          rawValue: "6",
          normalizedValue: "6",
          availability: "VALUE",
          unit: null,
          valueState: "numeric_value",
          displayValue: "6",
          inferred: false,
          source: null,
        },
        {
          valueId: "premium-speaker",
          rawValue: "8",
          normalizedValue: "8",
          availability: "VALUE",
          unit: null,
          valueState: "numeric_value",
          displayValue: "8",
          inferred: false,
          source: {
            sheetName: "T19C MY ICE",
            rowNumber: 185,
            columnNumber: 6,
            columnLetter: "F",
            cell: "F185",
            sourceCell: "D185",
            mergedRange: "D185:F185",
          },
        },
      ],
    },
  ],
  groups: [],
  totalFeatures: 2,
  shownFeatures: 2,
};

const compareDataWithCommon: CompareResponse = {
  ...compareData,
  rows: [
    ...compareData.rows,
    {
      category: "基本参数",
      featureCode: "country",
      featureName: "Country / 国家",
      comparisonType: "COMMON_SAME",
      uniqueTrimIds: [],
      businessNote: "共同配置",
      values: [
        {
          valueId: "comfort-country",
          rawValue: "EU",
          normalizedValue: "eu",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "EU",
          inferred: false,
          source: null,
        },
        {
          valueId: "premium-country",
          rawValue: "EU",
          normalizedValue: "eu",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "EU",
          inferred: false,
          source: null,
        },
      ],
    },
  ],
  totalFeatures: 3,
  shownFeatures: 3,
};

const compareDataWithMutuallyExclusiveAudio: CompareResponse = {
  trims: compareData.trims,
  rows: [
    {
      category: "信息娱乐 Information&Entertainment",
      featureCode: "audio_6_speakers",
      featureName: "6 speakers / 6扬声器",
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: ["comfort"],
      businessNote: "Comfort 标配基础音响",
      values: [
        {
          valueId: "comfort-audio-6",
          rawValue: "●",
          normalizedValue: "standard",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
          inferred: false,
          source: null,
        },
        {
          valueId: "premium-audio-6",
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          displayValue: "不配备",
          inferred: false,
          source: null,
        },
      ],
    },
    {
      category: "信息娱乐 Information&Entertainment",
      featureCode: "sony_8_speakers",
      featureName: "SONY 8 speakers / SONY 8扬声器",
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: ["premium"],
      businessNote: "Premium 升级音响",
      values: [
        {
          valueId: "comfort-sony-8",
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          displayValue: "不配备",
          inferred: false,
          source: null,
        },
        {
          valueId: "premium-sony-8",
          rawValue: "●",
          normalizedValue: "standard",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
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

type CompareRow = CompareResponse["rows"][number];
type CompareValue = CompareRow["values"][number];
type CompareTrimId = "comfort" | "premium";

function availabilityValue(featureCode: string, trimId: CompareTrimId, availableTrimId: CompareTrimId): CompareValue {
  const available = trimId === availableTrimId;
  return {
    valueId: `${trimId}-${featureCode}`,
    rawValue: available ? "●" : "",
    normalizedValue: available ? "standard" : null,
    availability: available ? "STANDARD" : "NOT_AVAILABLE",
    unit: null,
    valueState: available ? "marker_value" : "blank",
    displayValue: available ? "标配" : "不配备",
    inferred: false,
    source: null,
  };
}

function swapFeatureRow(featureCode: string, featureName: string, category: string, availableTrimId: CompareTrimId): CompareRow {
  return {
    category,
    featureCode,
    featureName,
    comparisonType: "UNIQUE_OR_PARTIAL",
    uniqueTrimIds: [availableTrimId],
    businessNote: "互斥配置",
    values: [
      availabilityValue(featureCode, "comfort", availableTrimId),
      availabilityValue(featureCode, "premium", availableTrimId),
    ],
  };
}

function compareDataWithLuxuryTarget(): CompareResponse {
  return {
    ...compareData,
    trims: [
      ...compareData.trims,
      {
        trimId: "luxury",
        fullTrimName: "两驱豪华型 Luxury-FWD",
        brand: "OMODA",
        modelName: "T19C MY ICE",
        trimName: "Luxury-FWD",
      },
    ],
    rows: compareData.rows.map((row) => ({
      ...row,
      values: [
        row.values[0],
        row.values[1],
        row.featureCode === "speaker_count"
          ? {
              valueId: "luxury-speaker",
              rawValue: "10",
              normalizedValue: "10",
              availability: "VALUE",
              unit: null,
              valueState: "numeric_value",
              displayValue: "10",
              inferred: false,
              source: null,
            }
          : row.values[1],
      ],
    })),
  };
}

const compareDataWithManyUpgradeInsights: CompareResponse = {
  trims: compareData.trims,
  rows: [
    swapFeatureRow("audio_6_speakers", "6 speakers / 6扬声器", "信息娱乐 Information&Entertainment", "comfort"),
    swapFeatureRow("sony_8_speakers", "SONY 8 speakers / SONY 8扬声器", "信息娱乐 Information&Entertainment", "premium"),
    swapFeatureRow("wheel_16", "16 inch wheel / 16寸轮毂", "外饰 Exterior", "comfort"),
    swapFeatureRow("wheel_18", "18 inch wheel / 18寸轮毂", "外饰 Exterior", "premium"),
    swapFeatureRow("rear_camera", "Rear camera / 倒车影像", "驾驶辅助 Drive assist", "comfort"),
    swapFeatureRow("camera_360", "360 round view camera / 360度高清全景影像", "驾驶辅助 Drive assist", "premium"),
    swapFeatureRow("halogen_headlight", "Halogen headlights / 卤素大灯", "外饰 Exterior", "comfort"),
    swapFeatureRow("led_headlight", "LED headlights / LED大灯", "外饰 Exterior", "premium"),
  ],
  groups: [],
  totalFeatures: 8,
  shownFeatures: 8,
};

const originalClipboard = navigator.clipboard;

describe("BusinessSummaryPanel", () => {
  function getAiSummaryRegenerateButton(): HTMLElement {
    const actions = within(screen.getByLabelText("AI 配置对比摘要")).getByLabelText("AI 摘要操作") as HTMLDetailsElement;
    if (!actions.open) {
      fireEvent.click(within(actions).getByText("摘要操作"));
    }
    return within(actions).getByRole("button", { name: "重新生成" });
  }

  afterEach(() => {
    cleanup();
    clearEngineeringConfigBusinessSummaryCache();
    vi.clearAllMocks();
    vi.useRealTimers();
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: originalClipboard,
    });
  });

  it("summarizes target deltas from the selected base trim", () => {
    render(<BusinessSummaryPanel data={compareData} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    expect(screen.getByText("配置业务摘要")).toBeTruthy();
    expect(screen.getByLabelText("配置业务摘要")).toBeTruthy();
    expect(screen.getByLabelText("摘要统计口径").textContent).toContain("表格默认展示全部配置行；摘要只提炼其中的业务差异，查看差异项后才收窄表格。");
    expect(screen.getByText(/Comfort-FWD 作为基准列，当前对比 1 个目标配置列，累计 2 个目标差异/)).toBeTruthy();
    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD：新增 1 项，值变化 1 项/)).toBeTruthy();
    expect(screen.getByText(/其中规则推断 1 项/)).toBeTruthy();
    const targetBrief = screen.getByLabelText("Premium-FWD 业务解读");
    expect(targetBrief.textContent).toContain("业务解读");
    expect(targetBrief.textContent).toContain("Premium-FWD 相比 Comfort-FWD：主要增加 1项 · 泊车辅助：360 round view camera / 360度高清全景影像");
    expect(targetBrief.textContent).toContain("配置表达变化 1项 · 音响系统：Speaker count / 扬声器数量");
    expect(targetBrief.textContent).toContain("其中 1 项为规则推断，解释结论前应点开来源核对。");
    const focusGroups = screen.getByLabelText("Premium-FWD 业务重点分组");
    expect(focusGroups.textContent).toContain("业务重点分组");
    expect(focusGroups.textContent).toContain("泊车辅助");
    expect(focusGroups.textContent).toContain("新增 1 项");
    expect(focusGroups.textContent).toContain("360 round view camera / 360度高清全景影像");
    expect(focusGroups.textContent).toContain("驾驶辅助 Drive assist · 推断 1");
    expect(focusGroups.textContent).toContain("音响系统");
    expect(focusGroups.textContent).toContain("值变化 1 项");
    expect(focusGroups.textContent).toContain("Speaker count / 扬声器数量");
    const conclusionDraft = screen.getByLabelText("Premium-FWD 结论草稿");
    expect(conclusionDraft.textContent).toContain("需核对推断");
    expect(conclusionDraft.textContent).toContain("推断值待来源确认");
    expect(conclusionDraft.textContent).toContain("初稿：Premium-FWD 相比 Comfort-FWD 增加 1 项配置，参数变化 1 项");
    expect(conclusionDraft.textContent).toContain("含规则推断 1 项；不配备* 需回看来源后再引用。");
    const conclusionStatus = screen.getByLabelText("结论状态汇总");
    expect(conclusionStatus.textContent).toContain("结论状态");
    expect(conclusionStatus.textContent).toContain("需核对推断");
    expect(conclusionStatus.textContent).toContain("1");
    expect(conclusionStatus.textContent).toContain("Premium-FWD");
    const targetAction = screen.getByLabelText("Premium-FWD 业务动作建议");
    expect(targetAction.textContent).toContain("核对推断");
    expect(targetAction.textContent).toContain("业务结论需带证据口径");
    expect(targetAction.textContent).toContain("1 项为规则推断，带 * 的“不配备”不能当作 Excel 原文直接引用。");
    const baseStoryline = screen.getByLabelText("基准配置列差异脉络");
    expect(baseStoryline.textContent).toContain("基准角色");
    expect(baseStoryline.textContent).toContain("Comfort-FWD");
    expect(baseStoryline.textContent).toContain("业务方向");
    expect(baseStoryline.textContent).toContain("目标差异 2");
    expect(baseStoryline.textContent).toContain("新增 1 · 值变化 1");
    expect(baseStoryline.textContent).toContain("配置行去重 2 行");
    expect(baseStoryline.textContent).toContain("集中维度");
    expect(baseStoryline.textContent).toContain("泊车辅助 1 · 音响系统 1");
    expect(baseStoryline.textContent).toContain("证据边界");
    expect(baseStoryline.textContent).toContain("规则推断目标差异 1");
    expect(baseStoryline.textContent).toContain("配置行去重 1 行");
    const targetOverview = screen.getByLabelText("当前基准对比速览");
    expect(targetOverview.textContent).toContain("当前基准对比速览");
    expect(targetOverview.textContent).toContain("真实版本顺序见相邻版本升级路径");
    expect(targetOverview.textContent).toContain("Base · Comfort-FWD");
    expect(targetOverview.textContent).toContain("当前基准");
    expect(targetOverview.textContent).toContain("Target 1 · Premium-FWD");
    expect(targetOverview.textContent).toContain("Premium-FWD");
    expect(targetOverview.textContent).toContain("新增 1 项，值变化 1 项");
    expect(targetOverview.textContent).toContain("含规则推断 1 项");
    expect(screen.queryByLabelText("相邻版本升级路径")).toBeNull();
    const targetInsight = screen.getByLabelText("Premium-FWD 业务结论");
    expect(targetInsight.textContent).toContain("增配重点");
    expect(targetInsight.textContent).toContain("1项 · 泊车辅助：");
    expect(targetInsight.textContent).toContain("360 round view camera / 360度高清全景影像");
    expect(targetInsight.textContent).toContain("参数变化");
    expect(targetInsight.textContent).toContain("1项 · 音响系统：");
    expect(targetInsight.textContent).toContain("Speaker count / 扬声器数量");
    expect(targetInsight.textContent).toContain("证据提示");
    expect(targetInsight.textContent).toContain("含规则推断 1 项，优先点开来源核对。");
    expect(screen.getByText("主要增加")).toBeTruthy();
    expect(screen.getByText("主要减少")).toBeTruthy();
    expect(screen.getByText("值 / 选装变化")).toBeTruthy();
    expect(screen.getByText("待证据确认")).toBeTruthy();
    expect(screen.getAllByText("驾驶辅助 Drive assist").length).toBeGreaterThan(0);
    expect(screen.getAllByText("信息娱乐 Information&Entertainment").length).toBeGreaterThan(0);
    expect(screen.getByText("新增 1 · 推断 1")).toBeTruthy();
    expect(screen.getByText("值变化 1")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "1新增配置")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "1值变化")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "1规则推断")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "0待确认")).toBeTruthy();
    expect(screen.queryByLabelText("Excel 对比导读")).toBeNull();
  });

  it("renders LLM-composed business summary when enabled", async () => {
    const onFocusDeltaType = vi.fn();
    const onFocusFeatureRow = vi.fn();
    const onOpenEvidence = vi.fn();
    const compareDataWithReviewNote: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode === "camera_360"
        ? {
            ...row,
            businessNote: "需核对：OCR 值单元格像配置项文本，引用前回看原始图片。",
          }
        : row),
    };
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "Premium 相比 Basic 的主要升级集中在泊车辅助、音响和舒适便利配置。",
          mainUpgrades: [
            "泊车辅助：倒车影像升级为 360 全景影像",
            "音响：6 扬声器升级为 SONY 8 扬声器",
          ],
          replacementsOrReductions: ["手动空调被双区自动空调替代"],
          evidenceStatus: ["36 项来自规则推断，不是 Excel 原文"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "premium:ADDED:camera_360",
              featureCode: "camera_360",
              category: "驾驶辅助 Drive assist",
              reason: "AI 摘要中的 360 全景影像升级来自 camera_360 配置差异。",
            },
            {
              section: "mainUpgrades",
              itemIndex: 1,
              evidenceKey: "premium:VALUE_CHANGED:speaker_count",
              featureCode: "speaker_count",
              category: "信息娱乐 Information&Entertainment",
              reason: "AI 摘要中的音响升级来自 speaker_count 参数变化。",
            },
          ],
          recommendedUse: "可用于配置对比页的业务摘要，但引用前需要核对 evidence。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 100,
        completionTokens: 50,
        totalTokens: 150,
      },
    });

    render(
      <BusinessSummaryPanel
        data={compareDataWithReviewNote}
        baseTrimId="comfort"
        llmSummaryEnabled
        onFocusDeltaType={onFocusDeltaType}
        onFocusFeatureRow={onFocusFeatureRow}
        onOpenEvidence={onOpenEvidence}
      />,
    );

    expect(await screen.findByText("AI 业务摘要")).toBeTruthy();
    expect(await screen.findByText("Premium 相比 Basic 的主要升级集中在泊车辅助、音响和舒适便利配置。")).toBeTruthy();
    expect(screen.getByText("deepseek / deepseek-chat 运行时生成 · 当前对比实时生成，缓存命中会复用，不是上传文件的持久摘要")).toBeTruthy();
    expect(screen.getByText("泊车辅助：倒车影像升级为 360 全景影像")).toBeTruthy();
    expect(screen.getByText("手动空调被双区自动空调替代")).toBeTruthy();
    expect(screen.getByText("36 项来自规则推断，不是 Excel 原文")).toBeTruthy();
    await waitFor(() => {
      expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
    });
    const composePayload = vi.mocked(api.composeEngineeringConfigBusinessSummary).mock.calls[0][0];
    expect(composePayload).toEqual({
      trimIds: ["comfort", "premium"],
      baseTrimId: "comfort",
      versionScope: "published",
      filters: {
        deltaFilter: "ALL",
        category: null,
        search: null,
        targetTrimId: "premium",
      },
    });
    expect(composePayload).not.toHaveProperty("targets");
    expect(composePayload).not.toHaveProperty("context");
    fireEvent.click(screen.getByRole("button", { name: "查看 AI 摘要证据：两驱尊贵型 Premium-FWD 泊车辅助：倒车影像升级为 360 全景影像" }));
    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "camera_360" }),
      trim: expect.objectContaining({ trimId: "comfort" }),
      cell: expect.objectContaining({ valueId: "comfort-camera" }),
      selectionReason: "AI 摘要中的 360 全景影像升级来自 camera_360 配置差异。",
    }));
    fireEvent.click(screen.getByRole("button", { name: "定位 AI 摘要配置行：两驱尊贵型 Premium-FWD 泊车辅助：倒车影像升级为 360 全景影像" }));
    expect(onFocusFeatureRow).toHaveBeenCalledWith(
      expect.objectContaining({ featureCode: "camera_360" }),
      "premium",
      "INFERRED",
    );
    expect(screen.getByLabelText("高级规则诊断已收起")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "复制全部结论" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "查看高级规则诊断" }));
    await waitFor(() => {
      expect(screen.getByLabelText("Premium-FWD 业务重点分组")).toBeTruthy();
    });
    expect(screen.getByLabelText("高级规则诊断说明").textContent).toContain("对外话术以 AI 结论为准");
    expect(screen.queryByRole("button", { name: "复制全部结论" })).toBeNull();
    expect(screen.queryByLabelText("规则审核入口")).toBeNull();
    expect(screen.queryByText("规则审核已收起")).toBeNull();
    expect(screen.getByLabelText("结论状态汇总")).toBeTruthy();
    expect(screen.getByLabelText("基准对比结论")).toBeTruthy();
    expect(screen.getByLabelText("基准配置列差异脉络")).toBeTruthy();
    expect(screen.getByLabelText("当前基准对比速览")).toBeTruthy();
    expect(screen.getByLabelText("Premium-FWD 业务解读")).toBeTruthy();
    expect(screen.getByLabelText("Premium-FWD 结论草稿")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "复制当前 AI 摘要" }));
    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copiedText = writeText.mock.calls[0][0];
    expect(copiedText).toContain("AI 配置对比摘要");
    expect(copiedText).toContain("基准配置列：Comfort-FWD");
    expect(copiedText).toContain("AI 来源：deepseek / deepseek-chat");
    expect(copiedText).toContain("目标配置列 1：两驱尊贵型 Premium-FWD");
    expect(copiedText).toContain("泊车辅助：倒车影像升级为 360 全景影像");
    expect(copiedText).toContain("36 项来自规则推断，不是 Excel 原文");
    expect(copiedText).toContain("证据引用:");
    expect(copiedText).toContain("主要升级 #1 -> premium:ADDED:camera_360");
    expect(copiedText).toContain("AI 摘要中的 360 全景影像升级来自 camera_360 配置差异。");
    expect(await screen.findByText("AI 摘要已复制。")).toBeTruthy();
  });

  it("sends only identity and scope even when the table has many differences", async () => {
    const manyDifferenceRows: CompareResponse["rows"] = Array.from({ length: 45 }, (_, index) => ({
      category: index < 20 ? "舒适便利 Comfort&Convenient" : "驾驶辅助 Drive assist",
      featureCode: `rich_feature_${index}`,
      featureName: `Rich feature ${index} / 丰富事实 ${index}`,
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: ["premium"],
      businessNote: index % 7 === 0 ? "需核对：来源行需要人工确认。" : "配置差异事实。",
      values: [
        {
          valueId: `comfort-rich-${index}`,
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          displayValue: "不配备*",
          inferred: true,
          inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
          confidence: 0.7,
          source: null,
        },
        {
          valueId: `premium-rich-${index}`,
          rawValue: "●",
          normalizedValue: "standard",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
          inferred: false,
          source: {
            sheetName: "T19C MY ICE",
            rowNumber: 100 + index,
            columnNumber: 6,
            columnLetter: "F",
            cell: `F${100 + index}`,
            sourceCell: `F${100 + index}`,
            mergedRange: null,
          },
        },
      ],
    }));
    const richCompareData: CompareResponse = {
      trims: compareData.trims,
      rows: manyDifferenceRows,
      groups: [],
      totalFeatures: manyDifferenceRows.length,
      shownFeatures: manyDifferenceRows.length,
    };
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "Premium 相比 Comfort 的 AI 摘要覆盖多项配置事实。",
          mainUpgrades: ["舒适便利：新增多项配置"],
          replacementsOrReductions: [],
          evidenceStatus: ["部分配置来自规则推断，不是 Excel 原文"],
          recommendedUse: "用于验证 LLM facts payload。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 100,
        completionTokens: 50,
        totalTokens: 150,
      },
    });

    render(
      <BusinessSummaryPanel
        data={richCompareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText("Premium 相比 Comfort 的 AI 摘要覆盖多项配置事实。")).toBeTruthy();
    await waitFor(() => {
      expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
    });
    const composePayload = vi.mocked(api.composeEngineeringConfigBusinessSummary).mock.calls[0][0];
    expect(composePayload).toEqual({
      trimIds: ["comfort", "premium"],
      baseTrimId: "comfort",
      versionScope: "published",
      filters: {
        deltaFilter: "ALL",
        category: null,
        search: null,
        targetTrimId: "premium",
      },
    });
    expect(composePayload).not.toHaveProperty("rows");
    expect(composePayload).not.toHaveProperty("targets");
  });

  it("defers initial AI summary composition until after the first table paint but regenerates immediately", async () => {
    vi.useFakeTimers();
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValue({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "首屏延后生成的 AI 摘要",
          mainUpgrades: ["泊车辅助：倒车影像升级为 360 全景影像"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用前核对 evidence"],
          recommendedUse: "可用于业务摘要。",
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
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();
    expect(screen.getByText("AI 摘要将在首屏稳定后自动生成；下方配置表可以先查看。")).toBeTruthy();
    expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();

    await act(async () => {
      vi.runOnlyPendingTimers();
      await Promise.resolve();
    });

    expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
    expect(screen.getByText("首屏延后生成的 AI 摘要")).toBeTruthy();
    expect(screen.getByLabelText("AI 摘要操作")).toBeTruthy();

    const regenerateButton = getAiSummaryRegenerateButton();
    await act(async () => {
      fireEvent.click(regenerateButton);
      await Promise.resolve();
    });

    expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.composeEngineeringConfigBusinessSummary).mock.calls[1][0].filters).toEqual(expect.objectContaining({
      forceRefresh: true,
    }));
  });

  it("reuses a cached AI summary when the same compare scope remounts", async () => {
    const headline = "Premium 相比 Basic 的 AI 摘要来自缓存。";
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline,
          mainUpgrades: ["泊车辅助：倒车影像升级为 360 全景影像"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用前核对 evidence"],
          recommendedUse: "可用于业务摘要。",
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
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText(headline)).toBeTruthy();
    await waitFor(() => {
      expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
    });

    cleanup();
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockClear();

    const focusFeatureRow = vi.fn();
    const openEvidence = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onFocusFeatureRow={focusFeatureRow}
        onOpenEvidence={openEvidence}
      />,
    );

    expect(await screen.findByText(headline)).toBeTruthy();
    expect(screen.getByText("AI 结论已复用")).toBeTruthy();
    expect(screen.queryByText("deepseek / deepseek-chat 运行时缓存复用；不是上传文件的持久摘要，引用前点开来源证据核对")).toBeNull();
    expect(within(screen.getByLabelText("AI 配置对比摘要")).queryByRole("button", { name: "重新生成" })).toBeNull();
    expect(getAiSummaryRegenerateButton()).toBeTruthy();
    expect(api.composeEngineeringConfigBusinessSummary).not.toHaveBeenCalled();
  });

  it("labels backend composer cache hits as reused AI conclusions", async () => {
    const headline = "Premium 相比 Basic 的 AI 摘要来自后端缓存。";
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline,
          mainUpgrades: ["泊车辅助：倒车影像升级为 360 全景影像"],
          replacementsOrReductions: [],
          evidenceStatus: ["引用前核对 evidence"],
          recommendedUse: "可用于业务摘要。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 80,
        completionTokens: 40,
        totalTokens: 120,
        cacheHit: true,
      },
    });

    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText(headline)).toBeTruthy();
    expect(screen.getByText("AI 结论已复用")).toBeTruthy();
    expect(screen.queryByText("deepseek / deepseek-chat 运行时缓存复用；不是上传文件的持久摘要，引用前点开来源证据核对")).toBeNull();
    expect(within(screen.getByLabelText("AI 配置对比摘要")).queryByRole("button", { name: "重新生成" })).toBeNull();
    expect(getAiSummaryRegenerateButton()).toBeTruthy();
    expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
  });

  it("uses AI summary as the simple-mode primary view instead of showing deterministic blocks", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "Premium 相比 Basic 的主要升级集中在泊车辅助和音响。",
          mainUpgrades: [
            "泊车辅助：倒车影像升级为 360 全景影像",
            "音响：6 扬声器升级为 SONY 8 扬声器",
          ],
          replacementsOrReductions: ["手动折叠后视镜被电动折叠替代"],
          evidenceStatus: ["1 项来自规则推断，不是 Excel 原文"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "premium:ADDED:camera_360",
            },
          ],
          recommendedUse: "适合做业务摘要，引用前点开 evidence 核对。",
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

    const focusFeatureRow = vi.fn();
    const openEvidence = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onFocusFeatureRow={focusFeatureRow}
        onOpenEvidence={openEvidence}
      />,
    );

    expect(await screen.findByText("AI 配置对比摘要")).toBeTruthy();
    expect(await screen.findByText("Premium 相比 Basic 的主要升级集中在泊车辅助和音响。")).toBeTruthy();
    expect(screen.getByText("AI 结论已生成")).toBeTruthy();
    expect(screen.queryByText("由 deepseek / deepseek-chat 运行时生成；不是上传文件的持久摘要，引用前点开来源证据核对")).toBeNull();
    const tableJump = screen.getByRole("link", { name: "查看配置表" });
    expect(tableJump.getAttribute("href")).toBe("#config-compare-table");
    expect(within(screen.getByLabelText("AI 配置对比摘要")).queryByRole("button", { name: "重新生成" })).toBeNull();
    expect(screen.getByLabelText("AI 摘要操作")).toBeTruthy();
    expect(screen.queryByText("deepseek-chat · ok · 120 tokens")).toBeNull();
    expect(screen.queryByText("deepseek / deepseek-chat 运行时生成 · 当前对比实时生成，缓存命中会复用，不是上传文件的持久摘要")).toBeNull();
    expect(screen.getByText("证据提示：含规则推断，引用前核对来源证据。")).toBeTruthy();
    const summaryCard = screen.getByLabelText("AI 结论和证据：两驱尊贵型 Premium-FWD");
    expect(summaryCard.tagName).toBe("ARTICLE");
    const compactDetails = summaryCard.querySelector("details") as HTMLDetailsElement;
    expect(compactDetails).toBeTruthy();
    expect(compactDetails.open).toBe(false);
    expect(within(summaryCard).getByText((_content, element) => (
      element?.textContent === "展开 AI 要点"
    ))).toBeTruthy();
    expect(within(summaryCard).queryByLabelText("两驱尊贵型 Premium-FWD AI 摘要速读")).toBeNull();
    expect(within(summaryCard).queryByLabelText("两驱尊贵型 Premium-FWD AI 摘要证据快捷入口")).toBeNull();
    expect(within(summaryCard).getByRole("button", { name: "核对 AI 结论证据：两驱尊贵型 Premium-FWD" })).toBeTruthy();
    expect(within(summaryCard).getByRole("button", { name: "定位 AI 结论配置行：两驱尊贵型 Premium-FWD" })).toBeTruthy();
    expect(within(summaryCard).queryByRole("button", { name: "查看 AI 摘要证据：两驱尊贵型 Premium-FWD 泊车辅助：倒车影像升级为 360 全景影像" })).toBeNull();
    expect(within(summaryCard).queryByRole("button", { name: "定位 AI 摘要配置行：两驱尊贵型 Premium-FWD 泊车辅助：倒车影像升级为 360 全景影像" })).toBeNull();
    expect(within(summaryCard).queryByText("泊车辅助：倒车影像升级为 360 全景影像")).toBeNull();
    fireEvent.click(within(summaryCard).getByText("展开 AI 要点"));
    expect(compactDetails.open).toBe(true);
    expect(within(summaryCard).getByText("收起 AI 要点")).toBeTruthy();
    expect(within(summaryCard).getByText("泊车辅助：倒车影像升级为 360 全景影像")).toBeTruthy();
    expect(within(summaryCard).getAllByText("手动折叠后视镜被电动折叠替代").length).toBeGreaterThanOrEqual(1);
    fireEvent.click(within(summaryCard).getByText("收起 AI 要点"));
    expect(compactDetails.open).toBe(false);
    expect(within(summaryCard).getByText("展开 AI 要点")).toBeTruthy();
    expect(within(summaryCard).getByRole("button", { name: "核对 AI 结论证据：两驱尊贵型 Premium-FWD" })).toBeTruthy();
    expect(within(summaryCard).queryByText("泊车辅助：倒车影像升级为 360 全景影像")).toBeNull();
    fireEvent.click(within(summaryCard).getByText("展开 AI 要点"));
    expect(compactDetails.open).toBe(true);
    expect(within(summaryCard).queryByText("1 项来自规则推断，不是 Excel 原文")).toBeNull();
    expect(within(summaryCard).queryByLabelText("两驱尊贵型 Premium-FWD AI 证据边界")).toBeNull();
    expect(within(summaryCard).queryByLabelText("两驱尊贵型 Premium-FWD AI 结论要点")).toBeNull();
    expect(screen.queryByText("基准对比结论")).toBeNull();
    expect(screen.queryByText("当前基准对比速览")).toBeNull();
    expect(screen.queryByText("相邻版本升级路径")).toBeNull();
    expect(screen.queryByText("目标结论")).toBeNull();
    expect(screen.queryByText("业务重点分组")).toBeNull();
    expect(screen.queryByRole("button", { name: "复制全部结论" })).toBeNull();
    expect(screen.queryByRole("button", { name: "查看高级诊断" })).toBeNull();
    const expandedEvidenceButton = within(summaryCard).getByRole("button", { name: "核对 AI 结论证据：两驱尊贵型 Premium-FWD" });
    const expandedFocusButton = within(summaryCard).getByRole("button", { name: "定位 AI 结论配置行：两驱尊贵型 Premium-FWD" });
    expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
    fireEvent.click(expandedFocusButton);
    expect(focusFeatureRow).toHaveBeenCalledWith(
      expect.objectContaining({ featureCode: "camera_360" }),
      "premium",
      "INFERRED",
    );
    fireEvent.click(expandedEvidenceButton);
    expect(openEvidence).toHaveBeenCalledWith(expect.objectContaining({
      selectionReason: "AI 摘要引用了 360 round view camera / 360度高清全景影像，用于解释 Premium-FWD 相对 Comfort-FWD 的配置差异。",
    }));
    expect(screen.queryByLabelText("AI 摘要轻量兜底")).toBeNull();
    expect(screen.queryByLabelText("Excel 首屏速读")).toBeNull();
    expect(screen.queryByLabelText("Excel 对比导读")).toBeNull();
    expect(screen.queryByLabelText("简易模式表格与摘要关系")).toBeNull();
    expect(screen.queryByLabelText("版本差异速读")).toBeNull();
    expect(screen.queryByLabelText("基准对比结论")).toBeNull();
    expect(screen.queryByLabelText("基准配置列差异脉络")).toBeNull();
    expect(screen.queryByLabelText("当前基准对比速览")).toBeNull();
    expect(screen.queryByLabelText("规则审核入口")).toBeNull();
    expect(screen.queryByLabelText("目标配置列结论抽屉")).toBeNull();
    expect(screen.queryByLabelText("Excel 列对比结果")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务重点分组")).toBeNull();
  });

  it("keeps compact AI targets folded by default while detail lists remain expandable", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "Premium 相比 Comfort 的升级集中在泊车辅助。",
          mainUpgrades: [
            "泊车辅助：倒车影像升级为 360 全景影像",
            "舒适便利：新增电动尾门",
            "座椅：新增前排座椅加热",
            "灯光：新增自适应远近光",
          ],
          replacementsOrReductions: [],
          evidenceStatus: ["1 项来自规则推断，不是 Excel 原文"],
          evidenceRefs: [
            {
              section: "mainUpgrades",
              itemIndex: 0,
              evidenceKey: "premium:ADDED:camera_360",
            },
          ],
          recommendedUse: "",
        },
        {
          targetTrimId: "luxury",
          targetLabel: "两驱豪华型 Luxury-FWD",
          headline: "Luxury 相比 Comfort 的升级集中在音响和舒适配置。",
          mainUpgrades: ["音响：8 扬声器升级为 10 扬声器"],
          replacementsOrReductions: [],
          evidenceStatus: ["年款信息待补充，引用前核对 evidence"],
          recommendedUse: "",
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
      <BusinessSummaryPanel
        data={compareDataWithLuxuryTarget()}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText("Premium 相比 Comfort 的升级集中在泊车辅助。")).toBeTruthy();
    expect(await screen.findByText("Luxury 相比 Comfort 的升级集中在音响和舒适配置。")).toBeTruthy();

    const premiumCard = screen.getByLabelText("AI 结论和证据：两驱尊贵型 Premium-FWD");
    const luxuryCard = screen.getByLabelText("AI 结论和证据：两驱豪华型 Luxury-FWD");
    const premiumDetails = premiumCard.querySelector("details") as HTMLDetailsElement;
    const luxuryDetails = luxuryCard.querySelector("details") as HTMLDetailsElement;

    expect(premiumDetails.open).toBe(false);
    expect(luxuryDetails.open).toBe(false);
    expect(within(premiumCard).getByText("展开 AI 要点")).toBeTruthy();
    expect(within(premiumCard).getByRole("button", { name: "核对 AI 结论证据：两驱尊贵型 Premium-FWD" })).toBeTruthy();
    expect(within(premiumCard).queryByRole("button", { name: "查看 AI 摘要证据：两驱尊贵型 Premium-FWD 泊车辅助：倒车影像升级为 360 全景影像" })).toBeNull();
    expect(within(premiumCard).queryByText("泊车辅助：倒车影像升级为 360 全景影像")).toBeNull();
    fireEvent.click(within(premiumCard).getByText("展开 AI 要点"));
    expect(premiumDetails.open).toBe(true);
    expect(within(premiumCard).getByText("泊车辅助：倒车影像升级为 360 全景影像")).toBeTruthy();
    expect(within(premiumCard).getByText("舒适便利：新增电动尾门")).toBeTruthy();
    expect(within(premiumCard).getByText("座椅：新增前排座椅加热")).toBeTruthy();
    expect(within(premiumCard).getByText("灯光：新增自适应远近光")).toBeTruthy();
    expect(within(premiumCard).queryByText("另 1 项已收起；复制 AI 摘要或在下方配置表中核对完整条目。")).toBeNull();
    expect(within(premiumCard).queryByText("1 项来自规则推断，不是 Excel 原文")).toBeNull();
    expect(within(premiumCard).queryByLabelText("两驱尊贵型 Premium-FWD AI 证据边界")).toBeNull();
    expect(within(luxuryCard).getByText("展开 AI 要点")).toBeTruthy();
    expect(within(luxuryCard).queryByText("音响：8 扬声器升级为 10 扬声器")).toBeNull();
    expect(within(luxuryCard).getByText("证据提示：年款信息待补充，引用前核对来源证据")).toBeTruthy();
    fireEvent.click(within(luxuryCard).getByText("展开 AI 要点"));
    expect(luxuryDetails.open).toBe(true);
    expect(within(luxuryCard).getByText("音响：8 扬声器升级为 10 扬声器")).toBeTruthy();
    expect(within(luxuryCard).queryByLabelText("两驱豪华型 Luxury-FWD AI 证据边界")).toBeNull();
  });

  it("does not treat LLM text similarity as formal evidence when refs are missing", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "Premium 相比 Basic 的泊车辅助升级明显。",
          mainUpgrades: ["新增360度高清全景影像，取代倒车影像，提升泊车辅助体验"],
          replacementsOrReductions: [],
          evidenceStatus: ["360度高清全景影像需要人工核对；证据状态不是配置行入口。"],
          recommendedUse: "引用前点开 evidence 核对。",
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

    const focusFeatureRow = vi.fn();
    const openEvidence = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onFocusFeatureRow={focusFeatureRow}
        onOpenEvidence={openEvidence}
      />,
    );

    const summaryCard = await screen.findByLabelText("AI 结论和证据：两驱尊贵型 Premium-FWD");
    expect(within(summaryCard).getByText("证据提示：含需核对项，引用前核对来源证据。")).toBeTruthy();
    const compactDetails = summaryCard.querySelector("details") as HTMLDetailsElement;
    if (!compactDetails.open) fireEvent.click(summaryCard.querySelector("summary") as HTMLElement);
    expect(summaryCard.querySelectorAll('button[aria-label^="定位 AI 摘要配置行："]')).toHaveLength(0);
    expect(summaryCard.querySelectorAll('button[aria-label^="查看 AI 摘要证据："]')).toHaveLength(0);
    expect(within(summaryCard).getByText("未匹配配置证据，不可直接引用")).toBeTruthy();
    expect(within(summaryCard).queryByRole("button", { name: "定位 AI 结论配置行：两驱尊贵型 Premium-FWD" })).toBeNull();
    expect(within(summaryCard).queryByRole("button", { name: "核对 AI 结论证据：两驱尊贵型 Premium-FWD" })).toBeNull();
    expect(focusFeatureRow).not.toHaveBeenCalled();
    expect(openEvidence).not.toHaveBeenCalled();
  });

  it("marks an LLM claim as not directly citable when no config evidence matches", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "两驱尊贵型 Premium-FWD",
          headline: "Premium 的 AI 摘要包含一条无法核对的内容。",
          mainUpgrades: ["新增飞行模式"],
          replacementsOrReductions: [],
          evidenceStatus: ["1 条 AI 结论未匹配到配置证据，不可直接引用。"],
          evidenceRefs: [],
          evidenceBoundClaimCount: 0,
          unsupportedEvidenceCount: 1,
          recommendedUse: "仅使用已绑定证据的结论。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 20,
        completionTokens: 10,
        totalTokens: 30,
      },
    });

    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    const summaryCard = await screen.findByLabelText("AI 结论和证据：两驱尊贵型 Premium-FWD");
    const details = summaryCard.querySelector("details") as HTMLDetailsElement;
    if (!details.open) fireEvent.click(summaryCard.querySelector("summary") as HTMLElement);

    expect(within(summaryCard).getByText("新增飞行模式")).toBeTruthy();
    expect(within(summaryCard).getByText("未匹配配置证据，不可直接引用")).toBeTruthy();
    expect(summaryCard.querySelectorAll('button[aria-label^="查看 AI 摘要证据："]')).toHaveLength(0);
  });

  it("keeps expert deterministic blocks visible when AI summary is unavailable", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "missing_key",
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
        fallbackReason: "LLM provider unavailable",
      },
    });

    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText("LLM provider unavailable")).toBeTruthy();
    expect(screen.getByLabelText("基准对比结论")).toBeTruthy();
    expect(screen.getByLabelText("基准配置列差异脉络")).toBeTruthy();
    expect(screen.getByLabelText("当前基准对比速览")).toBeTruthy();
    expect(screen.getByLabelText("Premium-FWD 业务解读")).toBeTruthy();
    expect(screen.queryByLabelText("规则审核入口")).toBeNull();
  });

  it("keeps simple-mode deterministic blocks hidden and avoids raw provider errors when AI summary is unavailable", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "missing_key",
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
        fallbackReason: "[SSL: SSLV3_ALERT_BAD_RECORD_MAC] ssl/tls alert bad record mac (_ssl.c:2559)",
      },
    });

    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText("AI 摘要暂不可用；配置表和来源证据仍可继续查看。")).toBeTruthy();
    expect(screen.queryByText(/\[SSL:/)).toBeNull();
    expect(screen.queryByText(/_ssl\.c/)).toBeNull();
    expect(screen.getByText("AI 摘要暂不可用")).toBeTruthy();
    expect(screen.queryByText("AI 摘要暂不可用；仍可点开来源证据核对")).toBeNull();
    expect(within(screen.getByLabelText("AI 配置对比摘要")).queryByRole("button", { name: "重新生成" })).toBeNull();
    expect(getAiSummaryRegenerateButton()).toBeTruthy();
    expect(screen.queryByText("由 deepseek / deepseek-chat 运行时生成；不是上传文件的持久摘要，引用前点开来源证据核对")).toBeNull();
    expect(screen.queryByText("deepseek / deepseek-chat 运行时生成 · 当前对比实时生成，缓存命中会复用，不是上传文件的持久摘要")).toBeNull();
    expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
    expect(screen.queryByLabelText("AI 摘要轻量兜底")).toBeNull();
    expect(screen.queryByText("规则兜底")).toBeNull();
    expect(screen.queryByText("展开查看简洁速读")).toBeNull();
    expect(screen.queryByLabelText("Excel 首屏速读")).toBeNull();
    expect(screen.queryByLabelText("Excel 对比导读")).toBeNull();
    expect(screen.queryByLabelText("简易模式表格与摘要关系")).toBeNull();
    expect(screen.queryByLabelText("版本差异速读")).toBeNull();
    expect(screen.queryByLabelText("基准对比结论")).toBeNull();
    expect(screen.queryByLabelText("基准配置列差异脉络")).toBeNull();
    expect(screen.queryByLabelText("规则审核入口")).toBeNull();
    expect(screen.queryByLabelText("目标配置列结论抽屉")).toBeNull();
  });

  it("keeps simple deterministic blocks hidden while refreshing an existing LLM summary", async () => {
    const headline = "Premium 相比 Basic 的主要升级集中在泊车辅助、音响和舒适便利配置。";
    let resolveSummary: (value: Awaited<ReturnType<typeof api.composeEngineeringConfigBusinessSummary>>) => void;
    vi.mocked(api.composeEngineeringConfigBusinessSummary)
      .mockResolvedValueOnce({
        summaries: [
          {
            targetTrimId: "premium",
            targetLabel: "两驱尊贵型 Premium-FWD",
            headline,
            mainUpgrades: ["泊车辅助：倒车影像升级为 360 全景影像"],
            replacementsOrReductions: [],
            evidenceStatus: ["36 项来自规则推断，不是 Excel 原文"],
            recommendedUse: "可用于配置对比页的业务摘要，但引用前需要核对 evidence。",
          },
        ],
        usage: {
          provider: "deepseek",
          model: "deepseek-chat",
          status: "ok",
          promptTokens: 100,
          completionTokens: 50,
          totalTokens: 150,
        },
      })
      .mockReturnValueOnce(
        new Promise((resolve) => {
          resolveSummary = resolve;
        }),
      );

    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(await screen.findByText(headline)).toBeTruthy();
    expect(screen.getByLabelText("AI 摘要操作")).toBeTruthy();
    fireEvent.click(getAiSummaryRegenerateButton());

    expect(screen.getByText("正在把配置差异改写成业务摘要；下方配置表可以继续查看来源证据。")).toBeTruthy();
    expect(screen.getByText(headline)).toBeTruthy();
    expect(screen.queryByLabelText("AI 摘要后的表格提示")).toBeNull();
    expect(screen.queryByLabelText("结论状态汇总")).toBeNull();
    expect(screen.queryByLabelText("基准对比结论")).toBeNull();
    expect(screen.queryByLabelText("基准配置列差异脉络")).toBeNull();
    expect(screen.queryByLabelText("当前基准对比速览")).toBeNull();
    expect(screen.queryByLabelText("简易模式表格与摘要关系")).toBeNull();
    expect(screen.queryByLabelText("版本差异速读")).toBeNull();
    expect(screen.queryByLabelText("AI 摘要轻量兜底")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务解读")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务重点分组")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 结论草稿")).toBeNull();
    expect(screen.queryByLabelText("Excel 首屏速读")).toBeNull();

    resolveSummary!({
      summaries: [],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "fallback",
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
        fallbackReason: "LLM provider unavailable",
      },
    });

    await waitFor(() => {
      expect(screen.getByText("AI 摘要刷新暂不可用；当前继续显示上一版摘要。")).toBeTruthy();
    });
    expect(screen.getByText(headline)).toBeTruthy();
    expect(screen.queryByText("LLM provider unavailable")).toBeNull();
    expect(screen.queryByLabelText("AI 摘要轻量兜底")).toBeNull();
    expect(screen.queryByLabelText("结论状态汇总")).toBeNull();
    expect(screen.queryByLabelText("简易模式表格与摘要关系")).toBeNull();
    expect(screen.queryByLabelText("版本差异速读")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务解读")).toBeNull();
  });

  it("keeps simple mode focused on Excel-style comparison controls", () => {
    const onShowDifferenceRows = vi.fn();
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        onFocusDeltaType={onFocusDeltaType}
        onShowDifferenceRows={onShowDifferenceRows}
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("Excel 配置对比导读")).toBeTruthy();
    expect(screen.getByLabelText("Excel 配置对比导读")).toBeTruthy();
    const fallbackDetails = screen.getByLabelText("规则速读备用") as HTMLDetailsElement;
    expect(fallbackDetails.open).toBe(false);
    expect(fallbackDetails.textContent).toContain("AI 摘要未启用时可展开");
    fireEvent.click(within(fallbackDetails).getByText("规则速读备用"));
    expect(fallbackDetails.open).toBe(true);
    expect(screen.getByLabelText("摘要统计口径").textContent).toContain("表格口径");
    expect(screen.getByLabelText("摘要统计口径").textContent).toContain("默认展示全部 xlsx 配置行");
    expect(screen.getByText(/基准列 Comfort-FWD；当前对比 1 个目标配置列，发现 2 个差异/)).toBeTruthy();
    expect(screen.getByLabelText("基准对比结论")).toBeTruthy();
    expect(screen.queryByLabelText("结论状态汇总")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务重点分组")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 差异统计")).toBeNull();
    expect(screen.queryByLabelText("基准配置列差异脉络")).toBeNull();
    expect(screen.queryByLabelText("当前基准对比速览")).toBeNull();
    const excelGuide = screen.getByLabelText("Excel 对比导读");
    expect(excelGuide.textContent).toContain("基准列");
    expect(excelGuide.textContent).toContain("Comfort-FWD");
    expect(excelGuide.textContent).toContain("对比口径");
    expect(excelGuide.textContent).toContain("身份待补");
    expect(excelGuide.textContent).toContain("当前表格");
    expect(excelGuide.textContent).toContain("全部配置行");
    expect(excelGuide.textContent).toContain("下面表格仍是完整 xlsx 配置行");
    expect(excelGuide.textContent).toContain("差异行");
    expect(excelGuide.textContent).toContain("2 个差异");
    expect(excelGuide.textContent).toContain("主要差异");
    expect(excelGuide.textContent).toContain("泊车辅助 1：360 round view camera / 360度高清全景影像");
    expect(excelGuide.textContent).toContain("音响系统 1：Speaker count / 扬声器数量");
    expect(excelGuide.textContent).toContain("下一步");
    expect(excelGuide.textContent).toContain("需核对推断");
    fireEvent.click(within(excelGuide).getByRole("button", { name: "查看 Excel 对比导读：差异行，2 个差异" }));
    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", null);
    fireEvent.click(within(excelGuide).getByRole("button", { name: "查看 Excel 对比导读：下一步，需核对推断" }));
    expect(onFocusDeltaType).toHaveBeenLastCalledWith("INFERRED", "premium");
    const priorityBlock = screen.getByLabelText("Excel 首屏速读");
    expect(priorityBlock.textContent).toContain("表格范围全部配置行");
    expect(priorityBlock.textContent).toContain("版本差异速读");
    const scopeBridge = within(priorityBlock).getByLabelText("简易模式表格与摘要关系");
    expect(scopeBridge.textContent).toContain("表格范围全部配置行");
    expect(scopeBridge.textContent).toContain("表格保留完整配置行；摘要只提炼业务差异。");
    expect(scopeBridge.textContent).toContain("差异提炼业务差异 2");
    expect(scopeBridge.textContent).toContain("默认不隐藏共同项，先给完整配置基线，再给业务差异摘要。");
    expect(scopeBridge.textContent).toContain("目标配置列全部目标配置列");
    expect(scopeBridge.textContent).toContain("1 个目标配置列一起汇总。");
    expect(scopeBridge.textContent).toContain("重点目标Premium-FWD");
    expect(scopeBridge.textContent).toContain("差异最多 2 项");
    expect(scopeBridge.textContent).toContain("泊车辅助 1：360 round view camera / 360度高清全景影像");
    expect(scopeBridge.textContent).toContain("音响系统 1：Speaker count / 扬声器数量");
    fireEvent.click(within(scopeBridge).getByRole("button", { name: "查看重点目标差异行：Premium-FWD" }));
    expect(onFocusDeltaType).toHaveBeenLastCalledWith("DIFFERENCE", "premium");
    const versionNarrative = within(priorityBlock).getByLabelText("版本差异速读");
    expect(versionNarrative.textContent).toContain("Premium-FWD 相比 Comfort-FWD");
    expect(versionNarrative.textContent).toContain("新增 1、值变化 1，集中在 泊车辅助 1、音响系统 1。");
    expect(versionNarrative.textContent).toContain("重点在 泊车辅助 1 项 · 音响系统 1 项");
    expect(versionNarrative.textContent).toContain("含规则推断 1 项，先回看来源");
    expect(versionNarrative.textContent).toContain("需核对推断 · 不配备* 需回看来源");
    fireEvent.click(within(versionNarrative).getByRole("button", { name: "查看该列差异：Premium-FWD 相对基准" }));
    expect(onFocusDeltaType).toHaveBeenLastCalledWith("DIFFERENCE", "premium");
    fireEvent.click(within(scopeBridge).getByRole("button", { name: "从全量配置查看差异行；表格将只展示业务差异行" }));
    expect(onShowDifferenceRows).toHaveBeenCalledTimes(1);
    const conclusionDrawer = screen.getByLabelText("目标配置列结论抽屉");
    expect(conclusionDrawer.hasAttribute("open")).toBe(false);
    expect(within(conclusionDrawer).getByText("目标结论")).toBeTruthy();
    expect(within(conclusionDrawer).getByText("1 个配置列")).toBeTruthy();
    expect(within(conclusionDrawer).getByText("展开查看每个目标配置列的增配、减配和证据边界；不会改变下方表格范围。")).toBeTruthy();
    fireEvent.click(within(conclusionDrawer).getByText("目标结论"));
    expect(conclusionDrawer.hasAttribute("open")).toBe(true);
    const simpleConclusion = screen.getByLabelText("Excel 列对比结果");
    expect(simpleConclusion.textContent).toContain("基准列");
    expect(simpleConclusion.textContent).toContain("Comfort-FWD 是基准列；1 个目标配置列，2 个差异，涉及 2 行配置");
    expect(simpleConclusion.textContent).toContain("主要在 泊车辅助 1、音响系统 1");
    expect(simpleConclusion.textContent).toContain("含规则推断 1 项");
    expect(simpleConclusion.textContent).toContain("Premium-FWD新增 1、值变化 1，集中在 泊车辅助 1、音响系统 1。");
    expect(simpleConclusion.textContent).not.toContain("Premium-FWDPremium-FWD");
    expect(simpleConclusion.textContent).toContain("业务重点：泊车辅助 1 项 · 音响系统 1 项");
    const simplePoints = screen.getByLabelText("Premium-FWD 业务差异要点");
    expect(simplePoints.textContent).toContain("主要增加");
    expect(simplePoints.textContent).toContain("1项 · 泊车辅助：360 round view camera / 360度高清全景影像");
    expect(simplePoints.textContent).toContain("参数 / 选装变化");
    expect(simplePoints.textContent).toContain("1项 · 音响系统：Speaker count / 扬声器数量");
    expect(simplePoints.textContent).toContain("证据边界");
    expect(simplePoints.textContent).toContain("规则推断 1 项，带 * 值需回看来源。");
    expect(simpleConclusion.textContent).toContain("需核对推断 · 含规则推断 1 项；不配备* 需回看来源后再引用。");
    expect(simpleConclusion.textContent).toContain("查看推断范围");
    expect(screen.getByRole("button", { name: "聚焦 Premium-FWD 的表格范围：查看推断范围，需核对推断" })).toBeTruthy();
    expect(screen.queryByLabelText("Premium-FWD 业务解读")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 简洁业务重点")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务结论")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 结论草稿")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 身份与来源口径")).toBeNull();
    expect(screen.queryByLabelText("Premium-FWD 业务动作建议")).toBeNull();
  });

  it("separates cumulative target differences from table feature rows in simple mode", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareDataWithLuxuryTarget()}
        baseTrimId="comfort"
        mode="simple"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText(/基准列 Comfort-FWD；当前对比 2 个目标配置列，目标累计 4，表格差异行 2 行/)).toBeTruthy();
    const excelGuide = screen.getByLabelText("Excel 对比导读");
    expect(excelGuide.textContent).toContain("目标累计差异");
    expect(excelGuide.textContent).toContain("目标累计 4");
    expect(excelGuide.textContent).toContain("表格差异行 2 行");
    const simpleConclusion = screen.getByLabelText("Excel 列对比结果");
    expect(simpleConclusion.textContent).toContain("Comfort-FWD 是基准列；2 个目标配置列，目标累计 4，表格差异行 2 行");
    const scopeBridge = screen.getByLabelText("简易模式表格与摘要关系");
    expect(scopeBridge.textContent).toContain("表格范围全部配置行");
    expect(scopeBridge.textContent).toContain("差异提炼目标累计 4");
    expect(scopeBridge.textContent).toContain("目标配置列全部目标配置列");
    expect(scopeBridge.textContent).toContain("2 个目标配置列一起汇总。");
    expect(scopeBridge.textContent).toContain("重点目标Premium-FWD 等 2 个");
    expect(scopeBridge.textContent).toContain("并列最多 2 项");
    expect(scopeBridge.textContent).toContain("泊车辅助 2：360 round view camera / 360度高清全景影像、+1");
    expect(scopeBridge.textContent).toContain("音响系统 2：Speaker count / 扬声器数量、+1");
    fireEvent.click(within(scopeBridge).getByRole("button", { name: "查看重点目标差异行：Premium-FWD 等 2 个" }));
    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "premium");

    fireEvent.click(within(excelGuide).getByRole("button", { name: "查看 Excel 对比导读：目标累计差异，目标累计 4" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", null);
  });

  it("states that the simple guide is already narrowed when viewing difference rows", () => {
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        deltaFilter="DIFFERENCE"
        onFocusDeltaType={vi.fn()}
        onOpenEvidence={vi.fn()}
      />,
    );

    const excelGuide = screen.getByLabelText("Excel 对比导读");
    expect(excelGuide.textContent).toContain("当前表格");
    expect(excelGuide.textContent).toContain("差异行");
    expect(excelGuide.textContent).toContain("涉及 2 行配置；当前已按差异行收窄，点“恢复全部配置行”可回到完整 xlsx 表。");
    expect(excelGuide.textContent).not.toContain("点击可切到差异项，完整表格仍可恢复。");
    expect(excelGuide.textContent).not.toContain("点“切到差异项”才会收窄表格。");
  });

  it("lets the simple mode target conclusion card focus the matching table scope", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的表格范围：查看推断范围，需核对推断" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("INFERRED", "premium");
    expect(screen.queryByRole("button", { name: "聚焦 Premium-FWD 的 增配重点" })).toBeNull();
    expect(screen.queryByRole("button", { name: "聚焦 Premium-FWD 的 参数变化" })).toBeNull();
    expect(screen.queryByRole("button", { name: "聚焦 Premium-FWD 的 证据提示" })).toBeNull();
  });

  it("shows simple mode upgrade clues for mutually exclusive feature rows", () => {
    render(
      <BusinessSummaryPanel
        data={compareDataWithMutuallyExclusiveAudio}
        baseTrimId="comfort"
        mode="simple"
        targetTrimFilterId="premium"
        onOpenEvidence={vi.fn()}
      />,
    );

    const simpleConclusion = screen.getByLabelText("Excel 列对比结果");
    expect(simpleConclusion.textContent).toContain("Premium-FWD新增 1、减少 1，集中在 音响系统 2。");
    const points = screen.getByLabelText("Premium-FWD 业务差异要点");
    expect(points.textContent).toContain("升级线索");
    expect(points.textContent).toContain("音响系统：6 speakers / 6扬声器 → SONY 8 speakers / SONY 8扬声器");
    expect(points.textContent).toContain("主要增加");
    expect(points.textContent).toContain("SONY 8 speakers / SONY 8扬声器");
    expect(points.textContent).toContain("主要减少");
    expect(points.textContent).toContain("6 speakers / 6扬声器");
  });

  it("keeps simple mode target-column focus distinct from row filtering", () => {
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        mode="simple"
        targetTrimFilterId="premium"
        onFocusDeltaType={vi.fn()}
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("目标 Premium-FWD Excel 对比导读")).toBeTruthy();
    expect(screen.getByText(/基准列 Comfort-FWD；当前查看 目标 Premium-FWD，当前对比 1 个目标配置列/)).toBeTruthy();
    const scopeBridge = screen.getByLabelText("简易模式表格与摘要关系");
    expect(scopeBridge.textContent).toContain("表格范围全部配置行");
    expect(scopeBridge.textContent).toContain("目标配置列Premium-FWD");
    expect(scopeBridge.textContent).toContain("只聚焦目标配置列，不减少配置行。");
  });

  it("lets business focus groups drill into delta scope without applying a source category", () => {
    const onFocusDeltaType = vi.fn();
    const onFocusCategory = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusCategory={onFocusCategory}
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的业务重点：泊车辅助" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("ADDED", "premium");
    expect(onFocusCategory).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的业务重点：音响系统" }));

    expect(onFocusDeltaType).toHaveBeenLastCalledWith("VALUE_CHANGED", "premium");
    expect(onFocusCategory).not.toHaveBeenCalled();
  });

  it("keeps extra business focus groups discoverable and actionable", () => {
    const dataWithManyFocusGroups: CompareResponse = {
      trims: compareData.trims,
      rows: [
        swapFeatureRow("rear_camera", "Rear Visual parking assist / 动态辅助线倒车影像", "驾驶辅助 Drive assist", "premium"),
        swapFeatureRow("front_seat_heating", "Front seat heating / 前排座椅加热", "舒适便利 Comfort&Convenient", "premium"),
        swapFeatureRow("center_screen", "12.3 inch screen / 12.3寸中控屏", "信息娱乐 Information&Entertainment", "premium"),
        swapFeatureRow("side_airbag", "Side airbag / 侧气囊", "安全 Safety", "premium"),
        swapFeatureRow("led_headlight", "LED headlight / LED大灯", "外饰 Exterior", "premium"),
        swapFeatureRow("motor_power", "Motor power / 电机功率", "性能 Performance", "premium"),
      ],
      groups: [],
      totalFeatures: 6,
      shownFeatures: 6,
    };
    const onFocusDeltaType = vi.fn();
    const onFocusCategory = vi.fn();
    const { container } = render(
      <BusinessSummaryPanel
        data={dataWithManyFocusGroups}
        baseTrimId="comfort"
        onFocusCategory={onFocusCategory}
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    const focusGroups = screen.getByLabelText("Premium-FWD 业务重点分组");
    expect(focusGroups.textContent).toContain("6 个维度");
    expect(container.querySelectorAll(".business-summary-focus-groups__items:not(.business-summary-focus-groups__items--nested) > .business-summary-focus-group")).toHaveLength(4);
    const more = container.querySelector(".business-summary-focus-groups__more") as HTMLDetailsElement | null;
    expect(more).toBeTruthy();
    expect(more?.open).toBe(false);
    expect(more?.querySelectorAll(".business-summary-focus-group")).toHaveLength(2);

    fireEvent.click(screen.getByText("展开 2 个业务重点"));

    expect(more?.open).toBe(true);
    const hiddenButton = more?.querySelector("button.business-summary-focus-group") as HTMLButtonElement | null;
    expect(hiddenButton).toBeTruthy();
    fireEvent.click(hiddenButton as HTMLButtonElement);

    expect(onFocusDeltaType).toHaveBeenCalledWith("ADDED", "premium");
    expect(onFocusCategory).not.toHaveBeenCalled();
  });

  it("shows target identity and source context before business actions", () => {
    const dataWithIdentityContext: CompareResponse = {
      ...compareData,
      trims: [
        {
          ...compareData.trims[0],
          market: "EU",
          modelYear: "2025",
          materialNo: "MM001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: "own-config.xlsx",
        },
        {
          ...compareData.trims[1],
          market: "UK",
          modelYear: null,
          materialNo: null,
          salesVersion: "SV-PREM",
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          sourceFileName: "competitor-site.pdf",
        },
      ],
    };

    render(<BusinessSummaryPanel data={dataWithIdentityContext} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    const context = screen.getByLabelText("Premium-FWD 身份与来源口径");
    expect(context.textContent).toContain("对比身份");
    expect(context.textContent).toContain("本品 → 竞品 / 外部");
    expect(context.textContent).toContain("基准 物料号 MM001；目标 Sales version SV-PREM");
    expect(context.textContent).toContain("市场 / 年款");
    expect(context.textContent).toContain("跨市场 · 年款待补");
    expect(context.textContent).toContain("市场 EU → UK；年款 2025 → 目标待补");
    expect(context.textContent).toContain("来源口径");
    expect(context.textContent).toContain("跨来源");
    expect(context.textContent).toContain("来源 own-config.xlsx → competitor-site.pdf");
  });

  it("keeps cross-source competitor deltas in review instead of marking them ready for wording", () => {
    const cleanDifferenceRows = compareData.rows.map((row) => ({
      ...row,
      values: row.values.map((value) => value ? ({
        ...value,
        displayValue: value.displayValue === "不配备*" ? "不配备" : value.displayValue,
        inferred: false,
      }) : null),
    }));
    const crossSourceData: CompareResponse = {
      ...compareData,
      trims: [
        {
          ...compareData.trims[0],
          market: "EU",
          modelYear: "2025",
          materialNo: "MM001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: "own-config.xlsx",
        },
        {
          ...compareData.trims[1],
          market: "EU",
          modelYear: "2025",
          materialNo: null,
          salesVersion: "SV-PREM",
          hasMaterialNo: false,
          dataOrigin: "external_or_scraped",
          sourceFileName: "competitor-site.pdf",
        },
      ],
      rows: cleanDifferenceRows,
    };

    const onOpenSourceContext = vi.fn();
    render(
      <BusinessSummaryPanel
        data={crossSourceData}
        baseTrimId="comfort"
        onOpenSourceContext={onOpenSourceContext}
        onOpenEvidence={vi.fn()}
      />,
    );

    const targetAction = screen.getByLabelText("Premium-FWD 业务动作建议");
    expect(targetAction.textContent).toContain("跨来源");
    expect(targetAction.textContent).toContain("先核对来源一致性");
    expect(targetAction.textContent).toContain("同国家同车型在不同网站或文件中的配置可能不一致");
    expect(targetAction.textContent).not.toContain("可转话术");
    const baseStoryline = screen.getByLabelText("基准配置列差异脉络");
    expect(baseStoryline.textContent).toContain("配置列口径");
    expect(baseStoryline.textContent).toContain("本品与外部配置列");
    expect(baseStoryline.textContent).toContain("无需先选本品 / 竞品模式");
    expect(baseStoryline.textContent).toContain("竞品 / 外部 1");
    expect(baseStoryline.textContent).toContain("跨来源 1");

    fireEvent.click(screen.getByRole("button", { name: "打开基准叙事来源：配置列口径" }));

    expect(onOpenSourceContext).toHaveBeenCalledWith("premium");

    fireEvent.click(screen.getByRole("button", { name: "打开 Premium-FWD 的来源入口：跨来源" }));

    expect(onOpenSourceContext).toHaveBeenLastCalledWith("premium");
  });

  it("passes same-market same-year multi-source boundaries into the AI summary payload", async () => {
    vi.mocked(api.composeEngineeringConfigBusinessSummary).mockResolvedValueOnce({
      summaries: [
        {
          targetTrimId: "premium",
          targetLabel: "Premium-FWD",
          headline: "Premium 相比 Comfort 的 AI 摘要需要保留来源口径。",
          mainUpgrades: ["配置差异需按来源核对"],
          replacementsOrReductions: [],
          evidenceStatus: ["同国家同年款多来源，引用前核对 source evidence"],
          evidenceRefs: [],
          recommendedUse: "来源口径核对后再引用。",
        },
      ],
      usage: {
        provider: "deepseek",
        model: "deepseek-chat",
        status: "ok",
        promptTokens: 10,
        completionTokens: 10,
        totalTokens: 20,
      },
    });
    const crossSourceData: CompareResponse = {
      ...compareData,
      trims: [
        {
          ...compareData.trims[0],
          market: "Germany",
          modelYear: "2026",
          sourceCreatedBy: "alice",
          sourceFileName: "dealer-config.xlsx",
        },
        {
          ...compareData.trims[1],
          market: "Germany",
          modelYear: "2026",
          sourceCreatedBy: "bob",
          sourceFileName: "brand-site.html",
        },
      ],
    };

    render(
      <BusinessSummaryPanel
        data={crossSourceData}
        baseTrimId="comfort"
        llmSummaryEnabled
        onOpenEvidence={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(api.composeEngineeringConfigBusinessSummary).toHaveBeenCalledTimes(1);
    });
    const composePayload = vi.mocked(api.composeEngineeringConfigBusinessSummary).mock.calls[0][0];
    expect(composePayload).toEqual({
      trimIds: ["comfort", "premium"],
      baseTrimId: "comfort",
      versionScope: "published",
      filters: {
        deltaFilter: "ALL",
        category: null,
        search: null,
        targetTrimId: "premium",
      },
    });
    expect(composePayload).not.toHaveProperty("context");
  });

  it("asks for source context before wording when a clean delta has no trim source", () => {
    const cleanDifferenceRows = compareData.rows.map((row) => ({
      ...row,
      values: row.values.map((value) => value ? ({
        ...value,
        displayValue: value.displayValue === "不配备*" ? "不配备" : value.displayValue,
        inferred: false,
      }) : null),
    }));
    const missingSourceData: CompareResponse = {
      ...compareData,
      trims: [
        {
          ...compareData.trims[0],
          market: "EU",
          modelYear: "2025",
          materialNo: "MM001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: "own-config.xlsx",
        },
        {
          ...compareData.trims[1],
          market: "EU",
          modelYear: "2025",
          materialNo: "MM002",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: null,
        },
      ],
      rows: cleanDifferenceRows,
    };

    const onOpenSourceContext = vi.fn();
    render(
      <BusinessSummaryPanel
        data={missingSourceData}
        baseTrimId="comfort"
        onOpenSourceContext={onOpenSourceContext}
        onOpenEvidence={vi.fn()}
      />,
    );

    const targetAction = screen.getByLabelText("Premium-FWD 业务动作建议");
    expect(targetAction.textContent).toContain("补来源口径");
    expect(targetAction.textContent).toContain("来源不足，先别转结论");
    expect(targetAction.textContent).toContain("基准列或目标配置列缺少来源文件");
    expect(targetAction.textContent).not.toContain("可转话术");

    fireEvent.click(screen.getByRole("button", { name: "打开 Premium-FWD 的来源入口：补来源口径" }));

    expect(onOpenSourceContext).toHaveBeenCalledWith("premium");
  });

  it("lets category chips focus a business category when a handler is provided", () => {
    const onFocusCategory = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusCategory={onFocusCategory}
        onOpenEvidence={vi.fn()}
      />,
    );
    const categoryButton = screen.getByRole("button", {
      name: "聚焦 Premium-FWD 的 驾驶辅助 Drive assist 差异大类，目标差异 1 项",
    });

    fireEvent.click(categoryButton);

    expect(onFocusCategory).toHaveBeenCalledWith("驾驶辅助 Drive assist");
  });

  it("lets target insight chips focus the matching delta type and target trim", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 增配重点" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("ADDED", "premium");

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 参数变化" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("VALUE_CHANGED", "premium");
  });

  it("lets target overview cards focus the target difference scope", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的基准对比摘要" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "premium");
  });

  it("shows a base-to-target version ladder when multiple target trims are selected", () => {
    const value = (trimId: string, featureCode: string, available: boolean): CompareValue => ({
      valueId: `${trimId}-${featureCode}`,
      rawValue: available ? "●" : "",
      normalizedValue: available ? "standard" : null,
      availability: available ? "STANDARD" : "NOT_AVAILABLE",
      unit: null,
      valueState: available ? "marker_value" : "blank",
      displayValue: available ? "标配" : "不配备",
      inferred: false,
      source: null,
    });
    const row = (featureCode: string, featureName: string, category: string, premiumAvailable: boolean, luxuryAvailable: boolean): CompareRow => ({
      category,
      featureCode,
      featureName,
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: [premiumAvailable ? "premium" : null, luxuryAvailable ? "luxury" : null].filter((item): item is string => Boolean(item)),
      businessNote: "版本阶梯差异",
      values: [
        value("comfort", featureCode, false),
        value("premium", featureCode, premiumAvailable),
        value("luxury", featureCode, luxuryAvailable),
      ],
    });
    const ladderData: CompareResponse = {
      trims: [
        compareData.trims[0],
        compareData.trims[1],
        {
          trimId: "luxury",
          fullTrimName: "两驱豪华型 Luxury-FWD",
          brand: "OMODA",
          modelName: "T19C MY ICE",
          trimName: "Luxury-FWD",
        },
      ],
      rows: [
        row("camera_360", "360 round view camera / 360度高清全景影像", "驾驶辅助 Drive assist", true, true),
        row("seat_heat", "Front seat heating / 前排座椅加热", "舒适便利 Comfort&Convenient", true, true),
        row("sony_audio", "SONY 8 speakers / SONY 8扬声器", "信息娱乐 Information&Entertainment", false, true),
      ],
      groups: [],
      totalFeatures: 3,
      shownFeatures: 3,
    };
    const onFocusDeltaType = vi.fn();
    const onFocusVersionStep = vi.fn();

    render(
      <BusinessSummaryPanel
        data={ladderData}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onFocusVersionStep={onFocusVersionStep}
        onOpenEvidence={vi.fn()}
      />,
    );

    const ladder = screen.getByLabelText("当前基准对比速览");
    expect(ladder.textContent).toContain("当前基准对比速览");
    expect(ladder.textContent).toContain("Base · Comfort-FWD");
    expect(ladder.textContent).toContain("Target 1 · Premium-FWD");
    expect(ladder.textContent).toContain("新增 2 项");
    expect(ladder.textContent).toContain("Target 2 · Luxury-FWD");
    expect(ladder.textContent).toContain("新增 3 项");
    const upgradePath = screen.getByLabelText("相邻版本升级路径");
    expect(upgradePath.textContent).toContain("Comfort-FWD → Premium-FWD");
    expect(upgradePath.textContent).toContain("新增 2 项");
    expect(upgradePath.textContent).toContain("Premium-FWD → Luxury-FWD");
    expect(upgradePath.textContent).toContain("新增 1 项");

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Luxury-FWD 的基准对比摘要" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "luxury");

    fireEvent.click(screen.getByRole("button", { name: "查看 Premium-FWD 到 Luxury-FWD 的相邻版本差异" }));

    expect(onFocusVersionStep).toHaveBeenCalledWith("premium", "luxury", "DIFFERENCE");
  });

  it("keeps adjacent upgrade steps in selected trim order when the active base is in the middle", () => {
    const value = (trimId: string, featureCode: string, available: boolean): CompareValue => ({
      valueId: `${trimId}-${featureCode}`,
      rawValue: available ? "●" : "",
      normalizedValue: available ? "standard" : null,
      availability: available ? "STANDARD" : "NOT_AVAILABLE",
      unit: null,
      valueState: available ? "marker_value" : "blank",
      displayValue: available ? "标配" : "不配备",
      inferred: false,
      source: null,
    });
    const stableOrderData: CompareResponse = {
      trims: [
        {
          trimId: "basic",
          fullTrimName: "两驱基本型 Basic-FWD",
          brand: "OMODA",
          modelName: "T19C MY ICE",
          trimName: "Basic-FWD",
        },
        compareData.trims[0],
        compareData.trims[1],
      ],
      rows: [
        {
          category: "驾驶辅助 Drive assist",
          featureCode: "camera_360",
          featureName: "360 round view camera / 360度高清全景影像",
          comparisonType: "UNIQUE_OR_PARTIAL",
          uniqueTrimIds: ["comfort", "premium"],
          businessNote: "版本阶梯差异",
          values: [
            value("basic", "camera_360", false),
            value("comfort", "camera_360", true),
            value("premium", "camera_360", true),
          ],
        },
        {
          category: "舒适便利 Comfort&Convenient",
          featureCode: "seat_heat",
          featureName: "Front seat heating / 前排座椅加热",
          comparisonType: "UNIQUE_OR_PARTIAL",
          uniqueTrimIds: ["premium"],
          businessNote: "版本阶梯差异",
          values: [
            value("basic", "seat_heat", false),
            value("comfort", "seat_heat", false),
            value("premium", "seat_heat", true),
          ],
        },
      ],
      groups: [],
      totalFeatures: 2,
      shownFeatures: 2,
    };
    const onFocusVersionStep = vi.fn();

    render(
      <BusinessSummaryPanel
        data={stableOrderData}
        baseTrimId="comfort"
        onFocusVersionStep={onFocusVersionStep}
        onOpenEvidence={vi.fn()}
      />,
    );

    const upgradePath = screen.getByLabelText("相邻版本升级路径");
    const basicToComfortIndex = upgradePath.textContent?.indexOf("Basic-FWD → Comfort-FWD") ?? -1;
    const comfortToPremiumIndex = upgradePath.textContent?.indexOf("Comfort-FWD → Premium-FWD") ?? -1;
    expect(basicToComfortIndex).toBeGreaterThanOrEqual(0);
    expect(comfortToPremiumIndex).toBeGreaterThan(basicToComfortIndex);
    expect(upgradePath.textContent).not.toContain("Comfort-FWD → Basic-FWD");

    fireEvent.click(screen.getByRole("button", { name: "查看 Basic-FWD 到 Comfort-FWD 的相邻版本差异" }));

    expect(onFocusVersionStep).toHaveBeenCalledWith("basic", "comfort", "DIFFERENCE");
  });

  it("keeps the target trim when target insight chips focus common or evidence scopes", () => {
    const onFocusDeltaType = vi.fn();
    const mergedData: CompareResponse = {
      ...compareDataWithCommon,
      rows: compareDataWithCommon.rows.map((row) => (
        row.featureCode === "country"
          ? {
            ...row,
            values: row.values.map((value, index) => {
              if (!value) return null;
              return {
                ...value,
                source: {
                  sheetName: "T19C MY ICE",
                  rowNumber: 11,
                  columnNumber: index + 4,
                  columnLetter: index === 0 ? "D" : "E",
                  cell: index === 0 ? "D11" : "E11",
                  sourceCell: "D11",
                  mergedRange: "D11:E11",
                },
              };
            }),
          }
          : row
      )),
    };
    const { rerender } = render(
      <BusinessSummaryPanel
        data={compareDataWithCommon}
        baseTrimId="comfort"
        deltaFilter="COMMON"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 共同基线" }));

    expect(onFocusDeltaType).toHaveBeenLastCalledWith("COMMON", "premium");

    rerender(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        deltaFilter="MISSING_SOURCE"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 证据提示" }));

    expect(onFocusDeltaType).toHaveBeenLastCalledWith("MISSING_SOURCE", "premium");

    rerender(
      <BusinessSummaryPanel
        data={mergedData}
        baseTrimId="comfort"
        deltaFilter="MERGED_SOURCE"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 证据提示" }));

    expect(onFocusDeltaType).toHaveBeenLastCalledWith("MERGED_SOURCE", "premium");
  });

  it("summarizes mutually exclusive feature rows as an upgrade clue", () => {
    const onFocusDeltaType = vi.fn();
    const onFocusCategory = vi.fn();
    const onOpenEvidence = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareDataWithMutuallyExclusiveAudio}
        baseTrimId="comfort"
        targetTrimFilterId="premium"
        onFocusDeltaType={onFocusDeltaType}
        onFocusCategory={onFocusCategory}
        onOpenEvidence={onOpenEvidence}
      />,
    );

    const targetBrief = screen.getByLabelText("Premium-FWD 业务解读");
    expect(targetBrief.textContent).toContain("升级线索 音响系统：6 speakers / 6扬声器 → SONY 8 speakers / SONY 8扬声器");
    expect(targetBrief.textContent).toContain("主要增加 1项 · 音响系统：SONY 8 speakers / SONY 8扬声器");
    expect(targetBrief.textContent).toContain("减少 1项 · 音响系统：6 speakers / 6扬声器");

    const targetInsight = screen.getByLabelText("Premium-FWD 业务结论");
    expect(targetInsight.textContent).toContain("升级线索");
    expect(targetInsight.textContent).toContain("音响系统：6 speakers / 6扬声器 → SONY 8 speakers / SONY 8扬声器");
    expect(screen.getAllByText("升级线索").length).toBeGreaterThan(1);
    const focusGroups = screen.getByLabelText("Premium-FWD 业务重点分组");
    expect(focusGroups.textContent).toContain("音响系统");
    expect(focusGroups.textContent).toContain("新增 1 项 · 减少 1 项");
    expect(focusGroups.textContent).toContain("SONY 8 speakers / SONY 8扬声器");
    expect(screen.getAllByText("音响系统").length).toBeGreaterThan(1);

    fireEvent.click(screen.getByRole("button", {
      name: "聚焦 Premium-FWD 的音响系统升级范围：6 speakers / 6扬声器 → SONY 8 speakers / SONY 8扬声器",
    }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "premium");
    expect(onFocusCategory).toHaveBeenCalledWith("信息娱乐 Information&Entertainment");

    fireEvent.click(screen.getByRole("button", {
      name: "查看 Comfort-FWD 的音响系统旧配置来源：6 speakers / 6扬声器",
    }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "audio_6_speakers" }),
      trim: expect.objectContaining({ trimId: "comfort" }),
      selectionReason: expect.stringContaining("旧配置来源"),
    }));

    fireEvent.click(screen.getByRole("button", {
      name: "查看 Premium-FWD 的音响系统新配置来源：SONY 8 speakers / SONY 8扬声器",
    }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "sony_8_speakers" }),
      trim: expect.objectContaining({ trimId: "premium" }),
      selectionReason: expect.stringContaining("新配置来源"),
    }));

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 升级线索" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "premium");
  });

  it("keeps steering wheel features out of wheel and tire grouping", () => {
    const steeringWheelData: CompareResponse = {
      trims: compareData.trims,
      rows: [
        swapFeatureRow("steering_wheel_heat", "Steering wheel heat / 方向盘加热", "外饰 Exterior", "premium"),
      ],
      groups: [],
      totalFeatures: 1,
      shownFeatures: 1,
    };

    render(
      <BusinessSummaryPanel
        data={steeringWheelData}
        baseTrimId="comfort"
        onOpenEvidence={vi.fn()}
      />,
    );

    const focusGroups = screen.getByLabelText("Premium-FWD 业务重点分组");
    expect(focusGroups.textContent).toContain("方向盘配置");
    expect(focusGroups.textContent).toContain("Steering wheel heat / 方向盘加热");
    expect(focusGroups.textContent).not.toContain("轮胎 / 轮毂");
  });

  it("lets users expand hidden upgrade clues with the same audit actions", () => {
    const onFocusDeltaType = vi.fn();
    const onFocusCategory = vi.fn();
    const onOpenEvidence = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareDataWithManyUpgradeInsights}
        baseTrimId="comfort"
        targetTrimFilterId="premium"
        onFocusDeltaType={onFocusDeltaType}
        onFocusCategory={onFocusCategory}
        onOpenEvidence={onOpenEvidence}
      />,
    );

    const expandControl = screen.getByText("展开 1 条升级线索");
    const details = expandControl.closest("details") as HTMLDetailsElement | null;
    expect(details?.open).toBe(false);

    fireEvent.click(expandControl);

    expect(details?.open).toBe(true);
    expect(screen.getAllByRole("button", { name: /升级范围/ })).toHaveLength(4);

    const checkButtons = screen.getAllByRole("button", { name: /升级范围/ });
    fireEvent.click(checkButtons[3]);

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "premium");
    expect(onFocusCategory).toHaveBeenCalled();

    const newSourceButtons = screen.getAllByRole("button", { name: /新配置来源/ });
    fireEvent.click(newSourceButtons[3]);

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      trim: expect.objectContaining({ trimId: "premium" }),
      selectionReason: expect.stringContaining("新配置来源"),
    }));
  });

  it("lets business action guidance focus the evidence work", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的 业务动作建议：核对推断" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("INFERRED", "premium");
  });

  it("lets conclusion status chips focus the matching target evidence scope", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦结论状态：需核对推断 1，Premium-FWD；先看 Premium-FWD" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("INFERRED", "premium");
  });

  it("lets a focused target continue to the next target in the same queue", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareDataWithLuxuryTarget()}
        baseTrimId="comfort"
        deltaFilter="INFERRED"
        targetTrimFilterId="premium"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    const targetQueue = screen.getByLabelText("目标处理队列");
    expect(targetQueue.textContent).toContain("规则推断目标队列 1/2");
    expect(targetQueue.textContent).toContain("下一个 Luxury-FWD");

    fireEvent.click(screen.getByRole("button", { name: "切到下一个规则推断目标：Luxury-FWD" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("INFERRED", "luxury");
  });

  it("lets the base storyline focus aggregate evidence work", () => {
    const onFocusDeltaType = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "聚焦基准叙事：证据边界" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("INFERRED", null);
  });

  it("keeps target feature evidence details collapsed until a target is focused", () => {
    const { container, rerender } = render(
      <BusinessSummaryPanel data={compareData} baseTrimId="comfort" onOpenEvidence={vi.fn()} />,
    );
    const detail = container.querySelector(".business-summary-detail") as HTMLDetailsElement | null;

    expect(detail).toBeTruthy();
    expect(detail?.open).toBe(false);
    expect(detail?.textContent).toContain("差异明细");
    expect(detail?.textContent).toContain("展开 / 收起来源样本");
    expect(detail?.textContent).toContain("2 项");

    rerender(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        targetTrimFilterId="premium"
        onOpenEvidence={vi.fn()}
      />,
    );

    const focusedDetail = container.querySelector(".business-summary-detail") as HTMLDetailsElement | null;
    expect(focusedDetail?.open).toBe(true);
  });

  it("labels target trim focus actions with the trim name", () => {
    const onFocusTargetTrim = vi.fn();
    const { rerender } = render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusTargetTrim={onFocusTargetTrim}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "从业务摘要聚焦 Premium-FWD 差异" }));

    expect(onFocusTargetTrim).toHaveBeenCalledWith("premium");

    rerender(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        targetTrimFilterId="premium"
        onFocusTargetTrim={onFocusTargetTrim}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "取消业务摘要中 Premium-FWD 目标聚焦" }));

    expect(onFocusTargetTrim).toHaveBeenCalledWith(null);
  });

  it("shows a baseline narrative for strongest target, category focus, and evidence status", () => {
    render(<BusinessSummaryPanel data={compareData} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    expect(screen.getByText("基准配置")).toBeTruthy();
    expect(screen.getAllByText("Comfort-FWD").length).toBeGreaterThan(0);
    expect(screen.getByText("差异最大")).toBeTruthy();
    expect(screen.getByText("Premium-FWD · 2")).toBeTruthy();
    expect(screen.getAllByText("集中维度").length).toBeGreaterThan(0);
    expect(screen.getAllByText("泊车辅助 1 · 音响系统 1").length).toBeGreaterThan(0);
    expect(screen.getByText("证据状态")).toBeTruthy();
    expect(screen.getByText("规则推断差异 1")).toBeTruthy();
    expect(screen.getByText("按目标配置列口径统计；不配备* 不是 Excel 原文。")).toBeTruthy();
  });

  it("lets baseline concentrated category chips focus the table category", () => {
    const onFocusCategory = vi.fn();
    const { container } = render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onFocusCategory={onFocusCategory}
        onOpenEvidence={vi.fn()}
      />,
    );
    const baselineCategoryButton = Array.from(container.querySelectorAll(".business-summary-baseline-action"))
      .find((element) => element.textContent?.includes("信息娱乐 Information&Entertainment"));

    expect(baselineCategoryButton).toBeTruthy();
    fireEvent.click(baselineCategoryButton as HTMLElement);

    expect(onFocusCategory).toHaveBeenCalledWith("信息娱乐 Information&Entertainment");
  });

  it("scopes the business narrative and metrics to the selected category", () => {
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        categoryFilter="驾驶辅助 Drive assist"
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("驾驶辅助 Drive assist 业务摘要")).toBeTruthy();
    expect(screen.getByText(/当前聚焦 驾驶辅助 Drive assist，当前对比 1 个目标配置列，累计 1 个目标差异/)).toBeTruthy();
    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD 在 驾驶辅助 Drive assist：新增 1 项，其中规则推断 1 项/)).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "1新增配置")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "0值变化")).toBeTruthy();
    expect(screen.queryByText("Speaker count / 扬声器数量")).toBeNull();
  });

  it("scopes the business narrative and metrics to the selected delta filter", () => {
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        deltaFilter="ADDED"
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("新增配置 差异摘要")).toBeTruthy();
    expect(screen.getByText(/当前聚焦 新增配置，当前对比 1 个目标配置列，累计 1 个目标差异/)).toBeTruthy();
    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD 在 新增配置：新增 1 项，其中规则推断 1 项/)).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "1新增配置")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "0值变化")).toBeTruthy();
    expect(screen.queryByText("Speaker count / 扬声器数量")).toBeNull();
  });

  it("labels the common-config scope as a non-difference summary", () => {
    const onOpenEvidence = vi.fn();
    const onFocusTargetTrim = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareDataWithCommon}
        baseTrimId="comfort"
        deltaFilter="COMMON"
        onFocusTargetTrim={onFocusTargetTrim}
        onOpenEvidence={onOpenEvidence}
      />,
    );

    expect(screen.getByText("共同配置 摘要")).toBeTruthy();
    expect(screen.getByLabelText("摘要统计口径").textContent).toContain("当前统计共同配置行，不计入业务差异结论。");
    expect(screen.queryByLabelText("当前基准对比速览")).toBeNull();
    expect(screen.getByLabelText("Premium-FWD 业务解读").textContent).toContain("Premium-FWD 与 Comfort-FWD 在当前范围保持一致：1项 · 基本参数：Country / 国家。点击共同配置可核对来源。");
    expect(screen.getByLabelText("Premium-FWD 业务结论").textContent).toContain("共同基线1项 · 基本参数：Country / 国家");
    expect(screen.queryByRole("button", { name: "聚焦 Premium-FWD 的 共同基线" })).toBeNull();
    expect(screen.getByText(/当前聚焦 共同配置，当前对比 1 个目标配置列，当前范围包含 1 行共同配置/)).toBeTruthy();
    expect(screen.getByText(/Premium-FWD 与 Comfort-FWD 在 共同配置：共同配置 1 项/)).toBeTruthy();
    expect(screen.getAllByText("共同配置").length).toBeGreaterThan(0);
    expect(screen.getAllByText((_content, element) => element?.textContent === "1共同配置").length).toBeGreaterThan(0);
    expect(screen.getByText("按配置行去重；跨目标配置列共有 1 条一致判断。")).toBeTruthy();
    expect(screen.getByText("共同配置行")).toBeTruthy();
    expect(screen.getAllByText("Country / 国家").length).toBeGreaterThan(0);
    expect(screen.queryByText("暂无新增配置")).toBeNull();

    const focusButton = screen.getByRole("button", { name: "查看 Premium-FWD 相对基准的差异" });
    expect(focusButton.textContent).toBe("查看差异");
    fireEvent.click(focusButton);

    expect(onFocusTargetTrim).toHaveBeenCalledWith("premium");

    fireEvent.click(screen.getByRole("button", { name: "查看 Premium-FWD Country / 国家 的共同配置来源" }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "country" }),
      selectionReason: "该配置在 Premium-FWD 与 Comfort-FWD 中保持一致。",
    }));
  });

  it("lets users expand hidden common config evidence", () => {
    const commonRows: CompareResponse["rows"] = Array.from({ length: 5 }, (_, index) => ({
      category: "基本参数",
      featureCode: `common_feature_${index + 1}`,
      featureName: `Common feature ${index + 1}`,
      comparisonType: "COMMON_SAME",
      uniqueTrimIds: [],
      businessNote: "共同配置",
      values: [
        {
          valueId: `comfort-common-${index + 1}`,
          rawValue: "same",
          normalizedValue: "same",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "same",
          inferred: false,
          source: null,
        },
        {
          valueId: `premium-common-${index + 1}`,
          rawValue: "same",
          normalizedValue: "same",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "same",
          inferred: false,
          source: null,
        },
      ],
    }));
    const dataWithManyCommon: CompareResponse = {
      ...compareData,
      rows: [
        ...compareData.rows,
        ...commonRows,
      ],
      totalFeatures: compareData.rows.length + commonRows.length,
      shownFeatures: compareData.rows.length + commonRows.length,
    };
    const onOpenEvidence = vi.fn();

    render(
      <BusinessSummaryPanel
        data={dataWithManyCommon}
        baseTrimId="comfort"
        deltaFilter="COMMON"
        onOpenEvidence={onOpenEvidence}
      />,
    );

    const expandControl = screen.getByText("展开 2 项共同配置");
    const details = expandControl.closest("details") as HTMLDetailsElement | null;
    expect(details?.open).toBe(false);

    fireEvent.click(expandControl);

    expect(details?.open).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "查看 Premium-FWD Common feature 4 的共同配置来源" }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "common_feature_4" }),
      trim: expect.objectContaining({ trimId: "premium" }),
      selectionReason: "该配置在 Premium-FWD 与 Comfort-FWD 中保持一致。",
    }));
  });

  it("labels the all-differences scope without repeating difference wording", () => {
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        deltaFilter="DIFFERENCE"
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("差异项 摘要")).toBeTruthy();
    expect(screen.queryByText("差异项 差异摘要")).toBeNull();
  });

  it("labels pending scope as evidence work instead of confirmed differences", () => {
    const onShowDifferenceRows = vi.fn();
    const dataWithPending: CompareResponse = {
      ...compareData,
      rows: [
        ...compareData.rows,
        {
          category: "数据质量 Data quality",
          featureCode: "source_pending",
          featureName: "Pending source config / 待确认配置",
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
              valueState: "blank",
              displayValue: "待确认",
              inferred: false,
              source: null,
            },
          ],
        },
      ],
      totalFeatures: 3,
      shownFeatures: 3,
    };

    render(
      <BusinessSummaryPanel
        data={dataWithPending}
        baseTrimId="comfort"
        deltaFilter="UNKNOWN"
        onShowDifferenceRows={onShowDifferenceRows}
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("待确认 摘要")).toBeTruthy();
    expect(screen.getByLabelText("摘要统计口径").textContent).toContain("当前只统计待确认项，空值不会自动当成无配置。");
    expect(screen.queryByText("待确认 差异摘要")).toBeNull();
    expect(screen.getByText(/当前聚焦 待确认，当前对比 1 个目标配置列，当前范围包含 1 个待确认项，涉及 1 行配置/)).toBeTruthy();
    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD 在 待确认：待确认 1 项/)).toBeTruthy();
    expect(screen.getByLabelText("Premium-FWD 业务解读").textContent).toContain("Premium-FWD 相比 Comfort-FWD：待确认 1项 · 数据质量 Data quality：Pending source config / 待确认配置。待确认项需要先补来源证据。");
    const conclusionDraft = screen.getByLabelText("Premium-FWD 结论草稿");
    expect(conclusionDraft.textContent).toContain("暂不引用");
    expect(conclusionDraft.textContent).toContain("待确认项未闭环");
    expect(conclusionDraft.textContent).toContain("先补来源证据或重新消化来源，再输出确定配置结论。");

    fireEvent.click(screen.getByRole("button", { name: "查看待确认项" }));

    expect(onShowDifferenceRows).toHaveBeenCalledTimes(1);
  });

  it("marks clean same-source target differences as a reusable conclusion draft", () => {
    const dataWithReadyContext: CompareResponse = {
      ...compareDataWithMutuallyExclusiveAudio,
      trims: [
        {
          ...compareDataWithMutuallyExclusiveAudio.trims[0],
          market: "EU",
          modelYear: "2025",
          materialNo: "MM001",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: "t19c-config.xlsx",
        },
        {
          ...compareDataWithMutuallyExclusiveAudio.trims[1],
          market: "EU",
          modelYear: "2025",
          materialNo: "MM002",
          hasMaterialNo: true,
          dataOrigin: "own_catalog",
          sourceFileName: "t19c-config.xlsx",
        },
      ],
    };
    const onFocusDeltaType = vi.fn();

    render(
      <BusinessSummaryPanel
        data={dataWithReadyContext}
        baseTrimId="comfort"
        onFocusDeltaType={onFocusDeltaType}
        onOpenEvidence={vi.fn()}
      />,
    );

    const conclusionDraft = screen.getByLabelText("Premium-FWD 结论草稿");
    expect(conclusionDraft.textContent).toContain("可引用初稿");
    expect(conclusionDraft.textContent).toContain("配置差异可转业务话术");
    expect(conclusionDraft.textContent).toContain("初稿：Premium-FWD 相比 Comfort-FWD 升级线索 音响系统：6 speakers / 6扬声器 → SONY 8 speakers / SONY 8扬声器");
    expect(conclusionDraft.textContent).toContain("当前差异状态明确，可继续转成版本卖点、短板或配置层级说明。");

    fireEvent.click(screen.getByRole("button", { name: "聚焦 Premium-FWD 的结论草稿：可引用初稿" }));

    expect(onFocusDeltaType).toHaveBeenCalledWith("DIFFERENCE", "premium");
  });

  it("copies the target conclusion draft with evidence boundaries", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(<BusinessSummaryPanel data={compareData} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "复制 Premium-FWD 的结论草稿" }));

    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copiedText = writeText.mock.calls[0][0];
    expect(copiedText).toContain("Target trim: Premium-FWD");
    expect(copiedText).toContain("Base trim: Comfort-FWD");
    expect(copiedText).toContain("Status: 需核对推断");
    expect(copiedText).toContain("Title: 推断值待来源确认");
    expect(copiedText).toContain("Conclusion: 初稿：Premium-FWD 相比 Comfort-FWD 增加 1 项配置，参数变化 1 项");
    expect(copiedText).toContain("Evidence note: 含规则推断 1 项；不配备* 需回看来源后再引用。");
    expect(await screen.findByText("结论草稿已复制。")).toBeTruthy();
  });

  it("copies all current target conclusion drafts from the summary header", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    const dataWithTwoTargets = compareDataWithLuxuryTarget();

    render(<BusinessSummaryPanel data={dataWithTwoTargets} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    const conclusionStatus = screen.getByLabelText("结论状态汇总");
    expect(conclusionStatus.textContent).toContain("先看 Premium-FWD · 共 2 个");

    fireEvent.click(screen.getByRole("button", { name: "复制当前摘要全部结论草稿" }));

    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copiedText = writeText.mock.calls[0][0];
    expect(copiedText).toContain("Config comparison conclusion drafts");
    expect(copiedText).toContain("Base trim: Comfort-FWD");
    expect(copiedText).toContain("Scope: 全部目标配置列");
    expect(copiedText).toContain("Target count: 2");
    expect(copiedText).toContain("Status summary: 需核对推断 2 (Premium-FWD, Luxury-FWD)");
    expect(copiedText).toContain("First action target: 需核对推断: Premium-FWD");
    expect(copiedText).toContain("Action order: 需核对推断: Premium-FWD -> Luxury-FWD");
    expect(copiedText).toContain("Target 1");
    expect(copiedText).toContain("Target trim: Premium-FWD");
    expect(copiedText).toContain("Target 2");
    expect(copiedText).toContain("Target trim: Luxury-FWD");
    expect(copiedText).toContain("Evidence note: 含规则推断 1 项；不配备* 需回看来源后再引用。");
    expect(await screen.findByText("全部结论草稿已复制。")).toBeTruthy();
  });

  it("shows a local fallback when conclusion draft copy is unavailable", async () => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: undefined,
    });

    render(<BusinessSummaryPanel data={compareData} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "复制 Premium-FWD 的结论草稿" }));

    expect(await screen.findByText("当前浏览器不支持复制，请手动选中结论草稿。")).toBeTruthy();
  });

  it("lets users expand hidden missing-source evidence items", () => {
    const missingRows: CompareResponse["rows"] = Array.from({ length: 5 }, (_, index) => ({
      category: "数据质量 Data quality",
      featureCode: `missing_source_${index + 1}`,
      featureName: `Missing source feature ${index + 1}`,
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: [],
      businessNote: "来源缺失，需要补 evidence。",
      values: [
        {
          valueId: `comfort-missing-${index + 1}`,
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          displayValue: "不配备",
          inferred: false,
          source: null,
        },
        {
          valueId: `premium-missing-${index + 1}`,
          rawValue: "●",
          normalizedValue: "standard",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
          inferred: false,
          source: null,
        },
      ],
    }));
    const dataWithManyMissingSource: CompareResponse = {
      ...compareData,
      rows: missingRows,
      totalFeatures: missingRows.length,
      shownFeatures: missingRows.length,
    };
    const onOpenEvidence = vi.fn();

    render(
      <BusinessSummaryPanel
        data={dataWithManyMissingSource}
        baseTrimId="comfort"
        deltaFilter="MISSING_SOURCE"
        onOpenEvidence={onOpenEvidence}
      />,
    );

    const expandControl = screen.getByText("展开 2 项来源问题");
    const details = expandControl.closest("details") as HTMLDetailsElement | null;
    expect(details?.open).toBe(false);

    fireEvent.click(expandControl);

    expect(details?.open).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "查看 Premium-FWD Missing source feature 4 的证据来源" }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "missing_source_4" }),
      trim: expect.objectContaining({ trimId: "premium" }),
      selectionReason: expect.stringContaining("缺值或缺少来源证据"),
    }));
  });

  it("lets users expand hidden merged-cell evidence items", () => {
    const mergedRows: CompareResponse["rows"] = Array.from({ length: 5 }, (_, index) => {
      const rowNumber = 21 + index;
      return {
        category: "基本参数 Basic parameters",
        featureCode: `merged_source_${index + 1}`,
        featureName: `Merged source feature ${index + 1}`,
        comparisonType: "COMMON_SAME",
        uniqueTrimIds: [],
        businessNote: "横向合并格展开。",
        values: [
          {
            valueId: `comfort-merged-${index + 1}`,
            rawValue: "EU",
            normalizedValue: "eu",
            availability: "VALUE",
            unit: null,
            valueState: "text_value",
            displayValue: "EU",
            inferred: false,
            source: {
              sheetName: "T19C MY ICE",
              rowNumber,
              columnNumber: 4,
              columnLetter: "D",
              cell: `D${rowNumber}`,
              sourceCell: `D${rowNumber}`,
              mergedRange: `D${rowNumber}:F${rowNumber}`,
            },
          },
          {
            valueId: `premium-merged-${index + 1}`,
            rawValue: "EU",
            normalizedValue: "eu",
            availability: "VALUE",
            unit: null,
            valueState: "text_value",
            displayValue: "EU",
            inferred: false,
            source: {
              sheetName: "T19C MY ICE",
              rowNumber,
              columnNumber: 5,
              columnLetter: "E",
              cell: `E${rowNumber}`,
              sourceCell: `D${rowNumber}`,
              mergedRange: `D${rowNumber}:F${rowNumber}`,
            },
          },
        ],
      };
    });
    const dataWithManyMergedSource: CompareResponse = {
      ...compareData,
      rows: mergedRows,
      totalFeatures: mergedRows.length,
      shownFeatures: mergedRows.length,
    };
    const onOpenEvidence = vi.fn();

    render(
      <BusinessSummaryPanel
        data={dataWithManyMergedSource}
        baseTrimId="comfort"
        deltaFilter="MERGED_SOURCE"
        onOpenEvidence={onOpenEvidence}
      />,
    );

    const expandControl = screen.getByText("展开 2 项合并格展开");
    const details = expandControl.closest("details") as HTMLDetailsElement | null;
    expect(details?.open).toBe(false);

    fireEvent.click(expandControl);

    expect(details?.open).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "查看 Premium-FWD Merged source feature 4 的证据来源" }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "merged_source_4" }),
      trim: expect.objectContaining({ trimId: "premium" }),
      cell: expect.objectContaining({
        source: expect.objectContaining({
          cell: "E24",
          sourceCell: "D24",
          mergedRange: "D24:F24",
        }),
      }),
      selectionReason: expect.stringContaining("合并格展开"),
    }));
  });

  it("scopes the business narrative and metrics to the table search", () => {
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        searchValue="speaker"
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("搜索：speaker 业务摘要")).toBeTruthy();
    expect(screen.getByText(/当前聚焦 搜索：speaker，当前对比 1 个目标配置列，累计 1 个目标差异/)).toBeTruthy();
    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD 在 搜索：speaker：值变化 1 项/)).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "0新增配置")).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "1值变化")).toBeTruthy();
    expect(screen.queryByText("360 round view camera / 360度高清全景影像")).toBeNull();
  });

  it("normalizes wrapped category labels in business summary content", () => {
    const dataWithWrappedCategory: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode === "camera_360"
        ? { ...row, category: "驾驶辅助\n Drive assist" }
        : row),
    };

    render(<BusinessSummaryPanel data={dataWithWrappedCategory} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    expect(screen.getAllByText("驾驶辅助 Drive assist").length).toBeGreaterThan(0);
    expect(screen.queryByText("驾驶辅助\n Drive assist")).toBeNull();
  });

  it("separates target delta count from affected feature count", () => {
    const threeTrimData: CompareResponse = {
      ...compareData,
      trims: [
        ...compareData.trims,
        {
          trimId: "flagship",
          fullTrimName: "两驱旗舰型 Flagship-FWD",
          brand: "OMODA",
          modelName: "T19C MY ICE",
          trimName: "Flagship-FWD",
        },
      ],
      rows: compareData.rows.map((row) => {
        if (row.featureCode === "camera_360") {
          return {
            ...row,
            values: [
              row.values[0],
              row.values[1],
              {
                valueId: "flagship-camera",
                rawValue: "●",
                normalizedValue: "standard",
                availability: "STANDARD",
                unit: null,
                valueState: "marker_value",
                displayValue: "标配",
                inferred: false,
                source: null,
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
    };

    render(<BusinessSummaryPanel data={threeTrimData} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    expect(screen.getByText(/Comfort-FWD 作为基准列，当前对比 2 个目标配置列，累计 3 个目标差异，涉及 2 行配置/)).toBeTruthy();
  });

  it("scopes the business summary to the focused target trim", () => {
    const threeTrimData: CompareResponse = {
      ...compareData,
      trims: [
        ...compareData.trims,
        {
          trimId: "flagship",
          fullTrimName: "两驱旗舰型 Flagship-FWD",
          brand: "OMODA",
          modelName: "T19C MY ICE",
          trimName: "Flagship-FWD",
        },
      ],
      rows: compareData.rows.map((row) => ({
        ...row,
        values: [
          row.values[0],
          row.values[1],
          row.featureCode === "camera_360"
            ? row.values[1]
            : {
                valueId: "flagship-speaker",
                rawValue: "10",
                normalizedValue: "10",
                availability: "VALUE",
                unit: null,
                valueState: "numeric_value",
                displayValue: "10",
                inferred: false,
                source: null,
              },
        ],
      })),
    };

    render(
      <BusinessSummaryPanel
        data={threeTrimData}
        baseTrimId="comfort"
        targetTrimFilterId="premium"
        onOpenEvidence={vi.fn()}
      />,
    );

    expect(screen.getByText("目标 Premium-FWD 业务摘要")).toBeTruthy();
    expect(screen.getByLabelText("摘要统计口径").textContent).toContain("摘要只统计当前范围内的业务差异；表格仍展示当前范围内全部配置行，查看差异项后才收窄。");
    expect(screen.getByText(/Comfort-FWD 作为基准列，当前聚焦 目标 Premium-FWD，当前对比 1 个目标配置列/)).toBeTruthy();
    expect(screen.getAllByText("Premium-FWD").length).toBeGreaterThan(0);
    expect(screen.queryByText("Flagship-FWD")).toBeNull();
  });

  it("lets users expand hidden business deltas and open their evidence", () => {
    const addedRows: CompareResponse["rows"] = Array.from({ length: 5 }, (_, index) => ({
      category: "舒适便利 Convenience",
      featureCode: `comfort_added_${index + 1}`,
      featureName: `Convenience upgrade ${index + 1}`,
      comparisonType: "UNIQUE_OR_PARTIAL",
      uniqueTrimIds: [],
      businessNote: "Premium 独有配置",
      values: [
        {
          valueId: `comfort-added-${index + 1}`,
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          inferred: false,
          displayValue: "不配备",
          source: null,
        },
        {
          valueId: `premium-added-${index + 1}`,
          rawValue: "●",
          normalizedValue: "standard",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
          inferred: false,
          source: null,
        },
      ],
    }));
    const dataWithManyAdded: CompareResponse = {
      ...compareData,
      rows: addedRows,
      totalFeatures: addedRows.length,
      shownFeatures: addedRows.length,
    };

    const onOpenEvidence = vi.fn();
    render(<BusinessSummaryPanel data={dataWithManyAdded} baseTrimId="comfort" onOpenEvidence={onOpenEvidence} />);

    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD：新增 5 项/)).toBeTruthy();
    expect(screen.getByLabelText("Premium-FWD 业务结论").textContent).toContain("增配重点5项 · 舒适便利：Convenience upgrade 1、Convenience upgrade 2、+3");
    expect(screen.getByText("Convenience upgrade 1")).toBeTruthy();
    expect(screen.getByText("Convenience upgrade 3")).toBeTruthy();
    const expandControl = screen.getByText("展开 2 项主要增加");
    const details = expandControl.closest("details") as HTMLDetailsElement | null;
    expect(details?.open).toBe(false);

    fireEvent.click(expandControl);

    expect(details?.open).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "查看 Premium-FWD Convenience upgrade 4 的新增来源" }));

    expect(onOpenEvidence).toHaveBeenCalledWith(expect.objectContaining({
      row: expect.objectContaining({ featureCode: "comfort_added_4" }),
      trim: expect.objectContaining({ trimId: "premium" }),
      selectionReason: expect.stringContaining("新增差异"),
    }));
  });

  it("can request the table to focus on difference rows", () => {
    const onShowDifferenceRows = vi.fn();
    render(
      <BusinessSummaryPanel
        data={compareData}
        baseTrimId="comfort"
        onShowDifferenceRows={onShowDifferenceRows}
        onOpenEvidence={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "查看差异项" }));

    expect(onShowDifferenceRows).toHaveBeenCalledTimes(1);
  });

  it("opens inferred evidence without changing the business delta direction", () => {
    const onOpenEvidence = vi.fn();
    render(<BusinessSummaryPanel data={compareData} baseTrimId="comfort" onOpenEvidence={onOpenEvidence} />);

    fireEvent.click(screen.getByRole("button", { name: /查看 Comfort-FWD 360 round view camera \/ 360度高清全景影像 的新增推断来源/ }));

    expect(onOpenEvidence).toHaveBeenCalledTimes(1);
    expect(onOpenEvidence.mock.calls[0][0].trim.trimId).toBe("comfort");
    expect(onOpenEvidence.mock.calls[0][0].cell?.inferred).toBe(true);
    expect(onOpenEvidence.mock.calls[0][0].row.featureCode).toBe("camera_360");
    expect(onOpenEvidence.mock.calls[0][0].selectionReason).toBe("业务摘要优先打开 Comfort-FWD 的推断值，用于解释 Premium-FWD 相比 Comfort-FWD 的新增差异。");
  });

  it("does not count matching blank unknown cells as business differences", () => {
    const dataWithSharedBlankUnknown: CompareResponse = {
      ...compareData,
      rows: [
        ...compareData.rows,
        {
          category: "参数 Parameters",
          featureCode: "shared_unknown_blank",
          featureName: "Shared blank parameter / 共通空白参数",
          comparisonType: "MISSING_OR_UNKNOWN",
          uniqueTrimIds: [],
          businessNote: "双方都是空白，需结合来源确认。",
          values: [
            {
              valueId: "comfort-shared-blank",
              rawValue: "",
              normalizedValue: null,
              availability: "UNKNOWN",
              unit: null,
              valueState: "blank",
              displayValue: "空白",
              inferred: false,
              source: null,
            },
            {
              valueId: "premium-shared-blank",
              rawValue: "",
              normalizedValue: null,
              availability: "UNKNOWN",
              unit: null,
              valueState: "blank",
              displayValue: "空白",
              inferred: false,
              source: null,
            },
          ],
        },
      ],
      totalFeatures: 3,
      shownFeatures: 3,
    };

    render(<BusinessSummaryPanel data={dataWithSharedBlankUnknown} baseTrimId="comfort" onOpenEvidence={vi.fn()} />);

    expect(screen.getByText(/Premium-FWD 相比 Comfort-FWD：新增 1 项，值变化 1 项/)).toBeTruthy();
    expect(screen.getByText((_content, element) => element?.textContent === "0待确认")).toBeTruthy();
    expect(screen.queryByText("Shared blank parameter / 共通空白参数")).toBeNull();
  });
});
