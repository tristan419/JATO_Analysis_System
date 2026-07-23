import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_T19C_TRIM_IDS = [
  "e952fa31-8a28-426d-89e9-2a55e4598e45",
  "a430cb0e-e24b-409c-9054-3c8b1011a0b2",
  "889aacd2-7ab7-47dc-ae8d-84f4515c3ea0",
];

const SOURCE_DIGEST_SEARCH_LABEL = /搜索 Source Digest 可比组|搜索来源\s*\/\s*车型\s*\/\s*配置列/;
const SOURCE_DIGEST_PICKER_LABEL = /选择 Source\s*\/\s*Model\s*\/\s*配置列|选择来源\s*\/\s*车型\s*\/\s*配置列/;

const VIEWPORTS = [
  {
    key: "desktop",
    label: "Desktop",
    width: 1440,
    height: 920,
    checkDeck: false,
    checkExports: true,
    checkLargeSourceSearch: true,
    checkMultiSourceSameModelSearch: true,
    checkUploadDiagnostics: true,
  },
  {
    key: "mobile",
    label: "Mobile",
    width: 390,
    height: 844,
    checkDeck: true,
    checkExports: false,
    checkLargeSourceSearch: false,
    checkMultiSourceSameModelSearch: false,
    checkUploadDiagnostics: true,
  },
];

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_T19C_SMOKE_BASE_URL || process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    trimIds: (process.env.PRODUCT_CONFIG_T19C_SMOKE_TRIM_IDS || "").split(",").map((value) => value.trim()).filter(Boolean),
    baseTrimId: process.env.PRODUCT_CONFIG_T19C_SMOKE_BASE_TRIM_ID || "",
    expectedRows: Number(process.env.PRODUCT_CONFIG_T19C_SMOKE_EXPECTED_ROWS || "227"),
    viewport: process.env.PRODUCT_CONFIG_T19C_SMOKE_VIEWPORT || "all",
    browserChannel: process.env.PRODUCT_CONFIG_SMOKE_BROWSER_CHANNEL || "",
    headed: false,
    timeoutMs: 90000,
    help: false,
  };
  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") options.help = true;
    else if (arg === "--headed") options.headed = true;
    else if (arg.startsWith("--base-url=")) options.baseUrl = arg.slice("--base-url=".length);
    else if (arg.startsWith("--trim-ids=")) options.trimIds = arg.slice("--trim-ids=".length).split(",").map((value) => value.trim()).filter(Boolean);
    else if (arg.startsWith("--base-trim-id=")) options.baseTrimId = arg.slice("--base-trim-id=".length);
    else if (arg.startsWith("--expected-rows=")) options.expectedRows = Number(arg.slice("--expected-rows=".length)) || options.expectedRows;
    else if (arg.startsWith("--viewport=")) options.viewport = arg.slice("--viewport=".length);
    else if (arg.startsWith("--channel=")) options.browserChannel = arg.slice("--channel=".length);
    else if (arg.startsWith("--timeout-ms=")) options.timeoutMs = Math.max(15000, Number(arg.slice("--timeout-ms=".length)) || options.timeoutMs);
  }
  options.baseUrl = options.baseUrl.replace(/\/+$/, "");
  if (options.trimIds.length === 0) options.trimIds = DEFAULT_T19C_TRIM_IDS;
  if (!options.baseTrimId) options.baseTrimId = options.trimIds[0] || "";
  return options;
}

function usage() {
  return [
    "Read-only Product Config T19C AI summary smoke.",
    "",
    "Checks the Excel-user simple mode with the known T19C Basic / Comfort / Premium config columns.",
    "",
    "  node scripts/product_config_t19c_ai_smoke.mjs \\",
    "    --base-url=http://127.0.0.1:5177",
    "",
    "Options:",
    "  --headed                    Show the browser.",
    "  --channel=chrome            Use an installed browser channel instead of Playwright's bundled browser.",
    "  --trim-ids=id1,id2,id3      Override selected config-column ids.",
    "  --base-trim-id=id1          Override the base config-column id.",
    "  --expected-rows=227         Expected full-table row count.",
    "  --viewport=all|desktop|mobile",
  ].join("\n");
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function tableStatusPattern(expectedRows) {
  return new RegExp(`当前展示\\s+${expectedRows}\\s*/\\s*${expectedRows}\\s*(配置行|行)`);
}

function evaluateChecks(bodyText, expectedRows, overflow) {
  return {
    hasAiBusinessPanel: bodyText.includes("基于配置事实生成业务结论"),
    hasComfortHeadline: bodyText.includes("Comfort-FWD") || bodyText.includes("两驱舒适型"),
    hasPremiumHeadline: bodyText.includes("Premium-FWD") || bodyText.includes("两驱尊贵型"),
    hasMainUpgradeText: bodyText.includes("主要升级"),
    hasEvidenceBoundary: bodyText.includes("证据边界") || bodyText.includes("证据提示"),
    evidenceStatusHiddenByDefault: !bodyText.includes("证据状态"),
    hasFullTableRows: tableStatusPattern(expectedRows).test(bodyText),
    hasFullRangeChip: bodyText.includes("范围 全部配置行"),
    hasSimpleSwitch: (bodyText.includes("简易") || bodyText.includes("简洁")) && bodyText.includes("专家"),
    hiddenExcelSpeedRead: !bodyText.includes("Excel 首屏速读"),
    noHorizontalOverflow: !overflow.hasDocumentOverflow && !overflow.hasBodyOverflow,
  };
}

function summarizeAiComposePayload(payload) {
  const targets = Array.isArray(payload?.targets) ? payload.targets : [];
  const targetSummaries = targets.map((target) => {
    const sourceEvidenceSummary = target && typeof target === "object" ? target.sourceEvidenceSummary : null;
    const summary = sourceEvidenceSummary && typeof sourceEvidenceSummary === "object" ? sourceEvidenceSummary : {};
    const sourceSheetNames = Array.isArray(summary.sourceSheetNames) ? summary.sourceSheetNames : [];
    return {
      targetTrimId: String(target?.targetTrimId ?? ""),
      hasSourceEvidenceSummary: Boolean(sourceEvidenceSummary && typeof sourceEvidenceSummary === "object"),
      differenceCount: Number(summary.differenceCount ?? -1),
      withSourceEvidenceCount: Number(summary.withSourceEvidenceCount ?? -1),
      missingSourceEvidenceCount: Number(summary.missingSourceEvidenceCount ?? -1),
      inferredCount: Number(summary.inferredCount ?? -1),
      unknownCount: Number(summary.unknownCount ?? -1),
      mergedCellExpandedCount: Number(summary.mergedCellExpandedCount ?? -1),
      sourceSheetCount: sourceSheetNames.length,
      evidenceFactsCount: Array.isArray(target?.evidenceFacts) ? target.evidenceFacts.length : 0,
      categoryFactsCount: Array.isArray(target?.categoryFacts) ? target.categoryFacts.length : 0,
    };
  });
  const richEvidenceFactTargetSummaries = targetSummaries.filter((target) => target.differenceCount > 0);
  return {
    targetCount: targets.length,
    hasContextCompareScope: Boolean(payload?.context?.compareScope),
    allTargetsHaveSourceEvidenceSummary: targetSummaries.length > 0
      && targetSummaries.every((target) => target.hasSourceEvidenceSummary),
    allTargetsHaveEvidenceFacts: targetSummaries.length > 0
      && targetSummaries.every((target) => target.evidenceFactsCount > 0),
    allTargetsHaveCategoryFacts: targetSummaries.length > 0
      && targetSummaries.every((target) => target.categoryFactsCount > 0),
    allTargetsHaveRichEvidenceFacts: richEvidenceFactTargetSummaries.length > 0
      && richEvidenceFactTargetSummaries.every((target) => (
        target.evidenceFactsCount >= Math.min(target.differenceCount, 20)
      )),
    minEvidenceFactsPerDifferenceTarget: richEvidenceFactTargetSummaries.length > 0
      ? Math.min(...richEvidenceFactTargetSummaries.map((target) => target.evidenceFactsCount))
      : 0,
    maxDifferenceCountPerTarget: richEvidenceFactTargetSummaries.length > 0
      ? Math.max(...richEvidenceFactTargetSummaries.map((target) => target.differenceCount))
      : 0,
    hasEvidenceBoundarySignal: targetSummaries.some((target) => (
      target.missingSourceEvidenceCount > 0
      || target.inferredCount > 0
      || target.mergedCellExpandedCount > 0
      || target.sourceSheetCount > 0
    )),
    targetSummaries,
  };
}

function summarizeExportPayload(payload) {
  const businessSummary = Array.isArray(payload?.businessSummary) ? payload.businessSummary : [];
  const usage = payload?.businessSummaryUsage && typeof payload.businessSummaryUsage === "object"
    ? payload.businessSummaryUsage
    : null;
  const summaryItems = businessSummary.map((item) => ({
    targetTrimId: String(item?.targetTrimId ?? ""),
    hasHeadline: Boolean(String(item?.headline ?? "").trim()),
    mainUpgradeCount: Array.isArray(item?.mainUpgrades) ? item.mainUpgrades.length : 0,
    evidenceStatusCount: Array.isArray(item?.evidenceStatus) ? item.evidenceStatus.length : 0,
    evidenceRefCount: Array.isArray(item?.evidenceRefs) ? item.evidenceRefs.length : 0,
    hasRecommendedUse: Boolean(String(item?.recommendedUse ?? "").trim()),
  }));
  return {
    rowCount: Array.isArray(payload?.rows) ? payload.rows.length : 0,
    trimCount: Array.isArray(payload?.trims) ? payload.trims.length : 0,
    businessSummaryCount: businessSummary.length,
    hasBusinessSummaryUsage: Boolean(usage),
    usageProvider: String(usage?.provider ?? ""),
    usageModel: String(usage?.model ?? ""),
    usageStatus: String(usage?.status ?? ""),
    allSummaryItemsHaveEvidenceStatus: summaryItems.length > 0
      && summaryItems.every((item) => item.evidenceStatusCount > 0),
    allSummaryItemsHaveRecommendedUse: summaryItems.length > 0
      && summaryItems.every((item) => item.hasRecommendedUse),
    hasEvidenceRefs: summaryItems.some((item) => item.evidenceRefCount > 0),
    summaryItems,
  };
}

function isObservedProductConfigRequest(url) {
  return (
    url.includes("/engineering-config/compare")
    || url.includes("/engineering-config/business-summary/compose")
    || url.includes("/engineering-config/business-summary/readiness")
    || url.includes("/engineering-config/ocr/readiness")
    || url.includes("/engineering-config/trims")
    || url.includes("/engineering-config/source/snapshots")
    || url.includes("/engineering-config/recommendations/competitors")
  );
}

async function evaluateRangeChipVisibility(page) {
  return page.evaluate(() => {
    const visibleByLabel = (label) => {
      const element = document.querySelector(`[aria-label="${label}"]`);
      if (!element) {
        return {
          found: false,
          visible: false,
          text: "",
        };
      }
      const rect = element.getBoundingClientRect();
      const style = window.getComputedStyle(element);
      return {
        found: true,
        visible: (
          rect.width > 0
          && rect.height > 0
          && style.display !== "none"
          && style.visibility !== "hidden"
          && style.opacity !== "0"
        ),
        text: element.textContent?.replace(/\s+/g, " ").trim() || "",
      };
    };
    const hero = visibleByLabel("顶部当前表格范围：全部配置行");
    const floatingDeck = visibleByLabel("FloatingDeck 当前表格范围：全部配置行");
    return {
      heroFullRangeChipFound: hero.found,
      heroFullRangeChipVisible: hero.visible,
      heroFullRangeChipText: hero.text,
      floatingDeckFullRangeChipFound: floatingDeck.found,
      floatingDeckFullRangeChipVisible: floatingDeck.visible,
      floatingDeckFullRangeChipText: floatingDeck.text,
    };
  });
}

async function evaluateSimpleAiSummaryLayout(page) {
  return page.evaluate(() => {
    const ariaLabels = Array.from(document.querySelectorAll("[aria-label]"))
      .map((element) => element.getAttribute("aria-label") || "")
      .filter(Boolean);
    const deterministicRuleLabels = ariaLabels.filter((label) => (
      label === "Excel 首屏速读"
      || label === "版本差异速读"
      || label === "Excel 对比导读"
      || label === "目标配置列结论抽屉"
      || label === "当前基准对比速览"
      || label === "相邻版本升级路径"
      || label.endsWith("业务解读")
      || label.endsWith("业务重点分组")
      || label.endsWith("结论草稿")
    ));
    const panel = document.querySelector(".business-summary-panel");
    const aiSummary = document.querySelector('[aria-label="AI 配置对比摘要"]');
    const compactAiCards = document.querySelectorAll(".business-summary-llm-card.is-compact");
    return {
      hasBusinessSummaryPanel: Boolean(panel),
      simpleAiOnlyPanelActive: Boolean(
        panel?.classList.contains("is-simple")
        && panel.classList.contains("has-ai-summary")
        && panel.classList.contains("is-ai-only"),
      ),
      aiSummaryVisible: Boolean(aiSummary),
      compactAiCardCount: compactAiCards.length,
      deterministicRuleLabels,
    };
  });
}

async function evaluateCompactAiCardLayout(page) {
  return page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll(".business-summary-llm-card.is-compact"));
    const cardItems = cards.map((card) => {
      const details = card.querySelector(".business-summary-llm-card__compact-details");
      const quickActionButton = card.querySelector(".business-summary-llm-card__compact-actions-row .btn");
      const cardRect = card.getBoundingClientRect();
      const buttonRect = quickActionButton?.getBoundingClientRect();
      return {
        open: details instanceof HTMLDetailsElement ? details.open : false,
        cardHeight: Math.round(cardRect.height),
        buttonText: quickActionButton?.textContent?.trim() || "",
        buttonWidth: Math.round(buttonRect?.width ?? 0),
        buttonHeight: Math.round(buttonRect?.height ?? 0),
      };
    });
    const expandedItems = cardItems.filter((item) => item.open);
    const collapsedItems = cardItems.filter((item) => !item.open);
    const maxExpandedHeight = expandedItems.reduce((maxHeight, item) => Math.max(maxHeight, item.cardHeight), 0);
    const maxCollapsedHeight = collapsedItems.reduce((maxHeight, item) => Math.max(maxHeight, item.cardHeight), 0);
    const collapsedActionItems = collapsedItems.filter((item) => item.buttonText.length > 0);
    const allCardsCollapsedByDefault = cardItems.length > 0 && expandedItems.length === 0 && collapsedItems.length === cardItems.length;
    const collapsedCardsStayCompact = collapsedItems.length > 0 && maxCollapsedHeight <= 220;
    const quickEvidenceButtonsHorizontal = collapsedActionItems.length > 0
      && collapsedActionItems.every((item) => (
        item.buttonWidth >= 56
        && item.buttonHeight > 0
        && item.buttonWidth > item.buttonHeight * 1.8
        && item.buttonHeight <= 36
      ));
    return {
      compactCardCount: cardItems.length,
      expandedCompactCardCount: expandedItems.length,
      collapsedCompactCardCount: collapsedItems.length,
      maxExpandedHeight,
      maxCollapsedHeight,
      allCardsCollapsedByDefault,
      collapsedCardsStayCompact,
      quickEvidenceButtonsHorizontal,
      cardItems,
    };
  });
}

async function evaluateCompactAiEvidenceBoundary(page) {
  const beforeExpand = await page.evaluate(() => {
    const firstCard = document.querySelector(".business-summary-llm-card.is-compact");
    const details = firstCard?.querySelector(".business-summary-llm-card__compact-details");
    const quickEvidenceButton = firstCard?.querySelector(".business-summary-llm-card__compact-actions-row .btn");
    return {
      cardCollapsed: details instanceof HTMLDetailsElement ? !details.open : false,
      quickEvidenceVisible: Boolean(quickEvidenceButton),
      cardText: firstCard?.textContent?.replace(/\s+/g, " ").trim() || "",
    };
  });
  const cardSummary = page.locator(".business-summary-llm-card.is-compact .business-summary-llm-card__compact-summary").first();
  if (await cardSummary.count() > 0) {
    await cardSummary.click({ timeout: 15000 });
  }
  const afterExpand = await page.evaluate(() => {
    const firstCard = document.querySelector(".business-summary-llm-card.is-compact");
    const details = firstCard?.querySelector(".business-summary-llm-card__compact-details");
    const disclosure = firstCard?.querySelector(".business-summary-llm-card__evidence-disclosure");
    return {
      cardOpen: details instanceof HTMLDetailsElement ? details.open : false,
      hasDisclosure: Boolean(disclosure),
      disclosureLabel: disclosure?.getAttribute("aria-label") || "",
      disclosureOpen: disclosure instanceof HTMLDetailsElement ? disclosure.open : null,
      evidenceBodyRendered: Boolean(firstCard?.querySelector(".business-summary-llm-card__evidence-disclosure-body")),
      cardText: firstCard?.textContent?.replace(/\s+/g, " ").trim() || "",
    };
  });
  return {
    beforeExpand,
    afterExpand,
    cardCollapsedByDefault: beforeExpand.cardCollapsed,
    quickEvidenceVisibleWhileCollapsed: beforeExpand.quickEvidenceVisible,
    inlineEvidenceBoundaryHidden: afterExpand.cardOpen
      && !afterExpand.hasDisclosure
      && afterExpand.evidenceBodyRendered === false
      && !afterExpand.cardText.includes("证据边界")
      && !afterExpand.cardText.includes("证据状态"),
  };
}

async function evaluateSimpleTableNavigator(page, expectedRows) {
  const readStatusState = async () => page.evaluate(() => {
    const status = document.querySelector('[aria-label="配置表范围状态"]');
    const buttonItems = Array.from(status?.querySelectorAll("button") ?? []).map((button) => ({
      ariaLabel: button.getAttribute("aria-label") || "",
      text: button.textContent?.replace(/\s+/g, " ").trim() || "",
    }));
    const hasButtonText = (text) => buttonItems.some((button) => button.text === text || button.ariaLabel === text);
    return {
      text: status?.textContent?.replace(/\s+/g, " ").trim() || "",
      buttonItems,
      hasFirstDifferenceJump: hasButtonText("定位首个差异行"),
      hasPreviousDifference: hasButtonText("上一个差异行") || hasButtonText("上一个"),
      hasNextDifference: hasButtonText("下一个差异行") || hasButtonText("下一个"),
      hasCopySelectedRow: hasButtonText("复制选中行"),
      hasRestoreAll: buttonItems.some((button) => button.ariaLabel === "从状态栏恢复全部配置行" || button.text === "恢复全部"),
      selectedDifferenceText: (status?.textContent || "").match(/差异行\s+\d+\/\d+/)?.[0] || "",
    };
  });

  const defaultState = await readStatusState();
  const differenceMetric = page.locator('button[aria-label^="显示差异行"]');
  const differenceMetricCount = await differenceMetric.count();
  if (differenceMetricCount > 0) {
    await differenceMetric.first().click({ timeout: 15000 });
    await page.waitForFunction((rows) => {
      const status = document.querySelector('[aria-label="配置表范围状态"]');
      const text = status?.textContent?.replace(/\s+/g, " ").trim() || "";
      return text.includes(`当前展示`) && text.includes(`/${rows}`) && text.includes("差异行");
    }, expectedRows, { timeout: 15000 });
  }
  const differenceScopeState = await readStatusState();
  const firstDifferenceButton = page.getByRole("button", { name: "定位首个差异行" });
  const firstDifferenceButtonCount = await firstDifferenceButton.count();
  if (firstDifferenceButtonCount > 0) {
    await firstDifferenceButton.first().click({ timeout: 15000 });
    await page.waitForFunction(() => {
      const status = document.querySelector('[aria-label="配置表范围状态"]');
      const text = status?.textContent || "";
      return /差异行\s+1\/\d+/.test(text) && text.includes("复制选中行");
    }, null, { timeout: 15000 });
  }
  const selectedRowState = await readStatusState();
  const restoreButton = page.locator('button[aria-label="从状态栏恢复全部配置行"]');
  if (await restoreButton.count() > 0) {
    await restoreButton.first().click({ timeout: 15000 });
    await page.waitForFunction((rows) => {
      const status = document.querySelector('[aria-label="配置表范围状态"]');
      const text = status?.textContent?.replace(/\s+/g, " ").trim() || "";
      return text.includes(`当前展示 ${rows}/${rows}`) && text.includes("全部配置行");
    }, expectedRows, { timeout: 15000 });
  }
  const restoredState = await readStatusState();

  return {
    defaultState,
    differenceScopeState,
    selectedRowState,
    restoredState,
    differenceMetricCount,
    firstDifferenceButtonCount,
    defaultFullRowsVisible: defaultState.text.includes(`当前展示 ${expectedRows}/${expectedRows}`),
    defaultNavigatorHidden: !defaultState.hasFirstDifferenceJump
      && !defaultState.hasPreviousDifference
      && !defaultState.hasNextDifference
      && !defaultState.hasCopySelectedRow,
    differenceScopeReached: differenceScopeState.text.includes(`/${expectedRows}`)
      && differenceScopeState.text.includes("差异行"),
    differenceNavigatorVisible: differenceScopeState.hasFirstDifferenceJump
      && differenceScopeState.hasPreviousDifference
      && differenceScopeState.hasNextDifference,
    copySelectedHiddenBeforeRowFocus: !differenceScopeState.hasCopySelectedRow,
    selectedRowCopyVisible: selectedRowState.selectedDifferenceText.startsWith("差异行 1/")
      && selectedRowState.hasCopySelectedRow,
    restoredFullRowsVisible: restoredState.text.includes(`当前展示 ${expectedRows}/${expectedRows}`)
      && restoredState.text.includes("全部配置行"),
    restoredNavigatorHidden: !restoredState.hasFirstDifferenceJump
      && !restoredState.hasCopySelectedRow,
  };
}

async function evaluateInitialLoadDeferral(page, requests) {
  const dom = await page.evaluate(() => {
    const bodyText = document.body.innerText.replace(/\s+/g, " ");
    const hasSourceDigestControls = (
      bodyText.includes("搜索来源 / 车型 / 配置列")
      || bodyText.includes("选择来源 / 车型 / 配置列")
      || bodyText.includes("搜索 Source Digest 可比组")
      || bodyText.includes("选择 Source / Model / 配置列")
    );
    return {
      bodyText: bodyText.slice(0, 1400),
      hasFloatingDeckPanel: Boolean(document.querySelector(".engineering-config-control-panel")),
      hasSourceDigestControls,
      hasSourceUploadPanel: bodyText.includes("配置表 / 价格单上传") || bodyText.includes("Source Digest 上传"),
      hasAdvancedRecommendationPanel: bodyText.includes("推荐竞品") || bodyText.includes("Advanced Analysis"),
    };
  });
  const requestUrls = requests.map((request) => request.url);
  return {
    dom,
    requestUrls,
    configLibraryDeferred: requestUrls.every((url) => !url.includes("/engineering-config/trims")),
    sourceLibraryDeferred: requestUrls.every((url) => !url.includes("/engineering-config/source/snapshots")),
    competitorRecommendationsDeferred: requestUrls.every((url) => !url.includes("/engineering-config/recommendations/competitors")),
    sourceDigestControlsHidden: !dom.hasFloatingDeckPanel && !dom.hasSourceDigestControls && !dom.hasSourceUploadPanel,
    advancedRecommendationsHidden: !dom.hasAdvancedRecommendationPanel,
  };
}

async function evaluateFloatingDeckLayout(page) {
  const trigger = page.getByRole("button", { name: /添加配置列 \/ 显示/ });
  await trigger.click({ timeout: 15000 });
  const panel = page.locator(".engineering-config-control-panel.deck-floating-panel");
  await panel.waitFor({ state: "visible", timeout: 15000 });
  return page.evaluate((expectedQuery) => {
    function rectInfo(element) {
      if (!element) return null;
      const rect = element.getBoundingClientRect();
      return {
        left: Math.round(rect.left),
        right: Math.round(rect.right),
        top: Math.round(rect.top),
        bottom: Math.round(rect.bottom),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
      };
    }
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const panelElement = document.querySelector(".engineering-config-control-panel.deck-floating-panel");
    const drawerElement = document.querySelector(".engineering-config-control-drawer.deck-floating-drawer");
    const toggleElement = document.querySelector(".engineering-config-control-drawer .deck-floating-toggle");
    const tabsElement = document.querySelector(".engineering-config-control-panel .deck-control-tabs");
    const panelRect = rectInfo(panelElement);
    const drawerRect = rectInfo(drawerElement);
    const toggleRect = rectInfo(toggleElement);
    const tabsRect = rectInfo(tabsElement);
    const fitsWidth = (rect) => Boolean(rect && rect.left >= -1 && rect.right <= viewportWidth + 1 && rect.width <= viewportWidth + 2);
    const panelHeightReasonable = Boolean(panelRect && panelRect.height <= Math.round(viewportHeight * 0.75) + 4);
    return {
      viewportWidth,
      viewportHeight,
      panelRect,
      drawerRect,
      toggleRect,
      tabsRect,
      hasPanel: Boolean(panelRect),
      panelWidthFitsViewport: fitsWidth(panelRect),
      drawerWidthFitsViewport: fitsWidth(drawerRect),
      toggleWidthFitsViewport: fitsWidth(toggleRect),
      tabsWidthFitsViewport: fitsWidth(tabsRect),
      panelHeightReasonable,
    };
  });
}

async function openFloatingDeck(page) {
  const panel = page.locator(".engineering-config-control-panel.deck-floating-panel");
  if (await panel.isVisible().catch(() => false)) return;
  await page.getByRole("button", { name: /添加配置列 \/ 显示/ }).click({ timeout: 15000 });
  await panel.waitFor({ state: "visible", timeout: 15000 });
}

async function clickDisplayTab(page) {
  await page.getByRole("tab", { name: /显示模式|显示\s*\/\s*编辑/ }).click({ timeout: 15000 });
}

async function clickSourceDigestTab(page) {
  await page.getByRole("tab", { name: /Source Digest|来源\s*\/\s*上传/ }).click({ timeout: 15000 });
}

async function waitForDeckExportButton(page, formatLabel) {
  const button = page.getByRole("button", { name: new RegExp(`导出当前范围\\s*${formatLabel}`, "i") });
  await button.waitFor({ state: "visible", timeout: 15000 });
  await page.waitForFunction((label) => {
    const buttons = Array.from(document.querySelectorAll("button"));
    const target = buttons.find((item) => item.textContent?.replace(/\s+/g, " ").trim().includes(`导出当前范围 ${label}`));
    return Boolean(target && !target.disabled);
  }, formatLabel, { timeout: 15000 });
  return button;
}

async function clickAndInspectExportResponse(page, format) {
  const formatLabel = format.toUpperCase();
  const endpoint = `/engineering-config/compare/export/${format}`;
  const button = await waitForDeckExportButton(page, formatLabel);
  const previousDownloadCount = await page.evaluate(() => window.__productConfigDownloads?.length ?? 0);
  const responsePromise = page.waitForResponse((response) => (
    response.url().includes(endpoint) && response.request().method() === "POST"
  ), { timeout: 45000 });
  await button.click({ timeout: 15000 });
  const response = await responsePromise;
  let exportPayloadSummary = null;
  try {
    exportPayloadSummary = summarizeExportPayload(response.request().postDataJSON());
  } catch {
    exportPayloadSummary = null;
  }
  const networkBody = await response.body();
  await page.waitForFunction((count) => {
    const downloads = window.__productConfigDownloads ?? [];
    const item = downloads[count];
    return Boolean(item && item.ready);
  }, previousDownloadCount, { timeout: 15000 });
  const download = await page.evaluate((count) => window.__productConfigDownloads?.[count] ?? null, previousDownloadCount);
  const firstBytes = Array.isArray(download?.firstBytes) ? download.firstBytes : [];
  const signature = format === "xlsx"
    ? firstBytes.length >= 2 && firstBytes[0] === 0x50 && firstBytes[1] === 0x4b
    : firstBytes.slice(0, 4).map((value) => String.fromCharCode(value)).join("") === "%PDF";
  return {
    format,
    endpoint,
    status: response.status(),
    contentType: response.headers()["content-type"] ?? "",
    networkBytes: networkBody.length,
    downloadType: download?.type ?? "",
    downloadBytes: download?.size ?? 0,
    firstBytes,
    signatureOk: signature,
    payloadSummary: exportPayloadSummary,
  };
}

async function evaluateDeckExports(page) {
  await openFloatingDeck(page);
  await clickDisplayTab(page);
  const xlsx = await clickAndInspectExportResponse(page, "xlsx");
  const pdf = await clickAndInspectExportResponse(page, "pdf");
  return {
    xlsx,
    pdf,
    xlsxExportOk: xlsx.status === 200 && xlsx.downloadBytes > 0 && xlsx.signatureOk,
    pdfExportOk: pdf.status === 200 && pdf.downloadBytes > 0 && pdf.signatureOk,
    xlsxPayloadOk: Boolean(
      xlsx.payloadSummary?.businessSummaryCount > 0
      && xlsx.payloadSummary?.hasBusinessSummaryUsage
      && xlsx.payloadSummary?.allSummaryItemsHaveEvidenceStatus
      && xlsx.payloadSummary?.allSummaryItemsHaveRecommendedUse,
    ),
    pdfPayloadOk: Boolean(
      pdf.payloadSummary?.businessSummaryCount > 0
      && pdf.payloadSummary?.hasBusinessSummaryUsage
      && pdf.payloadSummary?.allSummaryItemsHaveEvidenceStatus
      && pdf.payloadSummary?.allSummaryItemsHaveRecommendedUse,
    ),
  };
}

async function evaluateDeckEditGate(page) {
  await openFloatingDeck(page);
  await clickDisplayTab(page);
  const editControl = page.getByLabel("在线编辑控制");
  await editControl.waitFor({ state: "visible", timeout: 15000 });
  const triggerText = (await page.getByRole("button", { name: /添加配置列 \/ 显示/ }).innerText({ timeout: 5000 })).trim();
  const enableButton = editControl.getByRole("button", { name: "开启在线编辑" });
  await enableButton.waitFor({ state: "visible", timeout: 15000 });
  const before = await page.evaluate(() => ({
    editableCells: document.querySelectorAll(".compare-cell--editable").length,
    hasTableEditStatus: Boolean(document.querySelector('[aria-label="配置表在线编辑状态"]')),
    hasEditNotice: Boolean(document.querySelector('[aria-label="在线编辑安全提示"]')),
    mainSurfaceEditButtons: Array.from(document.querySelectorAll(".comparison-hero button, .product-config-selected-strip button, .comparison-container button"))
      .map((button) => button.textContent?.replace(/\s+/g, " ").trim() || "")
      .filter((text) => /在线编辑|开启编辑|关闭编辑/.test(text)).length,
  }));
  const enableButtonEnabled = await enableButton.isEnabled();
  await enableButton.click({ timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll(".compare-cell--editable").length > 0, null, { timeout: 15000 });
  const enabled = await page.evaluate(() => {
    const notice = document.querySelector('[aria-label="在线编辑安全提示"]');
    const tableStatus = document.querySelector('[aria-label="配置表在线编辑状态"]');
    const drawerStatus = document.querySelector('[aria-label="在线编辑控制"]');
    return {
      editableCells: document.querySelectorAll(".compare-cell--editable").length,
      hasTableEditStatus: Boolean(tableStatus),
      tableStatusText: tableStatus?.textContent?.replace(/\s+/g, " ").trim() || "",
      hasEditNotice: Boolean(notice),
      editNoticeText: notice?.textContent?.replace(/\s+/g, " ").trim() || "",
      drawerStatusText: drawerStatus?.textContent?.replace(/\s+/g, " ").trim() || "",
      noticeHasActionButtons: Array.from(notice?.querySelectorAll("button") ?? []).length > 0,
    };
  });
  const closeButton = editControl.getByRole("button", { name: "关闭在线编辑" });
  await closeButton.click({ timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll(".compare-cell--editable").length === 0, null, { timeout: 15000 });
  const after = await page.evaluate(() => ({
    editableCells: document.querySelectorAll(".compare-cell--editable").length,
    hasTableEditStatus: Boolean(document.querySelector('[aria-label="配置表在线编辑状态"]')),
    hasEditNotice: Boolean(document.querySelector('[aria-label="在线编辑安全提示"]')),
  }));
  const closeButtonGone = await editControl.getByRole("button", { name: "关闭在线编辑" }).count() === 0;
  return {
    triggerText,
    before,
    enabled,
    after,
    enableButtonEnabled,
    closeButtonGone,
    editControlOk: (
      !triggerText.includes("编辑")
      && enableButtonEnabled
      && before.editableCells === 0
      && !before.hasTableEditStatus
      && !before.hasEditNotice
      && before.mainSurfaceEditButtons === 0
      && enabled.editableCells > 0
      && enabled.hasTableEditStatus
      && enabled.tableStatusText.includes("在线编辑已开启")
      && !enabled.hasEditNotice
      && enabled.drawerStatusText.includes("编辑已开启")
      && !enabled.noticeHasActionButtons
      && after.editableCells === 0
      && !after.hasTableEditStatus
      && !after.hasEditNotice
      && closeButtonGone
    ),
  };
}

async function evaluateUploadDiagnosticsDeferred(page) {
  await openFloatingDeck(page);
  await clickSourceDigestTab(page);
  const diagnostics = page.getByLabel("上传诊断");
  await diagnostics.waitFor({ state: "visible", timeout: 15000 });
  return page.evaluate(() => {
    const diagnosticsElement = document.querySelector('[aria-label="上传诊断"]');
    const archiveElement = document.querySelector('[aria-label="来源归档"]');
    const uploadPanel = diagnosticsElement?.closest(".config-source-upload-panel");
    return {
      hasUploadPanel: Boolean(uploadPanel),
      diagnosticsOpen: diagnosticsElement instanceof HTMLDetailsElement ? diagnosticsElement.open : null,
      archiveOpen: archiveElement instanceof HTMLDetailsElement ? archiveElement.open : null,
      hasOcrReadinessCard: Boolean(document.querySelector('[aria-label="OCR 环境预检"]')),
      hasAiReadinessCard: Boolean(document.querySelector('[aria-label="AI 摘要运行边界"], [aria-label="AI 摘要运行状态详情"]')),
      uploadPanelText: uploadPanel?.textContent?.replace(/\s+/g, " ").trim().slice(0, 320) || "",
    };
  });
}

async function evaluateMobileDirectSearchLayout(page, query = "T19C") {
  const input = page.getByRole("combobox", { name: "直接添加配置列" });
  await input.waitFor({ state: "visible", timeout: 15000 });
  await input.fill(query);
  await page.waitForFunction(() => {
    const searchInput = document.querySelector('input[aria-label="直接添加配置列"]');
    const menu = searchInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    return Boolean(menu?.querySelector('[role="option"]'));
  }, null, { timeout: 15000 });
  return page.evaluate((expectedQuery) => {
    function rectInfo(element) {
      if (!element) return null;
      const rect = element.getBoundingClientRect();
      return {
        left: Math.round(rect.left),
        right: Math.round(rect.right),
        top: Math.round(rect.top),
        bottom: Math.round(rect.bottom),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
      };
    }
    const viewportWidth = window.innerWidth;
    const searchInput = document.querySelector('input[aria-label="直接添加配置列"]');
    const dropdownRoot = searchInput?.closest(".comparison-filter-dropdown");
    const menu = dropdownRoot?.querySelector(".comparison-filter-dropdown-menu");
    const firstOption = menu?.querySelector('[role="option"]');
    const options = Array.from(menu?.querySelectorAll('[role="option"]') ?? []);
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
    const queryUpper = normalize(expectedQuery).toUpperCase();
    const inputRect = rectInfo(searchInput);
    const menuRect = rectInfo(menu);
    const firstOptionRect = rectInfo(firstOption);
    const fitsWidth = (rect) => Boolean(rect && rect.left >= -1 && rect.right <= viewportWidth + 1 && rect.width <= viewportWidth + 2);
    const allOptionTexts = options.map((option) => normalize(option.textContent || ""));
    const allOptionUpper = allOptionTexts.map((text) => text.toUpperCase());
    const containsExpectedQuery = queryUpper.length > 0 && allOptionUpper.some((text) => text.includes(queryUpper));
    const containsIrrelevantBmw = allOptionUpper.some((text) => text.includes("BMW X7") || text.includes("BMW-X7"));
    const containsRuntimeSmoke = allOptionUpper.some((text) => (
      text.includes("RUNTIME-SMOKE")
      || text.includes("RUNTIME SOURCE SCOPE")
    ));
    return {
      query: expectedQuery,
      viewportWidth,
      inputRect,
      menuRect,
      firstOptionRect,
      optionCount: options.length,
      optionTexts: allOptionTexts.slice(0, 10),
      containsExpectedQuery,
      containsIrrelevantBmw,
      containsRuntimeSmoke,
      inputWidthFitsViewport: fitsWidth(inputRect),
      menuWidthFitsViewport: fitsWidth(menuRect),
      firstOptionWidthFitsViewport: fitsWidth(firstOptionRect),
      firstOptionReadableHeight: Boolean(firstOptionRect && firstOptionRect.height >= 32),
    };
  }, query);
}

async function evaluateMobileSourceDigestSearchLayout(page, query = "T19C") {
  await clickSourceDigestTab(page);
  const searchInput = page.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_LABEL });
  await searchInput.waitFor({ state: "visible", timeout: 15000 });
  await searchInput.fill(query);
  await page.waitForFunction(() => {
    const input = document.querySelector('input[aria-label="搜索 Source Digest 可比组"], input[aria-label="搜索来源 / 车型 / 配置列"]');
    const picker = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    return input?.value?.trim().length > 0 && Boolean(picker);
  }, null, { timeout: 15000 });
  const directPicker = page.getByRole("combobox", { name: SOURCE_DIGEST_PICKER_LABEL });
  await directPicker.click({ timeout: 15000 });
  await page.waitForFunction(() => {
    const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const menu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    return Boolean(menu?.querySelector('[role="option"]'));
  }, null, { timeout: 15000 });
  return page.evaluate(() => {
    function rectInfo(element) {
      if (!element) return null;
      const rect = element.getBoundingClientRect();
      return {
        left: Math.round(rect.left),
        right: Math.round(rect.right),
        top: Math.round(rect.top),
        bottom: Math.round(rect.bottom),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
      };
    }
    const viewportWidth = window.innerWidth;
    const searchInputElement = document.querySelector('input[aria-label="搜索 Source Digest 可比组"], input[aria-label="搜索来源 / 车型 / 配置列"]');
    const pickerInputElement = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const pickerDropdownRoot = pickerInputElement?.closest(".comparison-filter-dropdown");
    const pickerMenu = pickerDropdownRoot?.querySelector(".comparison-filter-dropdown-menu");
    const firstOption = pickerMenu?.querySelector('[role="option"]');
    const options = Array.from(pickerMenu?.querySelectorAll('[role="option"]') ?? []);
    const pathPreview = document.querySelector('[aria-label="Source Digest 命中路径预览"]');
    const searchInputRect = rectInfo(searchInputElement);
    const pickerInputRect = rectInfo(pickerInputElement);
    const pickerMenuRect = rectInfo(pickerMenu);
    const firstOptionRect = rectInfo(firstOption);
    const pathPreviewRect = rectInfo(pathPreview);
    const fitsWidth = (rect) => Boolean(rect && rect.left >= -1 && rect.right <= viewportWidth + 1 && rect.width <= viewportWidth + 2);
    const optionTexts = options.slice(0, 5).map((option) => option.textContent?.replace(/\s+/g, " ").trim() || "");
    return {
      viewportWidth,
      searchInputRect,
      pickerInputRect,
      pickerMenuRect,
      firstOptionRect,
      pathPreviewRect,
      optionCount: options.length,
      optionTexts,
      searchInputWidthFitsViewport: fitsWidth(searchInputRect),
      pickerInputWidthFitsViewport: fitsWidth(pickerInputRect),
      pickerMenuWidthFitsViewport: fitsWidth(pickerMenuRect),
      firstOptionWidthFitsViewport: fitsWidth(firstOptionRect),
      pathPreviewWidthFitsViewport: pathPreviewRect ? fitsWidth(pathPreviewRect) : true,
      firstOptionReadableHeight: Boolean(firstOptionRect && firstOptionRect.height >= 32),
    };
  });
}

async function evaluateLargeSourceDigestSearch(page, query = "欧盟在售车型可控资源表20260226.xlsx") {
  await openFloatingDeck(page);
  await clickSourceDigestTab(page);
  const searchInput = page.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_LABEL });
  await searchInput.waitFor({ state: "visible", timeout: 15000 });
  await searchInput.fill(query);
  await page.waitForFunction((expectedQuery) => {
    const text = document.body.innerText.replace(/\s+/g, " ").trim();
    return text.includes(expectedQuery)
      && text.includes("来源命中");
  }, query, { timeout: 45000 });
  await page.locator(".product-config-source-snapshot-hint").first().click({ timeout: 5000 }).catch(() => undefined);
  await page.waitForFunction(() => (
    Boolean(document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]'))
    && Boolean(document.querySelector('[aria-label="Source Digest 命中路径预览"]'))
  ), null, { timeout: 45000 }).catch(() => undefined);
  const directPicker = page.getByRole("combobox", { name: SOURCE_DIGEST_PICKER_LABEL });
  if (await directPicker.count() > 0) {
    await directPicker.first().click({ timeout: 15000 });
    await page.waitForFunction(() => {
      const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
      const menu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
      return Boolean(menu?.querySelector('[role="option"]'));
    }, null, { timeout: 15000 }).catch(() => undefined);
    const modelFocusOption = page.locator('[role="option"]').filter({ hasText: "T19C MY ICE" }).filter({ hasText: /聚焦 Model|聚焦车型/i }).first();
    if (await modelFocusOption.count() > 0) {
      await modelFocusOption.click({ timeout: 15000 }).catch(() => undefined);
      await page.waitForFunction(() => {
        const text = document.body.innerText.replace(/\s+/g, " ").toUpperCase();
        return (text.includes("聚焦 MODEL") || text.includes("聚焦车型")) && text.includes("T19C MY ICE");
      }, null, { timeout: 15000 }).catch(() => undefined);
      const focusedPicker = page.getByRole("combobox", { name: SOURCE_DIGEST_PICKER_LABEL });
      if (await focusedPicker.count() > 0) {
        await focusedPicker.first().click({ timeout: 15000 }).catch(() => undefined);
        await page.waitForFunction(() => {
          const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
          const menu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
          return Boolean(menu?.querySelector('[role="option"]'));
        }, null, { timeout: 15000 }).catch(() => undefined);
      }
    }
  }

  return page.evaluate((expectedQuery) => {
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
    const compact = (value) => normalize(value).replace(/\s+/g, "");
    const bodyText = normalize(document.body.innerText);
    const bodyCompact = compact(bodyText);
    const bodyUpper = bodyText.toUpperCase();
    const bodyCompactUpper = bodyCompact.toUpperCase();
    const pathPreview = document.querySelector('[aria-label="Source Digest 命中路径预览"]');
    const pathText = normalize(pathPreview?.textContent || "");
    const pathCompact = compact(pathText);
    const pathCompactUpper = pathCompact.toUpperCase();
    const detailBrowser = document.querySelector('[aria-label="来源组详情浏览"]');
    const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const pickerMenu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    const options = Array.from(pickerMenu?.querySelectorAll('[role="option"]') ?? []);
    const optionTexts = options.map((option) => normalize(option.textContent || ""));
    const optionCompact = optionTexts.map((text) => compact(text));
    const optionUpper = optionTexts.map((text) => text.toUpperCase());
    const optionCompactUpper = optionCompact.map((text) => text.toUpperCase());
    const containsInAnyOption = (needle) => optionUpper.some((text) => text.includes(needle.toUpperCase()));
    const containsCompactInAnyOption = (needle) => optionCompactUpper.some((text) => text.includes(needle.toUpperCase()));
    const comparableColumnMatch = bodyCompactUpper.match(
      /当前(?:SOURCE|来源)范围1来源·(?:13(?:MODEL|车型)·(3[6-9]|[4-9]\d)可比配置列|1(?:MODEL|车型)·3可比配置列)/,
    );
    const pathSummaryMatch = (
      /命中路径13个(?:SOURCE|来源)路径·13(?:MODEL|车型)·(3[6-9]|[4-9]\d)可比配置列/.test(pathCompactUpper)
      || /命中路径1个(?:SOURCE|来源)路径·1(?:MODEL|车型)·3可比配置列/.test(pathCompactUpper)
    );

    return {
      query: expectedQuery,
      optionCount: options.length,
      firstOptionTexts: optionTexts.slice(0, 8),
      pathPreviewText: pathText.slice(0, 1800),
      sourceScopeText: (bodyText.match(/当前(?: Source|来源)范围[^清]+/) || [""])[0].slice(0, 240),
      sourceHitText: (bodyText.match(/来源命中[^命]+命中 文件[^选]+/) || [""])[0].slice(0, 360),
      detailBrowserCollapsed: detailBrowser instanceof HTMLDetailsElement ? detailBrowser.open === false : true,
      queryShown: bodyText.includes(expectedQuery),
      sourceScopeOk: Boolean(comparableColumnMatch),
      sourceHitSummaryOk: (
        bodyCompact.includes("可比组13")
        && bodyCompact.includes("候选配置列37")
        && bodyCompact.includes("配置项2912")
        && bodyCompact.includes("差异348")
      ),
      pathSummaryOk: pathSummaryMatch,
      previewShowsMultipleModels: (
        pathText.includes("T19C MY ICE")
        && pathText.includes("E0Y")
        && pathText.includes("E03")
        && pathText.includes("T1EJ -ICE")
      ),
      modelFocusApplied: (bodyUpper.includes("聚焦 MODEL") || bodyText.includes("聚焦车型")) && bodyText.includes("T19C MY ICE"),
      focusedModelColumnPathOk: (
        pathText.includes("T19C MY ICE")
        && pathText.includes("两驱基本型 Basic-FWD")
        && pathText.includes("两驱舒适型 Comfort-FWD")
        && pathText.includes("两驱尊贵型 Premium-FWD")
        && pathText.includes("3 可比配置列")
      ),
      focusActionsOk: (
        (bodyUpper.includes("聚焦 SOURCE") || bodyText.includes("聚焦来源"))
        && (bodyUpper.includes("聚焦 MODEL") || bodyText.includes("聚焦车型"))
        && (bodyUpper.includes("筛选 BRAND") || bodyText.includes("筛选品牌"))
        && (bodyUpper.includes("筛选 MARKET") || bodyText.includes("筛选市场"))
      ),
      pickerHasManyOptions: options.length >= 6,
      pickerShowsSourceModelColumnPaths: (
        (containsInAnyOption("聚焦 Source") || containsInAnyOption("聚焦来源"))
        && (containsInAnyOption("聚焦 Model") || containsInAnyOption("聚焦车型"))
        && (containsCompactInAnyOption("生成配置列") || containsCompactInAnyOption("暂选配置列") || containsCompactInAnyOption("可比配置列"))
      ),
      noSourceDetailEagerLoad: detailBrowser instanceof HTMLDetailsElement ? detailBrowser.open === false : true,
    };
  }, query);
}

async function evaluateMultiSourceSameModelSearch(page, query = "BMW X7") {
  await openFloatingDeck(page);
  await clickSourceDigestTab(page);
  const releaseFocusButton = page.getByRole("button", { name: "解除来源聚焦" });
  if (await releaseFocusButton.count() > 0) {
    await releaseFocusButton.first().click({ timeout: 5000 }).catch(() => undefined);
  }
  const searchInput = page.getByRole("combobox", { name: SOURCE_DIGEST_SEARCH_LABEL });
  await searchInput.waitFor({ state: "visible", timeout: 15000 });
  await searchInput.fill(query);
  await page.waitForFunction((expectedQuery) => {
    const text = document.body.innerText.replace(/\s+/g, " ").toUpperCase();
    return text.includes(expectedQuery.toUpperCase())
      && (text.includes("当前 SOURCE 范围") || text.includes("当前来源范围"))
      && text.includes("来源命中")
      && text.includes("4 来源")
      && (text.includes("1 MODEL") || text.includes("2 MODEL") || text.includes("1 车型") || text.includes("2 车型"))
      && text.includes("8 可比配置列");
  }, query, { timeout: 45000 });
  const directPicker = page.getByRole("combobox", { name: SOURCE_DIGEST_PICKER_LABEL });
  await directPicker.waitFor({ state: "visible", timeout: 45000 });
  await directPicker.click({ timeout: 15000 });
  await page.waitForFunction(() => {
    const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const menu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    return Boolean(menu?.querySelectorAll('[role="option"]').length);
  }, null, { timeout: 15000 });

  return page.evaluate((expectedQuery) => {
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
    const compact = (value) => normalize(value).replace(/\s+/g, "");
    const bodyText = normalize(document.body.innerText);
    const bodyUpper = bodyText.toUpperCase();
    const bodyCompactUpper = compact(bodyText).toUpperCase();
    const pathPreview = document.querySelector('[aria-label="Source Digest 命中路径预览"]');
    const pathText = normalize(pathPreview?.textContent || "");
    const pathCompactUpper = compact(pathText).toUpperCase();
    const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const pickerMenu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    const options = Array.from(pickerMenu?.querySelectorAll('[role="option"]') ?? []);
    const optionTexts = options.map((option) => normalize(option.textContent || ""));
    const optionUpper = optionTexts.map((text) => text.toUpperCase());
    const optionCompactUpper = optionTexts.map((text) => compact(text).toUpperCase());
    const countOccurrences = (haystack, needle) => (haystack.match(new RegExp(needle, "g")) || []).length;
    const aliasSourceCount = countOccurrences(bodyUpper, "BMW-X7-COMPETITOR-ALIAS-CONFIG");
    const uniqueAliasSources = new Set(
      Array.from(bodyText.matchAll(/bmw-x7-competitor-alias-config-\d+\.csv/gi)).map((match) => match[0].toLowerCase()),
    );
    const sourceScopeOk = /当前(?:SOURCE|来源)范围4来源·[12](?:MODEL|车型)·8可比配置列/.test(bodyCompactUpper);
    const pathSummaryOk = /命中路径4个(?:SOURCE|来源)路径·[12](?:MODEL|车型)·8可比配置列/.test(pathCompactUpper);
    const hasCrossSourceModelFocus = optionUpper.some((text) => (
      (text.includes("聚焦同名 MODEL") || text.includes("聚焦同名车型"))
      && text.includes("跨来源")
      && (text.includes("3 来源") || text.includes("4 来源"))
      && (text.includes("6 配置列") || text.includes("8 配置列") || text.includes("8 可比配置列"))
    ));
    const hasSourceSpecificModelFocus = optionUpper.some((text) => (
      (text.includes("聚焦 MODEL") || text.includes("聚焦车型"))
      && text.includes("BMW-X7-COMPETITOR-ALIAS-CONFIG")
      && text.includes("2 可比配置列")
    ));
    const hasSourceFocusOptions = optionUpper.filter((text) => (
      (text.includes("聚焦来源") || text.includes("聚焦 SOURCE"))
      && text.includes("BMW-X7-COMPETITOR")
    )).length >= 4;
    const hasColumnOptions = optionCompactUpper.some((text) => (
      (text.includes("整组生成配置列") || text.includes("整组配置列生成配置列"))
      && text.includes("BMWX7XDRIVE40I")
      && text.includes("BMWX7M60I")
    )) && optionCompactUpper.some((text) => text.includes("暂选配置列"));

    return {
      query: expectedQuery,
      optionCount: options.length,
      firstOptionTexts: optionTexts.slice(0, 12),
      pathPreviewText: pathText.slice(0, 1800),
      sourceScopeText: (bodyText.match(/当前(?: SOURCE|来源)范围[^清]+/) || [""])[0].slice(0, 240),
      sourceHitText: (bodyText.match(/来源命中[^选]+/) || [""])[0].slice(0, 1000),
      queryShown: bodyUpper.includes(expectedQuery.toUpperCase()),
      sourceScopeOk,
      sourceHitSummaryOk: (
        bodyUpper.includes("已匹配 4 个来源")
        && bodyUpper.includes("4 个可转配置列来源")
        && uniqueAliasSources.size >= 3
        && aliasSourceCount >= 3
      ),
      pathSummaryOk,
      crossSourceModelFocusOk: hasCrossSourceModelFocus,
      sourceSpecificModelFocusOk: hasSourceSpecificModelFocus,
      sourceFocusOptionsOk: hasSourceFocusOptions,
      columnOptionsOk: hasColumnOptions,
      pickerHasManyOptions: options.length >= 20,
    };
  }, query);
}

async function waitForChecks(page, expectedRows, timeoutMs) {
  const startedAt = Date.now();
  let lastResult = null;
  while (Date.now() - startedAt < timeoutMs) {
    const bodyText = await page.locator("body").innerText({ timeout: 5000 });
    const overflow = await page.evaluate(() => ({
      viewportWidth: window.innerWidth,
      documentScrollWidth: document.documentElement.scrollWidth,
      bodyScrollWidth: document.body.scrollWidth,
      hasDocumentOverflow: document.documentElement.scrollWidth > window.innerWidth + 2,
      hasBodyOverflow: document.body.scrollWidth > window.innerWidth + 2,
    }));
    const checks = evaluateChecks(bodyText, expectedRows, overflow);
    lastResult = { bodyText, overflow, checks };
    if (Object.values(checks).every(Boolean)) return lastResult;
    await page.waitForTimeout(500);
  }
  return lastResult;
}

function selectedViewports(viewportOption) {
  if (viewportOption === "all") return VIEWPORTS;
  const selected = VIEWPORTS.find((viewport) => viewport.key === viewportOption);
  if (!selected) {
    throw new Error(`Unknown --viewport=${viewportOption}; expected all, desktop, or mobile.`);
  }
  return [selected];
}

async function runViewportSmoke(browser, options, targetUrl, artifactDir, viewport) {
  const observed = { requests: [], responses: [] };
  const context = await browser.newContext({ viewport: { width: viewport.width, height: viewport.height } });
  await context.addInitScript(() => {
    window.localStorage.setItem("jato_user_role", "admin");
    window.localStorage.setItem("product_config_summary_mode", "simple");
    window.localStorage.setItem("jato_product_config_summary_mode", "simple");
    window.__productConfigDownloads = [];
    const originalCreateObjectURL = window.URL.createObjectURL.bind(window.URL);
    window.URL.createObjectURL = (object) => {
      const url = originalCreateObjectURL(object);
      if (object instanceof Blob) {
        const record = {
          url,
          type: object.type || "",
          size: object.size,
          firstBytes: [],
          ready: false,
          error: "",
        };
        window.__productConfigDownloads.push(record);
        object.slice(0, 8).arrayBuffer().then((buffer) => {
          record.firstBytes = Array.from(new Uint8Array(buffer));
          record.ready = true;
        }).catch((error) => {
          record.error = error instanceof Error ? error.message : String(error);
          record.ready = true;
        });
      }
      return url;
    };
  });
  const page = await context.newPage();
  page.on("request", (request) => {
    const url = request.url();
    if (isObservedProductConfigRequest(url)) {
      const requestRecord = {
        url,
        method: request.method(),
      };
      if (url.includes("/engineering-config/business-summary/compose") && request.method() === "POST") {
        try {
          requestRecord.payloadSummary = summarizeAiComposePayload(request.postDataJSON());
        } catch (error) {
          requestRecord.payloadSummaryError = error instanceof Error ? error.message : String(error);
        }
      }
      observed.requests.push(requestRecord);
    }
  });
  page.on("response", (response) => {
    const url = response.url();
    if (isObservedProductConfigRequest(url)) {
      observed.responses.push({
        url,
        status: response.status(),
        method: response.request().method(),
      });
    }
  });

  try {
    await page.goto(targetUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
    const result = await waitForChecks(page, options.expectedRows, options.timeoutMs);
    const initialRangeChips = await evaluateRangeChipVisibility(page);
    const initialLoadDeferral = await evaluateInitialLoadDeferral(page, observed.requests);
    const initialSimpleAiSummaryLayout = await evaluateSimpleAiSummaryLayout(page);
    const compactAiCardLayout = await evaluateCompactAiCardLayout(page);
    const initialScreenshotPath = path.join(artifactDir, `product_config_t19c_ai_smoke_${viewport.key}_initial.png`);
    await page.screenshot({ path: initialScreenshotPath, fullPage: true });
    const simpleTableNavigator = await evaluateSimpleTableNavigator(page, options.expectedRows);
    const compactAiEvidenceBoundary = await evaluateCompactAiEvidenceBoundary(page);
    const floatingDeckLayout = viewport.checkDeck ? await evaluateFloatingDeckLayout(page) : null;
    const directSearchLayout = viewport.checkDeck ? await evaluateMobileDirectSearchLayout(page) : null;
    const directSearchSpecificLayout = viewport.checkDeck ? await evaluateMobileDirectSearchLayout(page, "T19C-BEV") : null;
    const sourceDigestSearchLayout = viewport.checkDeck ? await evaluateMobileSourceDigestSearchLayout(page) : null;
    const largeSourceDigestSearch = viewport.checkLargeSourceSearch ? await evaluateLargeSourceDigestSearch(page) : null;
    const multiSourceSameModelSearch = viewport.checkMultiSourceSameModelSearch ? await evaluateMultiSourceSameModelSearch(page) : null;
    const deckEditGate = viewport.checkExports ? await evaluateDeckEditGate(page) : null;
    const deckExportSmoke = viewport.checkExports ? await evaluateDeckExports(page) : null;
    const uploadDiagnostics = viewport.checkUploadDiagnostics ? await evaluateUploadDiagnosticsDeferred(page) : null;
    const finalRangeChips = await evaluateRangeChipVisibility(page);
    const screenshotPath = path.join(artifactDir, `product_config_t19c_ai_smoke_${viewport.key}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: true });
    const checks = result?.checks ?? {};
    const compareResponseOk = observed.responses.some((response) => (
      response.url.includes("/engineering-config/compare") && response.method === "GET" && response.status === 200
    ));
    const aiComposeResponseOk = observed.responses.some((response) => (
      response.url.includes("/engineering-config/business-summary/compose") && response.method === "POST" && response.status === 200
    ));
    const aiComposePayloadSummary = observed.requests
      .filter((request) => request.url.includes("/engineering-config/business-summary/compose") && request.method === "POST")
      .map((request) => request.payloadSummary)
      .find(Boolean) ?? null;
    const aiReadinessDeferred = observed.requests.every((request) => (
      !request.url.includes("/engineering-config/business-summary/readiness")
    ));
    const ocrReadinessDeferred = observed.requests.every((request) => (
      !request.url.includes("/engineering-config/ocr/readiness")
    ));
    const layoutChecks = {
      hasMultipleCompactAiCards: compactAiCardLayout.compactCardCount >= 2,
      allCompactAiCardsCollapsedByDefault: compactAiCardLayout.allCardsCollapsedByDefault,
      collapsedAiCardsStayCompact: compactAiCardLayout.collapsedCardsStayCompact,
      quickEvidenceButtonHorizontal: compactAiCardLayout.quickEvidenceButtonsHorizontal,
      compactEvidenceCardCollapsedByDefault: compactAiEvidenceBoundary.cardCollapsedByDefault,
      compactEvidenceQuickActionVisible: compactAiEvidenceBoundary.quickEvidenceVisibleWhileCollapsed,
      compactEvidenceInlineBoundaryHidden: compactAiEvidenceBoundary.inlineEvidenceBoundaryHidden,
    };
    const rangeChipChecks = {
      topFullRangeChipFound: initialRangeChips.heroFullRangeChipFound,
      topFullRangeChipVisible: initialRangeChips.heroFullRangeChipVisible,
      floatingDeckFullRangeChipFound: finalRangeChips.floatingDeckFullRangeChipFound,
      floatingDeckFullRangeChipVisible: finalRangeChips.floatingDeckFullRangeChipVisible,
    };
    const simpleSummaryChecks = {
      simpleAiOnlyPanelActive: initialSimpleAiSummaryLayout.simpleAiOnlyPanelActive,
      simpleAiSummaryVisible: initialSimpleAiSummaryLayout.aiSummaryVisible,
      simpleCompactAiCardsVisible: initialSimpleAiSummaryLayout.compactAiCardCount >= 1,
      simpleDeterministicRuleBlocksHidden: initialSimpleAiSummaryLayout.deterministicRuleLabels.length === 0,
    };
    const initialLoadDeferralChecks = {
      initialConfigLibraryDeferred: initialLoadDeferral.configLibraryDeferred,
      initialSourceLibraryDeferred: initialLoadDeferral.sourceLibraryDeferred,
      initialCompetitorRecommendationsDeferred: initialLoadDeferral.competitorRecommendationsDeferred,
      initialSourceDigestControlsHidden: initialLoadDeferral.sourceDigestControlsHidden,
      initialAdvancedRecommendationsHidden: initialLoadDeferral.advancedRecommendationsHidden,
    };
    const simpleTableNavigatorChecks = {
      simpleTableNavigatorDefaultFullRows: simpleTableNavigator.defaultFullRowsVisible,
      simpleTableNavigatorHiddenByDefault: simpleTableNavigator.defaultNavigatorHidden,
      simpleTableNavigatorDifferenceScopeReached: simpleTableNavigator.differenceScopeReached,
      simpleTableNavigatorVisibleInDifferenceScope: simpleTableNavigator.differenceNavigatorVisible,
      simpleTableNavigatorCopyHiddenBeforeRowFocus: simpleTableNavigator.copySelectedHiddenBeforeRowFocus,
      simpleTableNavigatorCopyVisibleAfterRowFocus: simpleTableNavigator.selectedRowCopyVisible,
      simpleTableNavigatorRestoresFullRows: simpleTableNavigator.restoredFullRowsVisible,
      simpleTableNavigatorHiddenAfterRestore: simpleTableNavigator.restoredNavigatorHidden,
    };
    const aiComposePayloadChecks = aiComposePayloadSummary ? {
      aiComposePayloadHasCompareScope: aiComposePayloadSummary.hasContextCompareScope,
      aiComposePayloadHasSourceEvidenceSummary: aiComposePayloadSummary.allTargetsHaveSourceEvidenceSummary,
      aiComposePayloadHasEvidenceFacts: aiComposePayloadSummary.allTargetsHaveEvidenceFacts,
      aiComposePayloadHasRichEvidenceFacts: aiComposePayloadSummary.allTargetsHaveRichEvidenceFacts,
      aiComposePayloadHasCategoryFacts: aiComposePayloadSummary.allTargetsHaveCategoryFacts,
      aiComposePayloadHasEvidenceBoundarySignal: aiComposePayloadSummary.hasEvidenceBoundarySignal,
    } : {
      aiComposePayloadHasCompareScope: false,
      aiComposePayloadHasSourceEvidenceSummary: false,
      aiComposePayloadHasEvidenceFacts: false,
      aiComposePayloadHasRichEvidenceFacts: false,
      aiComposePayloadHasCategoryFacts: false,
      aiComposePayloadHasEvidenceBoundarySignal: false,
    };
    const deckChecks = floatingDeckLayout ? {
      floatingDeckPanelVisible: floatingDeckLayout.hasPanel,
      floatingDeckPanelWidthFitsViewport: floatingDeckLayout.panelWidthFitsViewport,
      floatingDeckDrawerWidthFitsViewport: floatingDeckLayout.drawerWidthFitsViewport,
      floatingDeckToggleWidthFitsViewport: floatingDeckLayout.toggleWidthFitsViewport,
      floatingDeckTabsWidthFitsViewport: floatingDeckLayout.tabsWidthFitsViewport,
      floatingDeckPanelHeightReasonable: floatingDeckLayout.panelHeightReasonable,
    } : {};
    const directSearchChecks = directSearchLayout ? {
      directSearchInputWidthFitsViewport: directSearchLayout.inputWidthFitsViewport,
      directSearchMenuWidthFitsViewport: directSearchLayout.menuWidthFitsViewport,
      directSearchFirstOptionFitsViewport: directSearchLayout.firstOptionWidthFitsViewport,
      directSearchHasOptions: directSearchLayout.optionCount > 0,
      directSearchFirstOptionReadableHeight: directSearchLayout.firstOptionReadableHeight,
    } : {};
    const directSearchSpecificChecks = directSearchSpecificLayout ? {
      directSearchSpecificInputWidthFitsViewport: directSearchSpecificLayout.inputWidthFitsViewport,
      directSearchSpecificMenuWidthFitsViewport: directSearchSpecificLayout.menuWidthFitsViewport,
      directSearchSpecificFirstOptionFitsViewport: directSearchSpecificLayout.firstOptionWidthFitsViewport,
      directSearchSpecificHasOptions: directSearchSpecificLayout.optionCount > 0,
      directSearchSpecificContainsExpectedModel: directSearchSpecificLayout.containsExpectedQuery,
      directSearchSpecificExcludesBmwNoise: !directSearchSpecificLayout.containsIrrelevantBmw,
      directSearchSpecificExcludesRuntimeNoise: !directSearchSpecificLayout.containsRuntimeSmoke,
    } : {};
    const uploadDiagnosticsChecks = uploadDiagnostics ? {
      uploadDiagnosticsCollapsed: uploadDiagnostics.diagnosticsOpen === false,
      uploadDiagnosticsCardsDeferred: !uploadDiagnostics.hasOcrReadinessCard && !uploadDiagnostics.hasAiReadinessCard,
      uploadPanelVisible: uploadDiagnostics.hasUploadPanel,
    } : {};
    const largeSourceDigestSearchChecks = largeSourceDigestSearch ? {
      largeSourceQueryShown: largeSourceDigestSearch.queryShown,
      largeSourceScopeOk: largeSourceDigestSearch.sourceScopeOk,
      largeSourceHitSummaryOk: largeSourceDigestSearch.sourceHitSummaryOk,
      largeSourcePathSummaryOk: largeSourceDigestSearch.pathSummaryOk,
      largeSourceModelFocusApplied: largeSourceDigestSearch.modelFocusApplied,
      largeSourceFocusedModelColumnPathOk: largeSourceDigestSearch.focusedModelColumnPathOk,
      largeSourceFocusActionsOk: largeSourceDigestSearch.focusActionsOk,
      largeSourcePickerHasManyOptions: largeSourceDigestSearch.pickerHasManyOptions,
      largeSourcePickerShowsSourceModelColumnPaths: largeSourceDigestSearch.pickerShowsSourceModelColumnPaths,
      largeSourcePathPreviewAvailable: largeSourceDigestSearch.pathPreviewText.length > 0,
    } : {};
    const multiSourceSameModelSearchChecks = multiSourceSameModelSearch ? {
      multiSourceSameModelQueryShown: multiSourceSameModelSearch.queryShown,
      multiSourceSameModelScopeOk: multiSourceSameModelSearch.sourceScopeOk,
      multiSourceSameModelHitSummaryOk: multiSourceSameModelSearch.sourceHitSummaryOk,
      multiSourceSameModelPathSummaryOk: multiSourceSameModelSearch.pathSummaryOk,
      multiSourceSameModelCrossSourceFocusOk: multiSourceSameModelSearch.crossSourceModelFocusOk,
      multiSourceSameModelSourceSpecificFocusOk: multiSourceSameModelSearch.sourceSpecificModelFocusOk,
      multiSourceSameModelSourceFocusOptionsOk: multiSourceSameModelSearch.sourceFocusOptionsOk,
      multiSourceSameModelColumnOptionsOk: multiSourceSameModelSearch.columnOptionsOk,
      multiSourceSameModelPickerHasManyOptions: multiSourceSameModelSearch.pickerHasManyOptions,
    } : {};
    const sourceDigestSearchChecks = sourceDigestSearchLayout ? {
      sourceDigestSearchInputWidthFitsViewport: sourceDigestSearchLayout.searchInputWidthFitsViewport,
      sourceDigestPickerInputWidthFitsViewport: sourceDigestSearchLayout.pickerInputWidthFitsViewport,
      sourceDigestPickerMenuWidthFitsViewport: sourceDigestSearchLayout.pickerMenuWidthFitsViewport,
      sourceDigestPickerFirstOptionFitsViewport: sourceDigestSearchLayout.firstOptionWidthFitsViewport,
      sourceDigestPickerHasOptions: sourceDigestSearchLayout.optionCount > 0,
      sourceDigestPickerFirstOptionReadableHeight: sourceDigestSearchLayout.firstOptionReadableHeight,
      sourceDigestPathPreviewWidthFitsViewport: sourceDigestSearchLayout.pathPreviewWidthFitsViewport,
    } : {};
    const exportChecks = deckExportSmoke ? {
      xlsxDeckExportOk: deckExportSmoke.xlsxExportOk,
      pdfDeckExportOk: deckExportSmoke.pdfExportOk,
      xlsxDeckExportPayloadKeepsAiSummary: deckExportSmoke.xlsxPayloadOk,
      pdfDeckExportPayloadKeepsAiSummary: deckExportSmoke.pdfPayloadOk,
    } : {};
    const editChecks = deckEditGate ? {
      floatingDeckEditGateOk: deckEditGate.editControlOk,
    } : {};
    const allChecks = {
      ...checks,
      compareResponseOk,
      aiComposeResponseOk,
      aiReadinessDeferred,
      ocrReadinessDeferred,
      ...layoutChecks,
      ...rangeChipChecks,
      ...simpleSummaryChecks,
      ...initialLoadDeferralChecks,
      ...simpleTableNavigatorChecks,
      ...aiComposePayloadChecks,
      ...deckChecks,
      ...directSearchChecks,
      ...directSearchSpecificChecks,
      ...sourceDigestSearchChecks,
      ...largeSourceDigestSearchChecks,
      ...multiSourceSameModelSearchChecks,
      ...uploadDiagnosticsChecks,
      ...editChecks,
      ...exportChecks,
    };
    return {
      key: viewport.key,
      label: viewport.label,
      viewport: { width: viewport.width, height: viewport.height },
      passed: Object.values(allChecks).every(Boolean),
      checks: allChecks,
      overflow: result?.overflow ?? null,
      rangeChips: {
        initial: initialRangeChips,
        final: finalRangeChips,
      },
      initialLoadDeferral,
      initialSimpleAiSummaryLayout,
      simpleTableNavigator,
      aiComposePayloadSummary,
      compactAiCardLayout,
      compactAiEvidenceBoundary,
      floatingDeckLayout,
      directSearchLayout,
      directSearchSpecificLayout,
      sourceDigestSearchLayout,
      largeSourceDigestSearch,
      multiSourceSameModelSearch,
      uploadDiagnostics,
      deckEditGate,
      deckExportSmoke,
      requests: observed.requests,
      responses: observed.responses,
      initialScreenshotPath,
      screenshotPath,
    };
  } catch (error) {
    const screenshotPath = path.join(artifactDir, `product_config_t19c_ai_smoke_${viewport.key}_failure.png`);
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => undefined);
    const aiComposePayloadSummary = observed.requests
      .filter((request) => request.url.includes("/engineering-config/business-summary/compose") && request.method === "POST")
      .map((request) => request.payloadSummary)
      .find(Boolean) ?? null;
    return {
      key: viewport.key,
      label: viewport.label,
      viewport: { width: viewport.width, height: viewport.height },
      passed: false,
      error: error instanceof Error ? error.message : String(error),
      aiComposePayloadSummary,
      requests: observed.requests,
      responses: observed.responses,
      screenshotPath,
    };
  } finally {
    await context.close();
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    console.log(usage());
    return;
  }
  if (options.trimIds.length < 2) {
    console.error("At least two --trim-ids are required.");
    process.exit(2);
  }
  const viewportRuns = selectedViewports(options.viewport);

  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-t19c-ai-smoke", nowStamp());
  await mkdir(artifactDir, { recursive: true });

  const targetUrl = new URL(`${options.baseUrl}/product/compare/config`);
  targetUrl.searchParams.set("trimIds", options.trimIds.join(","));
  targetUrl.searchParams.set("baseTrimId", options.baseTrimId);

  const browser = await chromium.launch({
    headless: !options.headed,
    ...(options.browserChannel ? { channel: options.browserChannel } : {}),
  });

  try {
    const viewportResults = [];
    for (const viewport of viewportRuns) {
      viewportResults.push(await runViewportSmoke(browser, options, targetUrl, artifactDir, viewport));
    }
    const desktopResult = viewportResults.find((result) => result.key === "desktop") ?? viewportResults[0] ?? null;
    const passed = viewportResults.length > 0 && viewportResults.every((result) => result.passed);
    const summary = {
      passed,
      targetUrl: targetUrl.toString(),
      trimIds: options.trimIds,
      baseTrimId: options.baseTrimId,
      expectedRows: options.expectedRows,
      viewportMode: options.viewport,
      viewportResults,
      checks: desktopResult?.checks ?? null,
      overflow: desktopResult?.overflow ?? null,
      compactAiCardLayout: desktopResult?.compactAiCardLayout ?? null,
      requests: viewportResults.flatMap((result) => result.requests ?? []),
      responses: viewportResults.flatMap((result) => result.responses ?? []),
      initialScreenshotPath: desktopResult?.initialScreenshotPath ?? null,
      initialScreenshotPaths: Object.fromEntries(viewportResults.map((result) => [result.key, result.initialScreenshotPath])),
      screenshotPath: desktopResult?.screenshotPath ?? null,
      screenshotPaths: Object.fromEntries(viewportResults.map((result) => [result.key, result.screenshotPath])),
    };
    const summaryPath = path.join(artifactDir, "product_config_t19c_ai_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    if (!passed) {
      console.error(`Product config T19C AI smoke failed. Summary: ${summaryPath}`);
      process.exitCode = 1;
    } else {
      console.log(`Product config T19C AI smoke passed. Summary: ${summaryPath}`);
    }
  } catch (error) {
    const summaryPath = path.join(artifactDir, "product_config_t19c_ai_smoke.json");
    const summary = {
      passed: false,
      targetUrl: targetUrl.toString(),
      trimIds: options.trimIds,
      baseTrimId: options.baseTrimId,
      expectedRows: options.expectedRows,
      error: error instanceof Error ? error.message : String(error),
    };
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.error(`Product config T19C AI smoke failed. Summary: ${summaryPath}`);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exit(1);
});
