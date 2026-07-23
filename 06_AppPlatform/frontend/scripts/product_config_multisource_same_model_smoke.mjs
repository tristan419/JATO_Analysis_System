import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "http://127.0.0.1:8004/v1",
    country: process.env.PRODUCT_CONFIG_MULTISOURCE_COUNTRY || "",
    model: process.env.PRODUCT_CONFIG_MULTISOURCE_MODEL || "",
    brand: process.env.PRODUCT_CONFIG_MULTISOURCE_BRAND || "MultiSourceSmoke",
    powertrain: process.env.PRODUCT_CONFIG_MULTISOURCE_POWERTRAIN || "ICE",
    segment: process.env.PRODUCT_CONFIG_MULTISOURCE_SEGMENT || "Smoke Segment",
    userName: process.env.PRODUCT_CONFIG_SMOKE_USER || "product-config-multisource-smoke",
    browserChannel: process.env.PRODUCT_CONFIG_SMOKE_BROWSER_CHANNEL || "",
    headed: false,
    write: process.env.PRODUCT_CONFIG_SMOKE_WRITE === "1",
    cleanup: process.env.PRODUCT_CONFIG_SMOKE_CLEANUP !== "0",
    timeoutMs: 180000,
    help: false,
  };
  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") options.help = true;
    else if (arg === "--headed") options.headed = true;
    else if (arg === "--write") options.write = true;
    else if (arg === "--skip-cleanup") options.cleanup = false;
    else if (arg.startsWith("--base-url=")) options.baseUrl = arg.slice("--base-url=".length);
    else if (arg.startsWith("--api-base=")) options.apiBase = arg.slice("--api-base=".length);
    else if (arg.startsWith("--country=")) options.country = arg.slice("--country=".length);
    else if (arg.startsWith("--model=")) options.model = arg.slice("--model=".length);
    else if (arg.startsWith("--brand=")) options.brand = arg.slice("--brand=".length);
    else if (arg.startsWith("--powertrain=")) options.powertrain = arg.slice("--powertrain=".length);
    else if (arg.startsWith("--segment=")) options.segment = arg.slice("--segment=".length);
    else if (arg.startsWith("--user-name=")) options.userName = arg.slice("--user-name=".length);
    else if (arg.startsWith("--channel=")) options.browserChannel = arg.slice("--channel=".length);
    else if (arg.startsWith("--timeout-ms=")) options.timeoutMs = Math.max(30000, Number(arg.slice("--timeout-ms=".length)) || options.timeoutMs);
  }
  options.baseUrl = options.baseUrl.replace(/\/+$/, "");
  options.apiBase = options.apiBase.replace(/\/+$/, "");
  return options;
}

function usage() {
  return [
    "Visible Product Config multi-source same-model smoke.",
    "",
    "Creates two temporary CSV source snapshots for the same country/model with identical Basic/Premium trim names,",
    "leaves material numbers blank, verifies FloatingDeck Source Digest search keeps both source paths visible,",
    "creates editable config columns from both sources, opens formal compare, then cleans up.",
    "",
    "Run only against local/staging data:",
    "",
    "  node scripts/product_config_multisource_same_model_smoke.mjs --write \\",
    "    --base-url=http://127.0.0.1:5177 \\",
    "    --api-base=http://127.0.0.1:8004/v1",
    "",
    "Options:",
    "  --write                 Required. Acknowledges temporary source/config-column writes.",
    "  --skip-cleanup          Keep generated source/config columns for manual inspection.",
    "  --headed                Show the browser.",
    "  --channel=chrome        Use an installed browser channel instead of Playwright's bundled browser.",
    "  --country='Smoke ...'   Country/market. Defaults to a unique smoke country.",
    "  --model='Smoke ...'     Same-model test target. Defaults to a unique smoke model.",
  ].join("\n");
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function csvCell(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function compactText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function smokeHeaders(options, contentType = "application/json") {
  return {
    ...(contentType ? { "Content-Type": contentType } : {}),
    "X-User-Name": options.userName,
    ...(process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN ? { "X-Auth-Token": process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN } : {}),
  };
}

async function apiJsonRequest(options, method, apiPath, body) {
  const response = await fetch(`${options.apiBase}${apiPath}`, {
    method,
    headers: smokeHeaders(options, body === undefined ? "" : "application/json"),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(`${method} ${apiPath} failed with HTTP ${response.status}: ${JSON.stringify(payload)}`);
  }
  return payload;
}

async function apiBinaryRequest(options, method, apiPath, body, contentType) {
  const response = await fetch(`${options.apiBase}${apiPath}`, {
    method,
    headers: smokeHeaders(options, contentType),
    body,
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(`${method} ${apiPath} failed with HTTP ${response.status}: ${JSON.stringify(payload)}`);
  }
  return payload;
}

function sourceCsv({ brand, model, country, powertrain, segment, modelYear, sourceLabel }) {
  const sourceSuffix = sourceLabel === "A" ? "Website A" : "Website B";
  const rows = [
    ["Feature", "Basic", "Premium"],
    ["Brand / 品牌", brand, brand],
    ["Model / 车型", model, model],
    ["Country / 国家", country, country],
    ["Model year / 年款", modelYear, modelYear],
    ["Powertrain / 动力", powertrain, powertrain],
    ["Segment / 级别", segment, segment],
    ["Configuration version / 配置版型", "Basic", "Premium"],
    ["Material No. / 物料号", "", ""],
    ["Source site / 来源网站", sourceSuffix, sourceSuffix],
    ["Rear Visual parking assist / 动态辅助线倒车影像", "-", sourceLabel === "A" ? "●" : "-"],
    ["360 round view camera / 360度高清全景影像", "-", sourceLabel === "A" ? "-" : "●"],
    ["Power sunroof / 电动天窗", "-", sourceLabel === "A" ? "O" : "●"],
    ["Wireless charging / 手机无线充电", "-", "●"],
    ["SONY 8 speakers / SONY 8扬声器", "-", sourceLabel === "A" ? "-" : "●"],
    ["Heat pump / 热泵空调", "-", sourceLabel === "A" ? "-" : "O"],
  ];
  return `${rows.map((row) => row.map(csvCell).join(",")).join("\n")}\n`;
}

function digestTrimId(trim) {
  return String(trim?.trimId || trim?.trim_id || trim?.id || "").trim();
}

function extractDigestGroup(uploadPayload) {
  const digest = uploadPayload?.sourceDigest || uploadPayload?.source_digest || null;
  const groups = Array.isArray(digest?.compareGroups) ? digest.compareGroups : [];
  const group = groups.find((item) => Array.isArray(item?.trims) && item.trims.length >= 2 && Array.isArray(item?.rows) && item.rows.length > 0);
  if (!group) {
    throw new Error(`Uploaded source did not produce a comparable Source Digest group: ${JSON.stringify(uploadPayload?.sourceDigestStatus || uploadPayload?.source_digest_status)}`);
  }
  const trimIds = group.trims.map(digestTrimId).filter(Boolean).slice(0, 2);
  if (trimIds.length < 2) {
    throw new Error(`Comparable group did not expose two digest trim ids: ${JSON.stringify(group.trims)}`);
  }
  return {
    digest,
    group,
    groupId: String(group.groupId || group.id || ""),
    trimIds,
  };
}

async function uploadCsvSource(options, { fileName, csv, modelYear, sourceLabel }) {
  const bytes = Buffer.from(csv, "utf8");
  const params = new URLSearchParams({
    file_name: fileName,
    total_size: String(bytes.length),
    mime_type: "text/csv",
  });
  const initiate = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/initiate?${params.toString()}`);
  const uploadId = initiate?.uploadId || initiate?.upload_id;
  if (!uploadId) throw new Error(`Source upload initiate did not return uploadId: ${JSON.stringify(initiate)}`);
  await apiBinaryRequest(options, "PUT", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/parts/0`, bytes, "text/csv");
  const relatedContext = {
    brand: options.brand,
    model: options.model,
    market: options.country,
    country: options.country,
    modelYear,
    powertrain: options.powertrain,
    segment: options.segment,
    contextType: "model_trim_compare_target",
    scenario: "multi_source_same_model_smoke",
    sourceLabel,
  };
  const complete = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/complete`, { relatedContext });
  const sourceId = complete?.sourceId || complete?.source_id || complete?.importBatchId || complete?.import_batch_id;
  if (!sourceId) throw new Error(`Source upload complete did not return sourceId: ${JSON.stringify(complete)}`);
  return { uploadId, sourceId, complete, relatedContext, digestInfo: extractDigestGroup(complete) };
}

async function createDraftFromDigest(options, upload) {
  const result = await apiJsonRequest(
    options,
    "POST",
    `/engineering-config/source/snapshots/${encodeURIComponent(upload.sourceId)}/digest-groups/${encodeURIComponent(upload.digestInfo.groupId)}/draft`,
    { trim_ids: upload.digestInfo.trimIds },
  );
  const compareTrimIds = Array.isArray(result?.compareTrimIds)
    ? result.compareTrimIds
    : Array.isArray(result?.trimIds)
      ? result.trimIds.slice(0, 4)
      : [];
  if (compareTrimIds.length < 2) {
    throw new Error(`Draft creation did not return two compare trim ids: ${JSON.stringify(result)}`);
  }
  return { result, compareTrimIds };
}

async function cleanupArtifacts(options, { sourceIds, trimIds }) {
  const cleanup = {
    enabled: options.cleanup,
    sourceIds,
    trimIds,
    sourceTrashCleared: null,
    trimTrashCleared: null,
    errors: [],
  };
  if (!options.cleanup) return cleanup;
  for (const trimId of trimIds) {
    try {
      await apiJsonRequest(options, "PATCH", `/engineering-config/trims/${encodeURIComponent(trimId)}`, { status: "trashed" });
    } catch (error) {
      cleanup.errors.push({ step: "trash_trim", trimId, message: error instanceof Error ? error.message : String(error) });
    }
  }
  try {
    const result = await apiJsonRequest(options, "DELETE", `/engineering-config/trims/trash?market=${encodeURIComponent(options.country)}`);
    cleanup.trimTrashCleared = result?.cleared ?? null;
  } catch (error) {
    cleanup.errors.push({ step: "clear_trim_trash", message: error instanceof Error ? error.message : String(error) });
  }
  for (const sourceId of sourceIds) {
    try {
      await apiJsonRequest(options, "DELETE", `/engineering-config/source/snapshots/${encodeURIComponent(sourceId)}?country=${encodeURIComponent(options.country)}`);
    } catch (error) {
      cleanup.errors.push({ step: "trash_source", sourceId, message: error instanceof Error ? error.message : String(error) });
    }
  }
  try {
    const result = await apiJsonRequest(options, "DELETE", `/engineering-config/source/trash?country=${encodeURIComponent(options.country)}`);
    cleanup.sourceTrashCleared = result?.cleared ?? null;
  } catch (error) {
    cleanup.errors.push({ step: "clear_source_trash", message: error instanceof Error ? error.message : String(error) });
  }
  return cleanup;
}

async function openFloatingDeck(page) {
  const trigger = page.getByRole("button", { name: /添加配置列\s*\/\s*显示|添加 TRIM\s*\/\s*显示|打开控制/ }).first();
  await trigger.waitFor({ state: "visible", timeout: 30000 });
  await trigger.click();
  await page.locator(".deck-floating-panel").waitFor({ state: "visible", timeout: 30000 });
}

async function evaluateFloatingDeckMultiSourceSearch(page, options, fileNames) {
  await openFloatingDeck(page);
  await page.getByRole("tab", { name: /来源\s*\/\s*上传[\s\S]*配置表[\s\S]*价格单|Source Digest/ }).click({ timeout: 15000 });
  const releaseFocusButton = page.getByRole("button", { name: /解除来源聚焦|清除来源聚焦/ });
  if (await releaseFocusButton.count() > 0) {
    await releaseFocusButton.first().click({ timeout: 5000 }).catch(() => undefined);
  }
  const searchInput = page.getByRole("combobox", { name: /搜索来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|搜索 Source Digest 可比组/ });
  await searchInput.waitFor({ state: "visible", timeout: 15000 });
  await searchInput.fill(options.model);
  await page.waitForFunction(({ model, fileNames: expectedFileNames }) => {
    const text = document.body.innerText.replace(/\s+/g, " ").trim();
    return text.includes(model)
      && expectedFileNames.every((fileName) => text.includes(fileName))
      && /来源命中|当前 SOURCE 范围|当前 Source 范围/.test(text);
  }, { model: options.model, fileNames }, { timeout: options.timeoutMs });

  const directPicker = page.getByRole("combobox", { name: /选择来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|选择 Source \/ Model \/ 配置列/ });
  await directPicker.waitFor({ state: "visible", timeout: options.timeoutMs });
  await directPicker.click({ timeout: 15000 });
  await page.waitForFunction(() => {
    const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const menu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    return Boolean(menu?.querySelectorAll('[role="option"]').length);
  }, null, { timeout: 15000 });

  return page.evaluate(({ model, fileNames: expectedFileNames }) => {
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
    const bodyText = normalize(document.body.innerText);
    const bodyUpper = bodyText.toUpperCase();
    const pickerInput = document.querySelector('input[aria-label="选择 Source / Model / 配置列"], input[aria-label="选择来源 / Model / 配置列"], input[aria-label="选择来源 / 车型 / 配置列"]');
    const pickerMenu = pickerInput?.closest(".comparison-filter-dropdown")?.querySelector(".comparison-filter-dropdown-menu");
    const optionsText = Array.from(pickerMenu?.querySelectorAll('[role="option"]') ?? [])
      .map((option) => normalize(option.textContent || ""));
    const optionsUpper = optionsText.map((text) => text.toUpperCase());
    const pathPreview = normalize(document.querySelector('[aria-label="Source Digest 命中路径预览"]')?.textContent || "");
    const filesVisible = expectedFileNames.every((fileName) => bodyText.includes(fileName));
    const sourceScopeOk = (
      /2\s*来源|2\s*个来源|已匹配\s*2/.test(bodyText)
      || expectedFileNames.every((fileName) => pathPreview.includes(fileName))
    );
    const sourceSpecificOptions = expectedFileNames.filter((fileName) => optionsText.some((text) => text.includes(fileName))).length;
    const modelFocusOptions = optionsUpper.filter((text) => (
      text.includes(model.toUpperCase())
      && (
        text.includes("聚焦 MODEL")
        || text.includes("聚焦同名 MODEL")
        || text.includes("聚焦车型")
        || text.includes("聚焦同名车型")
      )
    )).length;
    const columnOptions = optionsUpper.filter((text) => (
      text.includes("BASIC") || text.includes("PREMIUM") || text.includes("生成配置列") || text.includes("暂选配置列")
    )).length;
    return {
      bodySnippet: bodyText.slice(0, 1600),
      pathPreviewText: pathPreview.slice(0, 1600),
      optionCount: optionsText.length,
      firstOptionTexts: optionsText.slice(0, 16),
      filesVisible,
      sourceScopeOk,
      sourceSpecificOptions,
      modelFocusOptions,
      columnOptions,
      passed: filesVisible && sourceScopeOk && sourceSpecificOptions >= 2 && modelFocusOptions >= 1 && columnOptions >= 2,
    };
  }, { model: options.model, fileNames });
}

async function getCompareData(options, trimIds) {
  return apiJsonRequest(options, "GET", `/engineering-config/compare?trim_ids=${encodeURIComponent(trimIds.join(","))}`);
}

function evaluateCompareApi(compareData, fileNames, trimIds) {
  const trims = Array.isArray(compareData?.trims) ? compareData.trims : [];
  const rows = Array.isArray(compareData?.rows) ? compareData.rows : [];
  const fullNames = trims.map((trim) => String(trim.fullTrimName || trim.full_trim_name || trim.name || ""));
  const trimNames = trims.map((trim) => String(trim.trimName || trim.trim_name || ""));
  const trimNameCounts = trimNames.reduce((acc, name) => {
    acc[name] = (acc[name] || 0) + 1;
    return acc;
  }, {});
  const sourceNames = trims.map((trim) => String(trim.sourceFileName || trim.source_file_name || trim.source || trim.sourceUploadId || ""));
  return {
    trimCount: trims.length,
    rowCount: rows.length,
    fullNames,
    trimNames,
    sourceNames,
    duplicateBasicPremiumKept: (trimNameCounts.Basic || 0) >= 2 && (trimNameCounts.Premium || 0) >= 2,
    fullNamesDistinct: new Set(fullNames).size === trims.length,
    sourcesVisible: fileNames.every((fileName) => (
      fullNames.some((name) => name.includes(fileName))
      || sourceNames.some((name) => name.includes(fileName))
    )),
    allTrimIdsReturned: trimIds.every((trimId) => trims.some((trim) => String(trim.trimId || trim.trim_id) === trimId)),
  };
}

async function evaluateFormalCompareUi(page, trimIds, fileNames) {
  const compareUrl = new URL(page.url());
  compareUrl.pathname = "/product/compare/config";
  compareUrl.search = "";
  compareUrl.searchParams.set("trimIds", trimIds.join(","));
  compareUrl.searchParams.set("baseTrimId", trimIds[0]);
  await page.goto(compareUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
  await page.getByText(/当前展示 \d+\/\d+ 配置行/).waitFor({ state: "visible", timeout: 60000 });
  return page.evaluate((expectedFileNames) => {
    const bodyText = document.body.innerText.replace(/\s+/g, " ").trim();
    const rowsStatus = (bodyText.match(/当前展示 \d+\/\d+ 配置行/) || [""])[0];
    return {
      rowsStatus,
      fileNamesVisible: expectedFileNames.every((fileName) => bodyText.includes(fileName)),
      basicVisible: bodyText.includes("Basic"),
      premiumVisible: bodyText.includes("Premium"),
      noHorizontalOverflow: document.documentElement.scrollWidth <= window.innerWidth + 2,
      bodySnippet: bodyText.slice(0, 1600),
    };
  }, fileNames);
}

async function runSmoke(options) {
  if (!options.write) {
    throw new Error("Refusing to run write-path smoke. Pass --write to create temporary source/config-column rows.");
  }

  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const stamp = runId.toLowerCase().replace(/[^a-z0-9]+/g, "").slice(0, 14);
  if (!options.country) options.country = `Multi Source Smoke Country ${stamp}`;
  if (!options.model) options.model = `Multi Source Same Model ${stamp}`;
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-multisource-same-model-smoke", runId);
  await mkdir(artifactDir, { recursive: true });

  const fileA = `multisource-${stamp}-website-a.csv`;
  const fileB = `multisource-${stamp}-website-b.csv`;
  const csvA = sourceCsv({ ...options, modelYear: "2025", sourceLabel: "A" });
  const csvB = sourceCsv({ ...options, modelYear: "2026", sourceLabel: "B" });
  const csvPathA = path.join(artifactDir, fileA);
  const csvPathB = path.join(artifactDir, fileB);
  await writeFile(csvPathA, csvA, "utf8");
  await writeFile(csvPathB, csvB, "utf8");

  const observed = { api: [], ui: [], cleanup: null };
  let uploads = [];
  let trimIds = [];
  let browser = null;
  let page = null;
  try {
    uploads = [
      await uploadCsvSource(options, { fileName: fileA, csv: csvA, modelYear: "2025", sourceLabel: "A" }),
      await uploadCsvSource(options, { fileName: fileB, csv: csvB, modelYear: "2026", sourceLabel: "B" }),
    ];
    observed.api.push(...uploads.map((upload) => ({
      step: "source_uploaded",
      sourceId: upload.sourceId,
      fileName: upload.complete?.sourceFileName || upload.complete?.source_file_name || "",
      groupId: upload.digestInfo.groupId,
      digestTrimIds: upload.digestInfo.trimIds,
      modelName: upload.digestInfo.group.modelName,
      rowCount: Array.isArray(upload.digestInfo.group.rows) ? upload.digestInfo.group.rows.length : null,
    })));

    const draftA = await createDraftFromDigest(options, uploads[0]);
    const draftB = await createDraftFromDigest(options, uploads[1]);
    trimIds = [...draftA.compareTrimIds.slice(0, 2), ...draftB.compareTrimIds.slice(0, 2)];
    observed.api.push(
      { step: "draft_created", sourceId: uploads[0].sourceId, trimIds: draftA.compareTrimIds },
      { step: "draft_created", sourceId: uploads[1].sourceId, trimIds: draftB.compareTrimIds },
    );

    const compareData = await getCompareData(options, trimIds);
    const compareApi = evaluateCompareApi(compareData, [fileA, fileB], trimIds);
    observed.api.push({ step: "formal_compare_api", ...compareApi });

    browser = await chromium.launch({
      headless: !options.headed,
      ...(options.browserChannel ? { channel: options.browserChannel } : {}),
    });
    const context = await browser.newContext({ viewport: { width: 1440, height: 920 } });
    await context.addInitScript(({ userName }) => {
      window.localStorage.setItem("jato_user_name", userName);
      window.localStorage.setItem("jato_user_role", "admin");
      window.localStorage.setItem("product_config_summary_mode", "simple");
      window.localStorage.setItem("jato_product_config_summary_mode", "simple");
    }, { userName: options.userName });
    page = await context.newPage();

    const sourceSearchUrl = new URL(`${options.baseUrl}/product/compare/config`);
    sourceSearchUrl.searchParams.set("market", options.country);
    sourceSearchUrl.searchParams.set("model", options.model);
    sourceSearchUrl.searchParams.set("powertrain", options.powertrain);
    sourceSearchUrl.searchParams.set("segment", options.segment);
    await page.goto(sourceSearchUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
    const floatingDeckSearch = await evaluateFloatingDeckMultiSourceSearch(page, options, [fileA, fileB]);
    observed.ui.push({ step: "floating_deck_multisource_search", ...floatingDeckSearch });

    const formalCompareUi = await evaluateFormalCompareUi(page, trimIds, [fileA, fileB]);
    observed.ui.push({ step: "formal_compare_ui", ...formalCompareUi });

    const screenshotPath = path.join(artifactDir, "product_config_multisource_same_model_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: true });
    observed.cleanup = await cleanupArtifacts(options, {
      sourceIds: uploads.map((upload) => upload.sourceId),
      trimIds,
    });
    const summary = {
      createdAt: new Date().toISOString(),
      sourceSearchUrl: sourceSearchUrl.toString(),
      artifactDir,
      screenshotPath,
      csvPaths: [csvPathA, csvPathB],
      sourceIds: uploads.map((upload) => upload.sourceId),
      trimIds,
      compareApi,
      floatingDeckSearch,
      formalCompareUi,
      cleanup: observed.cleanup,
      observed,
      passed: (
        uploads.length === 2
        && trimIds.length === 4
        && compareApi.trimCount === 4
        && compareApi.fullNamesDistinct
        && compareApi.duplicateBasicPremiumKept
        && compareApi.sourcesVisible
        && floatingDeckSearch.passed
        && formalCompareUi.fileNamesVisible
        && formalCompareUi.noHorizontalOverflow
        && (!observed.cleanup || observed.cleanup.errors.length === 0)
      ),
    };
    const summaryPath = path.join(artifactDir, "product_config_multisource_same_model_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ summaryPath, ...summary }, null, 2));
    if (!summary.passed) process.exitCode = 1;
  } catch (error) {
    const failureScreenshot = path.join(artifactDir, "failure.png");
    if (page) await page.screenshot({ path: failureScreenshot, fullPage: true }).catch(() => undefined);
    observed.cleanup = await cleanupArtifacts(options, {
      sourceIds: uploads.map((upload) => upload.sourceId),
      trimIds,
    }).catch((cleanupError) => ({
      enabled: options.cleanup,
      sourceIds: uploads.map((upload) => upload.sourceId),
      trimIds,
      errors: [{ step: "cleanup_after_failure", message: cleanupError instanceof Error ? cleanupError.message : String(cleanupError) }],
    }));
    const summaryPath = path.join(artifactDir, "product_config_multisource_same_model_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      artifactDir,
      failureScreenshot,
      csvPaths: [csvPathA, csvPathB],
      sourceIds: uploads.map((upload) => upload.sourceId),
      trimIds,
      cleanup: observed.cleanup,
      observed,
      error: error instanceof Error ? error.message : String(error),
      passed: false,
    }, null, 2)}\n`, "utf8");
    console.error(`Product config multi-source same-model smoke failed. Summary: ${summaryPath}`);
    throw error;
  } finally {
    if (browser) await browser.close().catch(() => undefined);
  }
}

const options = parseArgs(process.argv.slice(2));
if (options.help) {
  console.log(usage());
  process.exit(0);
}

runSmoke(options).catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
