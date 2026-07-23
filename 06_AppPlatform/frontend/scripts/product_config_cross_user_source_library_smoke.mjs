import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "http://127.0.0.1:8004/v1",
    country: process.env.PRODUCT_CONFIG_CROSS_USER_COUNTRY || "",
    model: process.env.PRODUCT_CONFIG_CROSS_USER_MODEL || "",
    brand: process.env.PRODUCT_CONFIG_CROSS_USER_BRAND || "SharedLibrarySmoke",
    powertrain: process.env.PRODUCT_CONFIG_CROSS_USER_POWERTRAIN || "ICE",
    segment: process.env.PRODUCT_CONFIG_CROSS_USER_SEGMENT || "Smoke Segment",
    uploaderUserName: process.env.PRODUCT_CONFIG_SMOKE_UPLOADER_USER || "product-config-uploader-smoke",
    consumerUserName: process.env.PRODUCT_CONFIG_SMOKE_CONSUMER_USER || "product-config-consumer-smoke",
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
    else if (arg.startsWith("--uploader-user=")) options.uploaderUserName = arg.slice("--uploader-user=".length);
    else if (arg.startsWith("--consumer-user=")) options.consumerUserName = arg.slice("--consumer-user=".length);
    else if (arg.startsWith("--channel=")) options.browserChannel = arg.slice("--channel=".length);
    else if (arg.startsWith("--timeout-ms=")) options.timeoutMs = Math.max(30000, Number(arg.slice("--timeout-ms=".length)) || options.timeoutMs);
  }
  options.baseUrl = options.baseUrl.replace(/\/+$/, "");
  options.apiBase = options.apiBase.replace(/\/+$/, "");
  return options;
}

function usage() {
  return [
    "Visible Product Config cross-user source-library smoke.",
    "",
    "Uploads a temporary Source Digest config source as one user, opens the real Product Compare UI as another user,",
    "searches the shared Source Digest library from FloatingDeck, creates editable config columns, verifies formal compare, then cleans up.",
    "",
    "Run only against local/staging data:",
    "",
    "  node scripts/product_config_cross_user_source_library_smoke.mjs --write \\",
    "    --base-url=http://127.0.0.1:5177 \\",
    "    --api-base=http://127.0.0.1:8004/v1",
    "",
    "Options:",
    "  --write                       Required. Acknowledges temporary source/config-column writes.",
    "  --skip-cleanup                Keep generated source/config columns for manual inspection.",
    "  --headed                      Show the browser.",
    "  --channel=chrome              Use an installed browser channel instead of Playwright's bundled browser.",
    "  --uploader-user=alice         User name used for source upload.",
    "  --consumer-user=bob           User name used for UI search and draft creation.",
    "  --country='Smoke ...'         Country/market. Defaults to a unique smoke country.",
    "  --model='Smoke ...'           Model context. Defaults to a unique smoke model.",
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

function smokeHeaders(userName, contentType = "application/json") {
  return {
    ...(contentType ? { "Content-Type": contentType } : {}),
    "X-User-Name": userName,
    ...(process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN ? { "X-Auth-Token": process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN } : {}),
  };
}

async function apiJsonRequest(options, userName, method, apiPath, body) {
  const response = await fetch(`${options.apiBase}${apiPath}`, {
    method,
    headers: smokeHeaders(userName, body === undefined ? "" : "application/json"),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(`${method} ${apiPath} as ${userName} failed with HTTP ${response.status}: ${JSON.stringify(payload)}`);
  }
  return payload;
}

async function apiBinaryRequest(options, userName, method, apiPath, body, contentType) {
  const response = await fetch(`${options.apiBase}${apiPath}`, {
    method,
    headers: smokeHeaders(userName, contentType),
    body,
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(`${method} ${apiPath} as ${userName} failed with HTTP ${response.status}: ${JSON.stringify(payload)}`);
  }
  return payload;
}

function sourceCsv(options, stamp) {
  const rows = [
    ["Feature", "Shared Basic", "Shared Premium"],
    ["Brand / 品牌", options.brand, options.brand],
    ["Model / 车型", options.model, options.model],
    ["Country / 国家", options.country, options.country],
    ["Model year / 年款", "2026", "2026"],
    ["Powertrain / 动力", options.powertrain, options.powertrain],
    ["Segment / 级别", options.segment, options.segment],
    ["Configuration version / 配置版型", "Shared Basic", "Shared Premium"],
    ["Material No. / 物料号", "", ""],
    ["Source owner / 来源上传人", options.uploaderUserName, options.uploaderUserName],
    ["Rear Visual parking assist / 动态辅助线倒车影像", "-", "●"],
    ["360 round view camera / 360度高清全景影像", "-", "●"],
    ["Power sunroof / 电动天窗", "-", "O"],
    ["Wireless charging / 手机无线充电", "-", "●"],
    ["SONY 8 speakers / SONY 8扬声器", "-", "●"],
    ["Smoke marker / 验收标记", stamp, stamp],
  ];
  return `${rows.map((row) => row.map(csvCell).join(",")).join("\n")}\n`;
}

function digestTrimId(trim) {
  return String(trim?.trimId || trim?.trim_id || trim?.id || "").trim();
}

function extractDigestInfo(sourcePayload) {
  const digest = sourcePayload?.sourceDigest || sourcePayload?.source_digest || null;
  const groups = Array.isArray(digest?.compareGroups) ? digest.compareGroups : [];
  const group = groups.find((item) => Array.isArray(item?.trims) && item.trims.length >= 2 && Array.isArray(item?.rows) && item.rows.length > 0);
  if (!group) {
    throw new Error(`Uploaded source did not produce a comparable Source Digest group: ${JSON.stringify(sourcePayload?.sourceDigestStatus || sourcePayload?.source_digest_status)}`);
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

async function uploadCsvSource(options, fileName, csv) {
  const bytes = Buffer.from(csv, "utf8");
  const params = new URLSearchParams({
    file_name: fileName,
    total_size: String(bytes.length),
    mime_type: "text/csv",
  });
  const initiate = await apiJsonRequest(options, options.uploaderUserName, "POST", `/engineering-config/source/upload/initiate?${params.toString()}`);
  const uploadId = initiate?.uploadId || initiate?.upload_id;
  if (!uploadId) throw new Error(`Source upload initiate did not return uploadId: ${JSON.stringify(initiate)}`);
  await apiBinaryRequest(options, options.uploaderUserName, "PUT", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/parts/0`, bytes, "text/csv");
  const relatedContext = {
    brand: options.brand,
    model: options.model,
    market: options.country,
    country: options.country,
    modelYear: "2026",
    powertrain: options.powertrain,
    segment: options.segment,
    contextType: "model_trim_compare_target",
    scenario: "cross_user_source_library_smoke",
  };
  const complete = await apiJsonRequest(options, options.uploaderUserName, "POST", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/complete`, { relatedContext });
  const sourceId = complete?.sourceId || complete?.source_id || complete?.importBatchId || complete?.import_batch_id;
  if (!sourceId) throw new Error(`Source upload complete did not return sourceId: ${JSON.stringify(complete)}`);
  return { uploadId, sourceId, complete, relatedContext, digestInfo: extractDigestInfo(complete) };
}

async function listSharedSourceAsConsumer(options, fileName) {
  const params = new URLSearchParams({
    country: options.country,
    q: fileName,
    limit: "20",
  });
  const payload = await apiJsonRequest(options, options.consumerUserName, "GET", `/engineering-config/source/snapshots?${params.toString()}`);
  const items = Array.isArray(payload?.items) ? payload.items : Array.isArray(payload) ? payload : [];
  const match = items.find((item) => String(
    item.fileName || item.file_name || item.sourceFileName || item.source_file_name || "",
  ).includes(fileName));
  return {
    total: payload?.total ?? items.length,
    itemCount: items.length,
    found: Boolean(match),
    createdBy: match?.createdBy || match?.created_by || match?.sourceCreatedBy || match?.source_created_by || null,
    sourceId: match?.sourceId || match?.source_id || match?.id || null,
  };
}

async function cleanupArtifacts(options, { sourceId, trimIds }) {
  const cleanup = {
    enabled: options.cleanup,
    sourceId,
    trimIds,
    sourceTrashCleared: null,
    trimTrashCleared: null,
    errors: [],
  };
  if (!options.cleanup) return cleanup;
  for (const trimId of trimIds) {
    try {
      await apiJsonRequest(options, options.consumerUserName, "PATCH", `/engineering-config/trims/${encodeURIComponent(trimId)}`, { status: "trashed" });
    } catch (error) {
      cleanup.errors.push({ step: "trash_trim", trimId, message: error instanceof Error ? error.message : String(error) });
    }
  }
  try {
    const result = await apiJsonRequest(options, options.consumerUserName, "DELETE", `/engineering-config/trims/trash?market=${encodeURIComponent(options.country)}`);
    cleanup.trimTrashCleared = result?.cleared ?? null;
  } catch (error) {
    cleanup.errors.push({ step: "clear_trim_trash", message: error instanceof Error ? error.message : String(error) });
  }
  if (sourceId) {
    try {
      await apiJsonRequest(options, options.consumerUserName, "DELETE", `/engineering-config/source/snapshots/${encodeURIComponent(sourceId)}?country=${encodeURIComponent(options.country)}`);
    } catch (error) {
      cleanup.errors.push({ step: "trash_source", sourceId, message: error instanceof Error ? error.message : String(error) });
    }
    try {
      const result = await apiJsonRequest(options, options.consumerUserName, "DELETE", `/engineering-config/source/trash?country=${encodeURIComponent(options.country)}`);
      cleanup.sourceTrashCleared = result?.cleared ?? null;
    } catch (error) {
      cleanup.errors.push({ step: "clear_source_trash", message: error instanceof Error ? error.message : String(error) });
    }
  }
  return cleanup;
}

async function openFloatingDeck(page) {
  const trigger = page.getByRole("button", { name: /添加配置列\s*\/\s*显示|添加 TRIM\s*\/\s*显示|打开控制/ }).first();
  await trigger.waitFor({ state: "visible", timeout: 30000 });
  await trigger.click();
  await page.locator(".deck-floating-panel").waitFor({ state: "visible", timeout: 30000 });
}

async function openSourcePanel(page) {
  const sourceTab = page.getByRole("tab", { name: /Source Digest|来源\s*\/\s*上传|来源|上传/ }).first();
  await sourceTab.waitFor({ state: "visible", timeout: 30000 });
  await sourceTab.click({ timeout: 15000 });
}

async function fillSourceSearch(page, fileName) {
  const sourceSearch = page.getByRole("combobox", { name: /搜索来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|搜索 Source Digest 可比组/ }).first();
  await sourceSearch.waitFor({ state: "visible", timeout: 30000 });
  await sourceSearch.fill(fileName);
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
}

async function chooseSourceDigestGroup(page, options, fileName) {
  const sourcePicker = page.getByRole("combobox", { name: /选择来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|选择 Source\s*\/\s*Model\s*\/\s*配置列/ }).first();
  await sourcePicker.waitFor({ state: "visible", timeout: 30000 });
  await sourcePicker.click();
  await sourcePicker.fill(fileName);
  await page.waitForFunction(({ model, sourceFileName }) => {
    const options = Array.from(document.querySelectorAll('button[role="option"]'));
    return options.some((option) => {
      const title = option.querySelector(".version-comparison-model-option-name")?.textContent || "";
      const text = option.textContent || "";
      return title.trim().startsWith("生成配置列")
        && text.includes(model)
        && text.includes(sourceFileName)
        && !option.disabled;
    });
  }, { model: options.model, sourceFileName: fileName }, { timeout: options.timeoutMs });
  await page.evaluate(({ model, sourceFileName }) => {
    const options = Array.from(document.querySelectorAll('button[role="option"]'));
    const target = options.find((option) => {
      const title = option.querySelector(".version-comparison-model-option-name")?.textContent || "";
      const text = option.textContent || "";
      return title.trim().startsWith("生成配置列")
        && text.includes(model)
        && text.includes(sourceFileName)
        && !option.disabled;
    });
    if (!(target instanceof HTMLButtonElement)) {
      throw new Error(`Could not find enabled Source Digest group option for ${sourceFileName}`);
    }
    target.click();
  }, { model: options.model, sourceFileName: fileName });
}

async function openSourceDigestDetail(page) {
  const expandPaths = page.getByRole("button", { name: /展开全部路径|展开来源组详情|打开来源组详情/ }).first();
  if (await expandPaths.count() > 0) {
    await expandPaths.click({ timeout: 10000 }).catch(() => undefined);
  }
  await page.evaluate(() => {
    for (const element of document.querySelectorAll("details[aria-label='Source Digest 详情浏览'], details[aria-label='来源组详情浏览']")) {
      if (element instanceof HTMLDetailsElement) element.open = true;
    }
  });
}

async function runSmoke(options) {
  if (!options.write) {
    throw new Error("Refusing to run write-path smoke. Pass --write to create temporary source/config-column rows.");
  }
  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const stamp = runId.toLowerCase().replace(/[^a-z0-9]+/g, "").slice(0, 14);
  if (!options.country) options.country = `Cross User Source Country ${stamp}`;
  if (!options.model) options.model = `Cross User Source Model ${stamp}`;
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-cross-user-source-library-smoke", runId);
  await mkdir(artifactDir, { recursive: true });
  const fileName = `cross-user-source-${stamp}.csv`;
  const csv = sourceCsv(options, stamp);
  const csvPath = path.join(artifactDir, fileName);
  await writeFile(csvPath, csv, "utf8");

  const observed = { api: [], ui: [], cleanup: null };
  let sourceId = null;
  let trimIds = [];
  let browser = null;
  let page = null;
  try {
    const upload = await uploadCsvSource(options, fileName, csv);
    sourceId = upload.sourceId;
    observed.api.push({
      step: "source_uploaded_by_uploader",
      uploaderUserName: options.uploaderUserName,
      sourceId,
      uploadId: upload.uploadId,
      fileName,
      groupId: upload.digestInfo.groupId,
      digestTrimIds: upload.digestInfo.trimIds,
      sourceDigestStatus: upload.complete?.sourceDigestStatus || upload.complete?.source_digest_status,
    });

    const consumerSourceList = await listSharedSourceAsConsumer(options, fileName);
    observed.api.push({
      step: "consumer_can_list_uploaded_source",
      consumerUserName: options.consumerUserName,
      ...consumerSourceList,
    });

    const targetUrl = new URL(`${options.baseUrl}/product/compare/config`);
    targetUrl.searchParams.set("market", options.country);
    targetUrl.searchParams.set("model", options.model);
    targetUrl.searchParams.set("powertrain", options.powertrain);
    targetUrl.searchParams.set("segment", options.segment);

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
    }, { userName: options.consumerUserName });
    page = await context.newPage();
    await page.goto(targetUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);

    await openFloatingDeck(page);
    await openSourcePanel(page);
    await fillSourceSearch(page, fileName);

    const draftResponsePromise = page.waitForResponse((response) => (
      response.url().includes("/engineering-config/source/snapshots/")
      && response.url().includes("/digest-groups/")
      && response.url().includes("/draft")
      && response.request().method() === "POST"
    ), { timeout: options.timeoutMs });
    await chooseSourceDigestGroup(page, options, fileName);
    const draftResponse = await draftResponsePromise;
    const draftPayload = await draftResponse.json();
    trimIds = Array.isArray(draftPayload.compareTrimIds)
      ? draftPayload.compareTrimIds
      : Array.isArray(draftPayload.trimIds)
        ? draftPayload.trimIds
        : [];
    observed.api.push({
      step: "consumer_created_draft_from_shared_source",
      consumerUserName: options.consumerUserName,
      status: draftResponse.status(),
      trimIds,
      createdTrimCount: draftPayload.createdTrimCount ?? draftPayload.created_trim_count ?? null,
      reusedTrimCount: draftPayload.reusedTrimCount ?? draftPayload.reused_trim_count ?? null,
    });

    await page.waitForFunction(({ expectedFileName }) => {
      const bodyText = document.body.innerText.replace(/\s+/g, " ").trim();
      return bodyText.includes(expectedFileName)
        && /已选配置列\s*2\/4/.test(bodyText)
        && /当前展示 \d+\/\d+(?: 配置)?行/.test(bodyText);
    }, { expectedFileName: fileName }, { timeout: options.timeoutMs });
    const uiResult = await page.evaluate(({ fileName: expectedFileName, uploader, consumer }) => {
      const bodyText = document.body.innerText.replace(/\s+/g, " ").trim();
      const bodyUpper = bodyText.toUpperCase();
      const rowsStatus = (bodyText.match(/当前展示 \d+\/\d+(?: 配置)?行/) || [""])[0];
      return {
        rowsStatus,
        fileVisible: bodyText.includes(expectedFileName),
        uploaderVisible: bodyUpper.includes(uploader.toUpperCase()),
        consumerVisible: bodyUpper.includes(consumer.toUpperCase()),
        successVisible: bodyText.includes("Source Digest 建列成功") || bodyText.includes("来源建列成功")
          || bodyText.includes("已转成正式配置列")
          || bodyText.includes("新配置列已加入当前对比表")
          || /已选配置列\s*2\/4/.test(bodyText),
        basicVisible: bodyUpper.includes("SHARED BASIC"),
        premiumVisible: bodyUpper.includes("SHARED PREMIUM"),
        noHorizontalOverflow: document.documentElement.scrollWidth <= window.innerWidth + 2,
        bodySnippet: bodyText.slice(0, 1600),
      };
    }, { fileName, uploader: options.uploaderUserName, consumer: options.consumerUserName });
    observed.ui.push({ step: "consumer_ui_created_formal_compare", ...uiResult });

    const screenshotPath = path.join(artifactDir, "product_config_cross_user_source_library_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: true });
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds });
    const summary = {
      createdAt: new Date().toISOString(),
      targetUrl: targetUrl.toString(),
      artifactDir,
      screenshotPath,
      csvPath,
      fileName,
      uploaderUserName: options.uploaderUserName,
      consumerUserName: options.consumerUserName,
      sourceId,
      trimIds,
      consumerSourceList,
      uiResult,
      cleanup: observed.cleanup,
      observed,
      passed: (
        Boolean(sourceId)
        && consumerSourceList.found
        && trimIds.length >= 2
        && uiResult.successVisible
        && uiResult.fileVisible
        && uiResult.basicVisible
        && uiResult.premiumVisible
        && uiResult.noHorizontalOverflow
        && (!observed.cleanup || observed.cleanup.errors.length === 0)
      ),
    };
    const summaryPath = path.join(artifactDir, "product_config_cross_user_source_library_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ summaryPath, ...summary }, null, 2));
    if (!summary.passed) process.exitCode = 1;
  } catch (error) {
    const failureScreenshot = path.join(artifactDir, "failure.png");
    if (page) await page.screenshot({ path: failureScreenshot, fullPage: true }).catch(() => undefined);
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds }).catch((cleanupError) => ({
      enabled: options.cleanup,
      sourceId,
      trimIds,
      errors: [{ step: "cleanup_after_failure", message: cleanupError instanceof Error ? cleanupError.message : String(cleanupError) }],
    }));
    const summaryPath = path.join(artifactDir, "product_config_cross_user_source_library_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      artifactDir,
      failureScreenshot,
      csvPath,
      fileName,
      uploaderUserName: options.uploaderUserName,
      consumerUserName: options.consumerUserName,
      sourceId,
      trimIds,
      cleanup: observed.cleanup,
      observed,
      error: error instanceof Error ? error.message : String(error),
      passed: false,
    }, null, 2)}\n`, "utf8");
    console.error(`Product config cross-user source-library smoke failed. Summary: ${summaryPath}`);
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
