import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "",
    country: process.env.PRODUCT_CONFIG_SMOKE_COUNTRY || "Germany",
    model: process.env.PRODUCT_CONFIG_SMOKE_MODEL || "T19C MY ICE",
    powertrain: process.env.PRODUCT_CONFIG_SMOKE_POWERTRAIN || "ICE",
    segment: process.env.PRODUCT_CONFIG_SMOKE_SEGMENT || "SUV C",
    userName: process.env.PRODUCT_CONFIG_SMOKE_USER || "product-config-smoke",
    browserChannel: process.env.PRODUCT_CONFIG_SMOKE_BROWSER_CHANNEL || "",
    headed: false,
    write: process.env.PRODUCT_CONFIG_SMOKE_WRITE === "1",
    readOnlyEntry: process.env.PRODUCT_CONFIG_SMOKE_READ_ONLY_ENTRY === "1",
    cleanup: process.env.PRODUCT_CONFIG_SMOKE_CLEANUP !== "0",
    timeoutMs: 120000,
    help: false,
  };
  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") options.help = true;
    else if (arg === "--headed") options.headed = true;
    else if (arg === "--write") options.write = true;
    else if (arg === "--read-only-entry") options.readOnlyEntry = true;
    else if (arg === "--skip-cleanup") options.cleanup = false;
    else if (arg.startsWith("--base-url=")) options.baseUrl = arg.slice("--base-url=".length);
    else if (arg.startsWith("--api-base=")) options.apiBase = arg.slice("--api-base=".length);
    else if (arg.startsWith("--country=")) options.country = arg.slice("--country=".length);
    else if (arg.startsWith("--model=")) options.model = arg.slice("--model=".length);
    else if (arg.startsWith("--powertrain=")) options.powertrain = arg.slice("--powertrain=".length);
    else if (arg.startsWith("--segment=")) options.segment = arg.slice("--segment=".length);
    else if (arg.startsWith("--user-name=")) options.userName = arg.slice("--user-name=".length);
    else if (arg.startsWith("--channel=")) options.browserChannel = arg.slice("--channel=".length);
    else if (arg.startsWith("--timeout-ms=")) options.timeoutMs = Math.max(10000, Number(arg.slice("--timeout-ms=".length)) || options.timeoutMs);
  }
  options.baseUrl = options.baseUrl.replace(/\/+$/, "");
  options.apiBase = (options.apiBase || `${options.baseUrl}/v1`).replace(/\/+$/, "");
  return options;
}

function usage() {
  return [
    "Visible Product Config competitor workflow smoke.",
    "",
    "Read-only entry mode checks the Advanced Analysis recommendation panel and Source Digest handoff.",
    "Write mode creates temporary Source Digest/config-column rows through the real UI.",
    "Run write mode only against local/staging data:",
    "",
    "  node scripts/product_config_competitor_workflow_smoke.mjs --read-only-entry \\",
    "    --base-url=http://127.0.0.1:5177",
    "",
    "  node scripts/product_config_competitor_workflow_smoke.mjs --write \\",
    "    --base-url=http://127.0.0.1:5177 \\",
    "    --api-base=http://127.0.0.1:8004/v1",
    "",
    "Options:",
    "  --read-only-entry       Verify recommendation entry and missing-source handoff without writes.",
    "  --write                 Required. Acknowledges this is a write-path smoke.",
    "  --skip-cleanup          Keep generated source/config columns for manual inspection.",
    "  --headed                Show the browser.",
    "  --channel=chrome        Use an installed browser channel instead of Playwright's bundled browser.",
    "  --country=Germany      Recommendation country/market.",
    "  --model='T19C MY ICE'  Target model.",
    "  --powertrain=ICE        Target powertrain.",
    "  --segment='SUV C'      Target segment.",
  ].join("\n");
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function slug(value, fallback = "competitor") {
  const text = String(value || "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "");
  return text || fallback;
}

function csvCell(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function competitorCsv({ brand, modelName, country, powertrain, segment, stamp }) {
  const rows = [
    ["Feature", `${modelName} Workflow Basic`, `${modelName} Workflow Premium`],
    ["Brand / 品牌", brand, brand],
    ["Model / 车型", modelName, modelName],
    ["Country / 国家", country, country],
    ["Powertrain / 动力", powertrain, powertrain],
    ["Segment / 级别", segment, segment],
    ["Configuration version / 配置版型", "Workflow Basic", "Workflow Premium"],
    ["Material No. / 物料号", `UI-SMOKE-${stamp}-BASIC`, `UI-SMOKE-${stamp}-PREM`],
    ["Rear Visual parking assist / 动态辅助线倒车影像", "-", "●"],
    ["360 round view camera / 360度高清全景影像", "-", "●"],
    ["Power sunroof / 电动天窗", "-", "O"],
    ["Wireless charging / 手机无线充电", "-", "●"],
  ];
  return `${rows.map((row) => row.map(csvCell).join(",")).join("\n")}\n`;
}

async function apiRequest(options, method, apiPath, body) {
  const response = await fetch(`${options.apiBase}${apiPath}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-User-Name": options.userName,
      ...(process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN ? { "X-Auth-Token": process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN } : {}),
    },
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

async function cleanupArtifacts(options, { sourceId, trimIds }) {
  const cleanup = { enabled: options.cleanup, sourceId, trimIds, sourceTrashed: false, sourceTrashCleared: null, trimTrashCleared: null, errors: [] };
  if (!options.cleanup) return cleanup;
  for (const trimId of trimIds) {
    try {
      await apiRequest(options, "PATCH", `/engineering-config/trims/${encodeURIComponent(trimId)}`, { status: "trashed" });
    } catch (error) {
      cleanup.errors.push({ step: "trash_trim", trimId, message: error instanceof Error ? error.message : String(error) });
    }
  }
  try {
    const result = await apiRequest(options, "DELETE", `/engineering-config/trims/trash?market=${encodeURIComponent(options.country)}`);
    cleanup.trimTrashCleared = result?.cleared ?? null;
  } catch (error) {
    cleanup.errors.push({ step: "clear_trim_trash", message: error instanceof Error ? error.message : String(error) });
  }
  if (sourceId) {
    try {
      await apiRequest(options, "DELETE", `/engineering-config/source/snapshots/${encodeURIComponent(sourceId)}?country=${encodeURIComponent(options.country)}`);
      cleanup.sourceTrashed = true;
    } catch (error) {
      cleanup.errors.push({ step: "trash_source", sourceId, message: error instanceof Error ? error.message : String(error) });
    }
    try {
      const result = await apiRequest(options, "DELETE", `/engineering-config/source/trash?country=${encodeURIComponent(options.country)}`);
      cleanup.sourceTrashCleared = result?.cleared ?? null;
    } catch (error) {
      cleanup.errors.push({ step: "clear_source_trash", message: error instanceof Error ? error.message : String(error) });
    }
  }
  return cleanup;
}

async function waitForExportResponse(page, button, urlPart, signature, expectedContentType) {
  const responsePromise = page.waitForResponse((response) => (
    response.url().includes(urlPart) && response.status() === 200
    && response.request().method() === "POST"
  ), { timeout: 60000 });
  await button.click();
  const response = await responsePromise;
  const contentType = response.headers()["content-type"] || "";
  if (!contentType.includes(expectedContentType)) {
    throw new Error(`${urlPart} response content-type was ${contentType || "missing"}, expected ${expectedContentType}`);
  }
  let bodyLength = 0;
  try {
    const body = await response.body();
    bodyLength = body.length;
    if (body.length > 0) {
      const expected = Buffer.from(signature);
      if (!body.subarray(0, expected.length).equals(expected)) {
        throw new Error(`${urlPart} response did not start with expected signature ${signature}; first-bytes=${body.subarray(0, 16).toString("hex")}`);
      }
    }
  } catch (error) {
    if (error instanceof Error && error.message.includes("expected signature")) throw error;
  }
  return {
    url: response.url(),
    bytes: bodyLength,
    contentType,
  };
}

async function runReadOnlyEntrySmoke(options) {
  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-competitor-entry-smoke", runId);
  await mkdir(artifactDir, { recursive: true });

  const targetUrl = new URL(`${options.baseUrl}/product/compare/config`);
  targetUrl.searchParams.set("market", options.country);
  targetUrl.searchParams.set("model", options.model);
  targetUrl.searchParams.set("powertrain", options.powertrain);
  targetUrl.searchParams.set("segment", options.segment);

  const browser = await chromium.launch({
    headless: !options.headed,
    ...(options.browserChannel ? { channel: options.browserChannel } : {}),
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 920 } });
  await context.addInitScript(({ userName }) => {
    window.localStorage.setItem("jato_user_name", userName);
    window.localStorage.setItem("jato_user_role", "admin");
    window.localStorage.setItem("product_config_summary_mode", "simple");
  }, { userName: options.userName });
  const page = await context.newPage();
  const observed = { requests: [], responses: [] };
  page.on("request", (request) => {
    const url = request.url();
    if (
      url.includes("/engineering-config/recommendations/competitors")
      || url.includes("/engineering-config/source/snapshots")
      || url.includes("/engineering-config/source/upload/")
    ) {
      observed.requests.push({ method: request.method(), url });
    }
  });
  page.on("response", (response) => {
    const url = response.url();
    if (
      url.includes("/engineering-config/recommendations/competitors")
      || url.includes("/engineering-config/source/snapshots")
      || url.includes("/engineering-config/source/upload/")
    ) {
      observed.responses.push({ method: response.request().method(), status: response.status(), url });
    }
  });

  try {
    await page.goto(targetUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
    const bodyText = await page.locator("body").innerText({ timeout: 15000 });
    if (/404\s*not\s*found/i.test(bodyText)) throw new Error("/product/compare/config showed 404 Not Found");

    const trigger = page.getByRole("button", { name: /添加配置列 \/ 显示/ });
    await trigger.click({ timeout: 15000 });
    const drawer = page.locator(".deck-floating-panel");
    await drawer.waitFor({ state: "visible", timeout: 15000 });
    const advancedToggle = drawer.getByRole("button", { name: /高级筛选 \/ 库内浏览/ });
    await advancedToggle.waitFor({ state: "visible", timeout: 15000 });
    if (await advancedToggle.getAttribute("aria-expanded") !== "true") {
      await advancedToggle.click();
    }
    await drawer.getByText("高级分析推荐竞品").waitFor({ state: "visible", timeout: options.timeoutMs });
    await page.waitForFunction(() => (
      document.querySelectorAll(".deck-floating-panel .comparison-competitor-card").length > 0
      || /当前口径暂无推荐竞品|推荐竞品加载失败/.test(document.querySelector(".deck-floating-panel")?.textContent || "")
    ), null, { timeout: options.timeoutMs });

    const recommendationState = await page.evaluate(() => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const summary = normalize(document.querySelector(".comparison-competitor-summary")?.textContent || "");
      const queue = normalize(document.querySelector(".comparison-competitor-queue")?.textContent || "");
      const cards = Array.from(document.querySelectorAll(".deck-floating-panel .comparison-competitor-card"))
        .map((card) => normalize(card.textContent || ""));
      const panelText = normalize(document.querySelector(".deck-floating-panel")?.textContent || "");
      const heroText = normalize(document.querySelector(".comparison-hero")?.textContent || "");
      const bmwCard = cards.find((text) => text.includes("BMW X7")) || "";
      const urusCard = cards.find((text) => text.includes("Urus")) || "";
      return {
        summary,
        queue,
        cards: cards.slice(0, 10),
        panelText: panelText.slice(0, 5000),
        heroText,
        bmwCard,
        urusCard,
        checks: {
          hasRecommendations: cards.length >= 10,
          topTenOk: summary.includes("Top 10/10"),
          coverageCountsOk: summary.includes("库内可用1") && summary.includes("待上传9"),
          scopeOk: summary.includes("Germany · ICE · SUV C"),
          queueVisible: queue.includes("补齐队列"),
          queueMissingPriorityOk: queue.includes("优先补上传缺口") && queue.includes("Urus"),
          queueCountsOk: queue.includes("库内可用1") && queue.includes("待上传9"),
          queuePrimaryActionOk: queue.includes("上传 Urus 来源"),
          bmwReadyOk: (
            bmwCard.includes("库内 8 配置列")
            && bmwCard.includes("来源库 4 来源 · 4 组 · 8 配置列")
            && bmwCard.includes("加入库内配置列")
            && bmwCard.includes("核对 BMW X7 来源")
          ),
          urusMissingOk: (
            urusCard.includes("LAMBORGHINI")
            && urusCard.includes("Urus")
            && urusCard.includes("配置资料缺口")
            && urusCard.includes("搜索 / 上传 Urus 来源")
          ),
        },
      };
    });

    const urusButton = drawer.getByRole("button", { name: /搜索 \/ 上传 Urus 来源/ });
    await urusButton.waitFor({ state: "visible", timeout: 15000 });
    await urusButton.click({ timeout: 15000 });
    await page.getByText("配置表 / 价格单上传（推荐）").waitFor({ state: "visible", timeout: 15000 });
    await page.waitForFunction(() => {
      const input = document.querySelector('input[aria-label="搜索来源 / 车型 / 配置列"], input[aria-label="搜索 Source Digest 可比组"]');
      const text = document.querySelector(".deck-floating-panel")?.textContent || "";
      return input?.value === "LAMBORGHINI Urus Germany ICE SUV C" && text.includes("推荐竞品缺口");
    }, null, { timeout: 15000 });

    const sourceHandoffState = await page.evaluate(() => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const sourceInput = document.querySelector('input[aria-label="搜索来源 / 车型 / 配置列"], input[aria-label="搜索 Source Digest 可比组"]');
      const panelText = normalize(document.querySelector(".deck-floating-panel")?.textContent || "");
      const heroText = normalize(document.querySelector(".comparison-hero")?.textContent || "");
      return {
        sourceSearchValue: sourceInput?.value || "",
        panelText: panelText.slice(0, 4000),
        heroText,
        checks: {
          sourceSearchPrefilled: sourceInput?.value === "LAMBORGHINI Urus Germany ICE SUV C",
          missingContextOk: (
            panelText.includes("LAMBORGHINI · Urus · Germany · ICE · SUV C")
            && panelText.includes("推荐竞品缺口")
            && panelText.includes("身份锚点 品牌 / 车型 / 市场")
          ),
          uploadSurfaceVisible: (
            panelText.includes("拖放配置表或来源文件")
            && panelText.includes(".xlsx")
            && panelText.includes(".pdf")
            && panelText.includes(".jpg")
          ),
          currentTargetPreserved: (
            heroText.includes("T19C MY ICE")
            && heroText.includes("Germany")
            && heroText.includes("ICE")
            && !heroText.includes("Urus")
          ),
          noUploadWriteRequests: !((window.__productConfigEntryWriteRequestCount || 0) > 0),
        },
      };
    });

    const writeRequestCount = observed.requests.filter((request) => (
      request.method !== "GET"
      && (
        request.url.includes("/engineering-config/source/upload/")
        || request.url.includes("/engineering-config/source/snapshots/")
        || request.url.includes("/engineering-config/trims/")
      )
    )).length;
    sourceHandoffState.checks.noUploadWriteRequests = writeRequestCount === 0;

    const screenshotPath = path.join(artifactDir, "product_config_competitor_entry_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: false });
    const checks = {
      ...recommendationState.checks,
      ...sourceHandoffState.checks,
    };
    const summary = {
      createdAt: new Date().toISOString(),
      mode: "read-only-entry",
      targetUrl: targetUrl.toString(),
      artifactDir,
      screenshotPath,
      recommendationState,
      sourceHandoffState,
      checks,
      requests: observed.requests,
      responses: observed.responses,
      passed: Object.values(checks).every(Boolean),
    };
    const summaryPath = path.join(artifactDir, "product_config_competitor_entry_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(`Product config competitor entry smoke ${summary.passed ? "passed" : "failed"}. Summary: ${summaryPath}`);
    if (!summary.passed) process.exitCode = 1;
  } catch (error) {
    const failureScreenshot = path.join(artifactDir, "failure.png");
    await page.screenshot({ path: failureScreenshot, fullPage: false }).catch(() => undefined);
    const summaryPath = path.join(artifactDir, "product_config_competitor_entry_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      mode: "read-only-entry",
      targetUrl: targetUrl.toString(),
      artifactDir,
      failureScreenshot,
      requests: observed.requests,
      responses: observed.responses,
      passed: false,
      error: error instanceof Error ? error.message : String(error),
    }, null, 2)}\n`, "utf8");
    console.error(`Product config competitor entry smoke failed. Summary: ${summaryPath}`);
    console.error(error);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    console.log(usage());
    return;
  }
  if (options.readOnlyEntry) {
    await runReadOnlyEntrySmoke(options);
    return;
  }
  if (!options.write) {
    console.error(usage());
    console.error("\nRefusing to run: pass --read-only-entry for the non-writing entry smoke, or --write to acknowledge temporary source/config-column rows.");
    process.exit(2);
  }

  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-competitor-workflow-smoke", runId);
  await mkdir(artifactDir, { recursive: true });

  const targetUrl = new URL(`${options.baseUrl}/product/compare/config`);
  targetUrl.searchParams.set("market", options.country);
  targetUrl.searchParams.set("model", options.model);
  targetUrl.searchParams.set("powertrain", options.powertrain);
  targetUrl.searchParams.set("segment", options.segment);

  const browser = await chromium.launch({
    headless: !options.headed,
    ...(options.browserChannel ? { channel: options.browserChannel } : {}),
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 920 } });
  await context.addInitScript(({ userName }) => {
    window.localStorage.setItem("jato_user_name", userName);
    window.localStorage.setItem("jato_user_role", "admin");
    window.localStorage.setItem("product_config_summary_mode", "simple");
  }, { userName: options.userName });
  const page = await context.newPage();
  const observed = { responses: [], cleanup: null };
  let sourceId = null;
  let trimIds = [];
  let csvPath = "";

  try {
    await page.goto(targetUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
    const bodyText = await page.locator("body").innerText({ timeout: 15000 });
    if (/404\s*not\s*found/i.test(bodyText)) throw new Error("/product/compare/config showed 404 Not Found");

    const trigger = page.getByRole("button", { name: /添加配置列 \/ 显示/ });
    await trigger.click({ timeout: 15000 });
    const drawer = page.locator(".deck-floating-panel");
    await drawer.waitFor({ state: "visible", timeout: 15000 });
    const advancedToggle = drawer.getByRole("button", { name: /高级筛选 \/ 库内浏览/ });
    await advancedToggle.waitFor({ state: "visible", timeout: 15000 });
    if (await advancedToggle.getAttribute("aria-expanded") !== "true") {
      await advancedToggle.click();
    }
    await drawer.getByText("高级分析推荐竞品").waitFor({ state: "visible", timeout: options.timeoutMs });

    const recommendations = drawer.locator(".comparison-competitor-card.is-missing");
    await page.waitForFunction(() => (
      document.querySelectorAll(".deck-floating-panel .comparison-competitor-card").length > 0
      || /当前口径暂无推荐竞品|推荐竞品加载失败/.test(document.querySelector(".deck-floating-panel")?.textContent || "")
    ), null, { timeout: options.timeoutMs });
    const missingCount = await recommendations.count();
    if (missingCount === 0) {
      throw new Error("No upload-needed competitor recommendation was visible in FloatingDeck.");
    }
    const card = recommendations.first();
    const modelName = (await card.locator("header strong").innerText({ timeout: 15000 })).trim();
    const brandLine = (await card.locator("header span").innerText({ timeout: 15000 })).trim();
    const brand = brandLine.replace(/^#\d+\s*/, "").trim() || "WorkflowRival";
    const queueState = await page.evaluate((modelNameFromCard) => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const queue = normalize(document.querySelector(".comparison-competitor-queue")?.textContent || "");
      return {
        queue,
        checks: {
          queueVisible: queue.includes("补齐队列"),
          queueMissingPriorityOk: queue.includes("优先补上传缺口") && queue.includes(modelNameFromCard),
          queuePrimaryActionOk: queue.includes(`上传 ${modelNameFromCard} 来源`),
          queueCountsOk: queue.includes("待上传"),
        },
      };
    }, modelName);
    const stamp = runId.toLowerCase().replace(/[^a-z0-9]+/g, "").slice(0, 14);
    const fileName = `ui-workflow-${slug(modelName)}-${stamp}.csv`;
    csvPath = path.join(artifactDir, fileName);
    await writeFile(csvPath, competitorCsv({
      brand,
      modelName,
      country: options.country,
      powertrain: options.powertrain,
      segment: options.segment,
      stamp,
    }), "utf8");

    await card.getByRole("button", { name: new RegExp(`搜索 / 上传 ${modelName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} 来源`) }).click({ timeout: 15000 });
    await page.getByText("配置表 / 价格单上传（推荐）").waitFor({ state: "visible", timeout: 15000 });
    const sourceInput = drawer.locator(".config-source-upload-panel input[type='file']");
    await sourceInput.setInputFiles(csvPath);

    const completeResponsePromise = page.waitForResponse((response) => (
      response.url().includes("/engineering-config/source/upload/")
      && response.url().includes("/complete")
      && response.request().method() === "POST"
    ), { timeout: options.timeoutMs });
    await page.getByRole("button", { name: "上传并生成 Source Digest" }).click({ timeout: 15000 });
    const completeResponse = await completeResponsePromise;
    const completePayload = await completeResponse.json();
    sourceId = completePayload.sourceId || completePayload.source_id || null;
    observed.responses.push({ step: "upload_complete", status: completeResponse.status(), sourceId });

    const candidateButton = page.locator('button[aria-label^="选择 Source Digest 可比组："]').first();
    await candidateButton.waitFor({ state: "visible", timeout: options.timeoutMs });
    const draftResponsePromise = page.waitForResponse((response) => (
      response.url().includes("/engineering-config/source/snapshots/")
      && response.url().includes("/digest-groups/")
      && response.url().includes("/draft")
      && response.request().method() === "POST"
    ), { timeout: options.timeoutMs });
    await candidateButton.click();
    const draftResponse = await draftResponsePromise;
    const draftPayload = await draftResponse.json();
    trimIds = Array.isArray(draftPayload.compareTrimIds)
      ? draftPayload.compareTrimIds
      : Array.isArray(draftPayload.trimIds)
        ? draftPayload.trimIds
        : [];
    observed.responses.push({ step: "draft_created", status: draftResponse.status(), trimIds });

    await page.getByLabel(/Source Digest 建列成功|来源建列成功/).waitFor({ state: "visible", timeout: options.timeoutMs });
    await page.getByText(/当前展示 \d+\/\d+ 配置行/).waitFor({ state: "visible", timeout: options.timeoutMs });

    const successCard = page.getByLabel(/Source Digest 建列成功|来源建列成功/);
    const xlsxButton = successCard.getByRole("button", { name: "导出建列结果 XLSX" });
    const pdfButton = successCard.getByRole("button", { name: "导出建列结果 PDF" });
    await xlsxButton.waitFor({ state: "visible", timeout: 30000 });
    await pdfButton.waitFor({ state: "visible", timeout: 30000 });
    observed.responses.push({
      step: "export_xlsx",
      ...(await waitForExportResponse(
        page,
        xlsxButton,
        "/engineering-config/compare/export/xlsx",
        "PK",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      )),
    });
    observed.responses.push({
      step: "export_pdf",
      ...(await waitForExportResponse(
        page,
        pdfButton,
        "/engineering-config/compare/export/pdf",
        "%PDF",
        "application/pdf",
      )),
    });

    const screenshotPath = path.join(artifactDir, "product_config_competitor_workflow_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: false });
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds });
    const summary = {
      createdAt: new Date().toISOString(),
      targetUrl: targetUrl.toString(),
      artifactDir,
      screenshotPath,
      csvPath,
      recommendation: { brand, modelName },
      queueState,
      sourceId,
      trimIds,
      cleanup: observed.cleanup,
      responses: observed.responses,
      passed: trimIds.length >= 2
        && Object.values(queueState.checks).every(Boolean)
        && observed.responses.some((item) => item.step === "export_xlsx" && String(item.contentType).includes("spreadsheetml.sheet"))
        && observed.responses.some((item) => item.step === "export_pdf" && String(item.contentType).includes("application/pdf"))
        && (!observed.cleanup || observed.cleanup.errors.length === 0),
    };
    const summaryPath = path.join(artifactDir, "product_config_competitor_workflow_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ summaryPath, ...summary }, null, 2));
    if (!summary.passed) process.exitCode = 1;
  } catch (error) {
    const failureScreenshot = path.join(artifactDir, "failure.png");
    await page.screenshot({ path: failureScreenshot, fullPage: false }).catch(() => undefined);
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds }).catch((cleanupError) => ({
      enabled: options.cleanup,
      sourceId,
      trimIds,
      errors: [{ step: "cleanup_after_failure", message: cleanupError instanceof Error ? cleanupError.message : String(cleanupError) }],
    }));
    const summaryPath = path.join(artifactDir, "product_config_competitor_workflow_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      targetUrl: targetUrl.toString(),
      artifactDir,
      failureScreenshot,
      csvPath,
      sourceId,
      trimIds,
      cleanup: observed.cleanup,
      responses: observed.responses,
      passed: false,
      error: error instanceof Error ? error.message : String(error),
    }, null, 2)}\n`, "utf8");
    console.error(`Product config competitor workflow smoke failed. Summary: ${summaryPath}`);
    console.error(error);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
