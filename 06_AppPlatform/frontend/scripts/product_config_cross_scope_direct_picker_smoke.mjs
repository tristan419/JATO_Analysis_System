import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "http://127.0.0.1:8004/v1",
    userName: process.env.PRODUCT_CONFIG_SMOKE_USER || "product-config-cross-scope-smoke",
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
    "Visible Product Config cross-country/cross-model direct picker smoke.",
    "",
    "Creates temporary Germany/France source snapshots for different models, builds editable config columns,",
    "opens a compare with the Germany model, then uses the real FloatingDeck direct config-column dropdown",
    "to add the France model columns without choosing any own/competitor mode.",
    "",
    "Run only against local/staging data:",
    "",
    "  node scripts/product_config_cross_scope_direct_picker_smoke.mjs --write \\",
    "    --base-url=http://127.0.0.1:5177 \\",
    "    --api-base=http://127.0.0.1:8004/v1",
    "",
    "Options:",
    "  --write                 Required. Acknowledges temporary source/config-column writes.",
    "  --skip-cleanup          Keep generated source/config columns for manual inspection.",
    "  --headed                Show the browser.",
    "  --channel=chrome        Use an installed browser channel instead of Playwright's bundled browser.",
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

function sourceCsv({ brand, model, country, modelYear, powertrain, segment, basicLabel, premiumLabel }) {
  const rows = [
    ["Feature", basicLabel, premiumLabel],
    ["Brand / 品牌", brand, brand],
    ["Model / 车型", model, model],
    ["Country / 国家", country, country],
    ["Model year / 年款", modelYear, modelYear],
    ["Powertrain / 动力", powertrain, powertrain],
    ["Segment / 级别", segment, segment],
    ["Configuration version / 配置版型", basicLabel, premiumLabel],
    ["Material No. / 物料号", "", ""],
    ["Rear Visual parking assist / 动态辅助线倒车影像", "-", "●"],
    ["360 round view camera / 360度高清全景影像", "-", "●"],
    ["Power sunroof / 电动天窗", "-", "O"],
    ["Wireless charging / 手机无线充电", "-", "●"],
    ["SONY 8 speakers / SONY 8扬声器", "-", "●"],
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
  return { group, groupId: String(group.groupId || group.id || ""), trimIds };
}

async function uploadCsvSource(options, source) {
  const bytes = Buffer.from(source.csv, "utf8");
  const params = new URLSearchParams({
    file_name: source.fileName,
    total_size: String(bytes.length),
    mime_type: "text/csv",
  });
  const initiate = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/initiate?${params.toString()}`);
  const uploadId = initiate?.uploadId || initiate?.upload_id;
  if (!uploadId) throw new Error(`Source upload initiate did not return uploadId: ${JSON.stringify(initiate)}`);
  await apiBinaryRequest(options, "PUT", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/parts/0`, bytes, "text/csv");
  const relatedContext = {
    brand: source.brand,
    model: source.model,
    market: source.country,
    country: source.country,
    modelYear: source.modelYear,
    powertrain: source.powertrain,
    segment: source.segment,
    contextType: "model_trim_compare_target",
    scenario: "cross_scope_direct_picker_smoke",
  };
  const complete = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/complete`, { relatedContext });
  const sourceId = complete?.sourceId || complete?.source_id || complete?.importBatchId || complete?.import_batch_id;
  if (!sourceId) throw new Error(`Source upload complete did not return sourceId: ${JSON.stringify(complete)}`);
  return { ...source, uploadId, sourceId, complete, digestInfo: extractDigestGroup(complete) };
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

async function cleanupArtifacts(options, sources, trimIds) {
  const cleanup = {
    enabled: options.cleanup,
    sourceIds: sources.map((source) => source.sourceId).filter(Boolean),
    trimIds,
    sourceTrashClearedByCountry: {},
    trimTrashClearedByCountry: {},
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
  const countries = Array.from(new Set(sources.map((source) => source.country).filter(Boolean)));
  for (const country of countries) {
    try {
      const result = await apiJsonRequest(options, "DELETE", `/engineering-config/trims/trash?market=${encodeURIComponent(country)}`);
      cleanup.trimTrashClearedByCountry[country] = result?.cleared ?? null;
    } catch (error) {
      cleanup.errors.push({ step: "clear_trim_trash", country, message: error instanceof Error ? error.message : String(error) });
    }
  }
  for (const source of sources) {
    if (!source.sourceId) continue;
    try {
      await apiJsonRequest(options, "DELETE", `/engineering-config/source/snapshots/${encodeURIComponent(source.sourceId)}?country=${encodeURIComponent(source.country)}`);
    } catch (error) {
      cleanup.errors.push({ step: "trash_source", sourceId: source.sourceId, country: source.country, message: error instanceof Error ? error.message : String(error) });
    }
  }
  for (const country of countries) {
    try {
      const result = await apiJsonRequest(options, "DELETE", `/engineering-config/source/trash?country=${encodeURIComponent(country)}`);
      cleanup.sourceTrashClearedByCountry[country] = result?.cleared ?? null;
    } catch (error) {
      cleanup.errors.push({ step: "clear_source_trash", country, message: error instanceof Error ? error.message : String(error) });
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

async function selectDirectConfigColumn(page, query, expectedText, options) {
  const picker = page.getByRole("combobox", { name: /搜索并添加配置列|搜索来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|直接添加配置列|选择来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|选择 Source \/ Model \/ 配置列/ });
  await picker.waitFor({ state: "visible", timeout: 30000 });
  await picker.fill(query);
  const expectedPattern = escapeRegExp(expectedText);
  const startedAt = Date.now();
  let lastOptionTexts = [];
  while (Date.now() - startedAt < options.timeoutMs) {
    const result = await page.evaluate((pattern) => {
      const matcher = new RegExp(pattern, "i");
      const optionElements = Array.from(document.querySelectorAll('[role="option"]'));
      const optionTexts = optionElements.map((element) => element.textContent?.replace(/\s+/g, " ").trim() || "");
      const target = optionElements.find((element) => matcher.test(element.textContent || ""));
      if (target instanceof HTMLElement) {
        target.scrollIntoView({ block: "nearest" });
        target.click();
        return { clicked: true, expanded: false, optionTexts };
      }
      const more = document.querySelector(".version-comparison-model-more");
      if (more instanceof HTMLElement) {
        more.click();
        return { clicked: false, expanded: true, optionTexts };
      }
      return { clicked: false, expanded: false, optionTexts };
    }, expectedPattern);
    lastOptionTexts = result.optionTexts;
    if (result.clicked) return;
    if (!result.expanded) {
      await page.waitForTimeout(250);
      continue;
    }
    await page.waitForTimeout(150);
  }
  throw new Error(`Direct config dropdown option "${expectedText}" was not found after searching "${query}". Visible options: ${JSON.stringify(lastOptionTexts.slice(0, 20))}`);
}

async function selectedConfigColumnCount(page) {
  return page.evaluate(() => {
    const text = document.body.innerText.replace(/\s+/g, " ");
    const match = text.match(/已选配置列\s*(\d+)\s*\/\s*4/);
    return match ? Number(match[1]) : 0;
  });
}

async function evaluateDirectPickerFlow(page, options, sourceA, sourceB, trimIdsA) {
  const url = new URL(`${options.baseUrl}/product/compare/config`);
  url.searchParams.set("trimIds", trimIdsA.join(","));
  url.searchParams.set("baseTrimId", trimIdsA[0]);
  await page.goto(url.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);
  await page.getByText(/当前展示 \d+\/\d+ 配置行/).waitFor({ state: "visible", timeout: 60000 });
  await openFloatingDeck(page);

  await selectDirectConfigColumn(page, sourceB.fileName, sourceB.basicLabel, options);
  await page.waitForFunction(() => {
    const text = document.body.innerText.replace(/\s+/g, " ");
    return /已选配置列\s*[34]\s*\/\s*4/.test(text) || /[34]\s*\/\s*4/.test(text);
  }, null, { timeout: options.timeoutMs });
  if (await selectedConfigColumnCount(page) < 4) {
    await selectDirectConfigColumn(page, sourceB.fileName, sourceB.premiumLabel, options);
  }

  await page.waitForFunction(({ countryA, countryB, modelA, modelB, fileA, fileB }) => {
    const text = document.body.innerText.replace(/\s+/g, " ").trim();
    return text.includes(countryA)
      && text.includes(countryB)
      && text.includes(modelA)
      && text.includes(modelB)
      && text.includes(fileA)
      && text.includes(fileB)
      && (/已选配置列\s*4\/4/.test(text) || /当前展示 \d+\/\d+ 配置行/.test(text));
  }, {
    countryA: sourceA.country,
    countryB: sourceB.country,
    modelA: sourceA.model,
    modelB: sourceB.model,
    fileA: sourceA.fileName,
    fileB: sourceB.fileName,
  }, { timeout: options.timeoutMs });

  return page.evaluate(({ countryA, countryB, modelA, modelB, fileA, fileB }) => {
    const bodyText = document.body.innerText.replace(/\s+/g, " ").trim();
    const drawerText = document.querySelector(".deck-floating-panel")?.textContent?.replace(/\s+/g, " ").trim() || "";
    const selectedList = document.querySelector('[aria-label="当前已选配置列"]')?.textContent?.replace(/\s+/g, " ").trim() || "";
    const rowsStatus = (bodyText.match(/当前展示 \d+\/\d+ 配置行/) || [""])[0];
    return {
      rowsStatus,
      selectedList: selectedList.slice(0, 2000),
      drawerSnippet: drawerText.slice(0, 1800),
      bodySnippet: bodyText.slice(0, 1800),
      countriesVisible: bodyText.includes(countryA) && bodyText.includes(countryB),
      modelsVisible: bodyText.includes(modelA) && bodyText.includes(modelB),
      sourcesVisible: bodyText.includes(fileA) && bodyText.includes(fileB),
      selectedFourColumns: /已选配置列\s*4\/4/.test(bodyText) || /已选\s*4\/4/.test(drawerText),
      noModeText: !/本品对竞品模式|竞品对本品模式|本品对本品模式|竞品对竞品模式/.test(drawerText),
      noHorizontalOverflow: document.documentElement.scrollWidth <= window.innerWidth + 2,
    };
  }, {
    countryA: sourceA.country,
    countryB: sourceB.country,
    modelA: sourceA.model,
    modelB: sourceB.model,
    fileA: sourceA.fileName,
    fileB: sourceB.fileName,
  });
}

async function getCompareData(options, trimIds) {
  return apiJsonRequest(options, "GET", `/engineering-config/compare?trim_ids=${encodeURIComponent(trimIds.join(","))}`);
}

async function runSmoke(options) {
  if (!options.write) {
    throw new Error("Refusing to run write-path smoke. Pass --write to create temporary source/config-column rows.");
  }
  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const stamp = runId.toLowerCase().replace(/[^a-z0-9]+/g, "").slice(0, 14);
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-cross-scope-direct-picker-smoke", runId);
  await mkdir(artifactDir, { recursive: true });

  const base = {
    brand: "CrossScopeSmoke",
    powertrain: "BEV",
    segment: "Smoke Segment",
    modelYear: "2026",
  };
  const sourceA = {
    ...base,
    country: `Cross Scope Germany ${stamp}`,
    model: `Cross Scope Model A ${stamp}`,
    basicLabel: "Alpha Basic",
    premiumLabel: "Alpha Premium",
    fileName: `cross-scope-${stamp}-germany-alpha.csv`,
  };
  const sourceB = {
    ...base,
    country: `Cross Scope France ${stamp}`,
    model: `Cross Scope Model B ${stamp}`,
    basicLabel: "Beta Basic",
    premiumLabel: "Beta Premium",
    fileName: `cross-scope-${stamp}-france-beta.csv`,
  };
  sourceA.csv = sourceCsv(sourceA);
  sourceB.csv = sourceCsv(sourceB);
  const csvPathA = path.join(artifactDir, sourceA.fileName);
  const csvPathB = path.join(artifactDir, sourceB.fileName);
  await writeFile(csvPathA, sourceA.csv, "utf8");
  await writeFile(csvPathB, sourceB.csv, "utf8");

  const observed = { api: [], ui: [], cleanup: null };
  let uploadedSources = [];
  let trimIds = [];
  let browser = null;
  let page = null;
  try {
    uploadedSources = [
      await uploadCsvSource(options, sourceA),
      await uploadCsvSource(options, sourceB),
    ];
    observed.api.push(...uploadedSources.map((source) => ({
      step: "source_uploaded",
      sourceId: source.sourceId,
      fileName: source.fileName,
      country: source.country,
      model: source.model,
      groupId: source.digestInfo.groupId,
      digestTrimIds: source.digestInfo.trimIds,
    })));

    const draftA = await createDraftFromDigest(options, uploadedSources[0]);
    const draftB = await createDraftFromDigest(options, uploadedSources[1]);
    trimIds = [...draftA.compareTrimIds.slice(0, 2), ...draftB.compareTrimIds.slice(0, 2)];
    observed.api.push(
      { step: "draft_created", sourceId: uploadedSources[0].sourceId, trimIds: draftA.compareTrimIds },
      { step: "draft_created", sourceId: uploadedSources[1].sourceId, trimIds: draftB.compareTrimIds },
    );

    const compareData = await getCompareData(options, trimIds);
    const compareTrims = Array.isArray(compareData?.trims) ? compareData.trims : [];
    observed.api.push({
      step: "cross_scope_compare_api",
      trimCount: compareTrims.length,
      rowCount: Array.isArray(compareData?.rows) ? compareData.rows.length : null,
      countries: compareTrims.map((trim) => trim.market || trim.country),
      models: compareTrims.map((trim) => trim.modelName || trim.model_name),
      sourceFileNames: compareTrims.map((trim) => trim.sourceFileName || trim.source_file_name),
    });

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
    const directPickerFlow = await evaluateDirectPickerFlow(page, options, uploadedSources[0], uploadedSources[1], draftA.compareTrimIds.slice(0, 2));
    observed.ui.push({ step: "direct_picker_cross_scope_add", ...directPickerFlow });

    const screenshotPath = path.join(artifactDir, "product_config_cross_scope_direct_picker_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: true });
    observed.cleanup = await cleanupArtifacts(options, uploadedSources, trimIds);
    const summary = {
      createdAt: new Date().toISOString(),
      artifactDir,
      screenshotPath,
      csvPaths: [csvPathA, csvPathB],
      sourceIds: uploadedSources.map((source) => source.sourceId),
      trimIds,
      directPickerFlow,
      cleanup: observed.cleanup,
      observed,
      passed: (
        trimIds.length === 4
        && directPickerFlow.countriesVisible
        && directPickerFlow.modelsVisible
        && directPickerFlow.sourcesVisible
        && directPickerFlow.selectedFourColumns
        && directPickerFlow.noModeText
        && directPickerFlow.noHorizontalOverflow
        && (!observed.cleanup || observed.cleanup.errors.length === 0)
      ),
    };
    const summaryPath = path.join(artifactDir, "product_config_cross_scope_direct_picker_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ summaryPath, ...summary }, null, 2));
    if (!summary.passed) process.exitCode = 1;
  } catch (error) {
    const failureScreenshot = path.join(artifactDir, "failure.png");
    if (page) await page.screenshot({ path: failureScreenshot, fullPage: true }).catch(() => undefined);
    observed.cleanup = await cleanupArtifacts(options, uploadedSources, trimIds).catch((cleanupError) => ({
      enabled: options.cleanup,
      sourceIds: uploadedSources.map((source) => source.sourceId).filter(Boolean),
      trimIds,
      errors: [{ step: "cleanup_after_failure", message: cleanupError instanceof Error ? cleanupError.message : String(cleanupError) }],
    }));
    const summaryPath = path.join(artifactDir, "product_config_cross_scope_direct_picker_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      artifactDir,
      failureScreenshot,
      csvPaths: [csvPathA, csvPathB],
      sourceIds: uploadedSources.map((source) => source.sourceId).filter(Boolean),
      trimIds,
      cleanup: observed.cleanup,
      observed,
      error: error instanceof Error ? error.message : String(error),
      passed: false,
    }, null, 2)}\n`, "utf8");
    console.error(`Product config cross-scope direct picker smoke failed. Summary: ${summaryPath}`);
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
