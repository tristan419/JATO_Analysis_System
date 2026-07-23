import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { chromium } from "playwright";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);

function normaliseImageFormat(value) {
  const raw = String(value || "png").trim().toLowerCase().replace(/^\./, "");
  if (raw === "png") {
    return {
      extension: "png",
      mimeType: "image/png",
      pilFormat: "PNG",
      label: "PNG",
    };
  }
  if (raw === "jpg" || raw === "jpeg") {
    return {
      extension: "jpg",
      mimeType: "image/jpeg",
      pilFormat: "JPEG",
      label: "JPEG",
    };
  }
  throw new Error(`Unsupported review-row image format "${value}". Use png, jpg, or jpeg.`);
}

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "http://127.0.0.1:8004/v1",
    imageFormat: process.env.PRODUCT_CONFIG_REVIEW_ROW_IMAGE_FORMAT || "png",
    country: process.env.PRODUCT_CONFIG_REVIEW_ROW_COUNTRY || "",
    model: process.env.PRODUCT_CONFIG_REVIEW_ROW_MODEL || "",
    powertrain: process.env.PRODUCT_CONFIG_REVIEW_ROW_POWERTRAIN || "ICE",
    segment: process.env.PRODUCT_CONFIG_REVIEW_ROW_SEGMENT || "Smoke Segment",
    brand: process.env.PRODUCT_CONFIG_REVIEW_ROW_BRAND || "ReviewSmoke",
    userName: process.env.PRODUCT_CONFIG_SMOKE_USER || "product-config-review-row-smoke",
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
    else if (arg.startsWith("--image-format=")) options.imageFormat = arg.slice("--image-format=".length);
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
    "Visible Product Config source-library review-row smoke.",
    "",
    "Creates a temporary OCR-readable PNG/JPEG source, searches it from the real Source Digest library,",
    "selects the second review row, maps temporary OCR columns to real config-column names,",
    "creates editable config columns, verifies the formal table focuses that review row, edits it, exports XLSX/PDF, then cleans up.",
    "",
    "Run only against local/staging data:",
    "",
    "  node scripts/product_config_review_row_smoke.mjs --write \\",
    "    --base-url=http://127.0.0.1:5177 \\",
    "    --api-base=http://127.0.0.1:8004/v1",
    "",
    "Options:",
    "  --write                 Required. Acknowledges temporary source/config-column writes.",
    "  --skip-cleanup          Keep generated source/config columns for manual inspection.",
    "  --headed                Show the browser.",
    "  --channel=chrome        Use an installed browser channel instead of Playwright's bundled browser.",
    "  --image-format=jpeg     Use png, jpg, or jpeg. Defaults to png.",
    "  --country='Smoke ...'   Source-library country/market. Defaults to a unique smoke country.",
    "  --model='Smoke ...'     Current page model context. Defaults to a unique smoke model.",
  ].join("\n");
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function slug(value, fallback = "review-row") {
  const text = String(value || "").toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "");
  return text || fallback;
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

async function cleanupArtifacts(options, { sourceId, trimIds }) {
  const cleanup = {
    enabled: options.cleanup,
    sourceId,
    trimIds,
    sourceTrashed: false,
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
  if (sourceId) {
    try {
      await apiJsonRequest(options, "DELETE", `/engineering-config/source/snapshots/${encodeURIComponent(sourceId)}?country=${encodeURIComponent(options.country)}`);
      cleanup.sourceTrashed = true;
    } catch (error) {
      cleanup.errors.push({ step: "trash_source", sourceId, message: error instanceof Error ? error.message : String(error) });
    }
    try {
      const result = await apiJsonRequest(options, "DELETE", `/engineering-config/source/trash?country=${encodeURIComponent(options.country)}`);
      cleanup.sourceTrashCleared = result?.cleared ?? null;
    } catch (error) {
      cleanup.errors.push({ step: "clear_source_trash", message: error instanceof Error ? error.message : String(error) });
    }
  }
  return cleanup;
}

async function generateReviewRowImage(filePath, stamp, imageFormat) {
  const script = String.raw`
from pathlib import Path
import sys
from PIL import Image, ImageDraw, ImageFont

out = Path(sys.argv[1])
stamp = sys.argv[2]
image_format = sys.argv[3]
img = Image.new("RGB", (1160, 430), "white")
d = ImageDraw.Draw(img)
try:
    font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 26)
    small = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 22)
except Exception:
    font = ImageFont.load_default()
    small = font

rows = [
    ["Basic", "Number of seats", "seat count", "5", "5"],
    ["Exterior", "Roof rack", "roof rail", "-", "O"],
    ["Interior", "Black interior", "-", "●", "●"],
    ["Safety", "Rear camera", "-", "●", "●"],
    ["Comfort", "Wireless charge", "charging pad", "-", "●"],
    ["Exterior", "Power sunroof", "-", "-", "●"],
]
xs = [40, 250, 520, 810, 950]
ys = [45, 105, 165, 225, 285, 345]
for x in xs:
    d.line((x - 18, 20, x - 18, 392), fill=(185, 185, 185), width=2)
d.line((1110, 20, 1110, 392), fill=(185, 185, 185), width=2)
for y in [25, 85, 145, 205, 265, 325, 385]:
    d.line((20, y, 1110, y), fill=(185, 185, 185), width=2)
for row, y in zip(rows, ys):
    for text, x in zip(row, xs):
        d.text((x, y), text, fill="black", font=font if len(text) < 15 else small)
d.text((40, 404), f"smoke {stamp}", fill=(248, 248, 248), font=small)
save_args = {"format": image_format}
if image_format == "JPEG":
    save_args["quality"] = 92
img.save(out, **save_args)
`;
  await execFileAsync("python3", ["-c", script, filePath, stamp, imageFormat.pilFormat], { timeout: 30000 });
}

function extractDigestInfo(sourcePayload) {
  const digest = sourcePayload?.sourceDigest || sourcePayload?.source_digest || null;
  const groups = Array.isArray(digest?.compareGroups) ? digest.compareGroups : [];
  const group = groups.find((item) => item?.sourceKind === "ocr_headerless" || item?.identityStatus === "temporary_ocr_column") || groups[0];
  if (!group) {
    throw new Error(`Uploaded source did not produce a comparable Source Digest group: ${JSON.stringify(sourcePayload?.sourceDigestStatus || sourcePayload?.source_digest_status)}`);
  }
  const rows = Array.isArray(group.rows) ? group.rows : [];
  const reviewRows = rows.filter((row) => Array.isArray(row?.reviewFlags) && row.reviewFlags.length > 0);
  if (reviewRows.length < 2) {
    throw new Error(`Expected at least two review rows in the OCR/headerless group; got ${reviewRows.length}`);
  }
  const trims = Array.isArray(group.trims) ? group.trims : [];
  if (trims.length < 2) {
    throw new Error(`Expected at least two temporary OCR columns; got ${trims.length}`);
  }
  return {
    digest,
    group,
    groupId: String(group.groupId || group.id || ""),
    title: String(group.title || group.modelName || "OCR Headerless Model"),
    reviewRows,
    selectedReviewFeature: String(reviewRows[1]?.featureName || reviewRows[1]?.featureCode || ""),
    trims,
  };
}

async function uploadImageSource(options, imagePath, fileName, imageFormat) {
  const bytes = await readFile(imagePath);
  const params = new URLSearchParams({
    file_name: fileName,
    total_size: String(bytes.length),
    mime_type: imageFormat.mimeType,
  });
  const initiate = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/initiate?${params.toString()}`);
  const uploadId = initiate?.uploadId || initiate?.upload_id;
  if (!uploadId) throw new Error(`Source upload initiate did not return uploadId: ${JSON.stringify(initiate)}`);
  await apiBinaryRequest(options, "PUT", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/parts/0`, bytes, imageFormat.mimeType);
  const relatedContext = {
    brand: options.brand,
    model: options.model,
    market: options.country,
    country: options.country,
    powertrain: options.powertrain,
    segment: options.segment,
    contextType: "model_trim_compare_target",
    scenario: "source_library_review_row_smoke",
  };
  const complete = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/complete`, { relatedContext });
  const sourceId = complete?.sourceId || complete?.source_id || complete?.importBatchId || complete?.import_batch_id;
  if (!sourceId) throw new Error(`Source upload complete did not return sourceId: ${JSON.stringify(complete)}`);
  return { uploadId, sourceId, complete, relatedContext };
}

async function openFloatingDeck(page) {
  const panel = page.locator(".deck-floating-panel");
  if (await panel.isVisible().catch(() => false)) return;
  const trigger = page.getByRole("button", { name: /添加配置列\s*\/\s*显示|添加 TRIM\s*\/\s*显示|打开控制/ }).first();
  await trigger.waitFor({ state: "visible", timeout: 30000 });
  await trigger.click();
  await page.locator(".deck-floating-panel").waitFor({ state: "visible", timeout: 30000 });
}

async function openDisplayPanel(page) {
  await openFloatingDeck(page);
  await page.getByRole("tab", { name: /显示\s*\/\s*编辑|显示模式/ }).click({ timeout: 15000 });
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

async function editSelectedReviewFeature(page, featureName, options) {
  await openDisplayPanel(page);
  const editControl = page.getByLabel("在线编辑控制");
  await editControl.waitFor({ state: "visible", timeout: 15000 });
  await editControl.getByRole("button", { name: "开启在线编辑" }).click({ timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll(".compare-cell--editable").length > 0, null, { timeout: 15000 });

  const row = page.locator("tr").filter({ hasText: new RegExp(escapeRegExp(featureName), "i") }).first();
  await row.waitFor({ state: "visible", timeout: options.timeoutMs });
  const editableCell = row.locator(".compare-cell--editable").first();
  const beforeText = await editableCell.innerText({ timeout: 15000 });
  const saveResponsePromise = page.waitForResponse((response) => (
    response.url().includes("/engineering-config/values")
    && ["PATCH", "POST"].includes(response.request().method())
  ), { timeout: options.timeoutMs });
  await editableCell.click({ timeout: 15000 });
  const input = page.locator(".compare-cell-edit-input");
  await input.waitFor({ state: "visible", timeout: 15000 });
  await input.fill("O");
  await page.locator(".compare-cell-edit-save").click({ timeout: 15000 });
  const saveResponse = await saveResponsePromise;
  const savePayload = await saveResponse.json();
  await page.waitForFunction(({ featureName: targetFeature }) => {
    const rows = Array.from(document.querySelectorAll("tr"));
    return rows.some((item) => {
      const text = item.textContent || "";
      return text.includes(targetFeature) && text.includes("选装");
    });
  }, { featureName }, { timeout: options.timeoutMs });
  const afterText = await row.innerText({ timeout: 15000 });
  return {
    featureName,
    beforeText: beforeText.replace(/\s+/g, " ").trim(),
    afterText: afterText.replace(/\s+/g, " ").trim(),
    saveStatus: saveResponse.status(),
    savePayload,
    savedAsOptional: savePayload?.rawValue === "O" && savePayload?.displayValue === "选装" && afterText.includes("选装"),
  };
}

function editedReviewValueFromExportPayload(payload, featureName) {
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  const row = rows.find((item) => {
    const name = String(item?.featureName || item?.feature_name || "");
    const code = String(item?.featureCode || item?.feature_code || "");
    return name.includes(featureName) || code.toLowerCase().includes(featureName.toLowerCase().replace(/\s+/g, "_"));
  });
  const values = Array.isArray(row?.values) ? row.values : [];
  const editedValue = values.find((value) => (
    value
    && (value.rawValue === "O" || value.raw_value === "O")
    && (value.displayValue === "选装" || value.display_value === "选装")
  )) || null;
  return {
    foundRow: Boolean(row),
    featureName: row?.featureName || row?.feature_name || null,
    targetRawValue: editedValue?.rawValue ?? editedValue?.raw_value ?? null,
    targetDisplayValue: editedValue?.displayValue ?? editedValue?.display_value ?? null,
    targetAvailability: editedValue?.availability ?? null,
  };
}

async function waitForDeckExportButton(page, formatLabel) {
  await openDisplayPanel(page);
  const button = page.getByRole("button", { name: new RegExp(`导出当前范围\\s*${formatLabel}`, "i") });
  await button.waitFor({ state: "visible", timeout: 15000 });
  await page.waitForFunction((label) => {
    const buttons = Array.from(document.querySelectorAll("button"));
    const target = buttons.find((item) => item.textContent?.replace(/\s+/g, " ").trim().includes(`导出当前范围 ${label}`));
    return Boolean(target && !target.disabled);
  }, formatLabel, { timeout: 15000 });
  return button;
}

async function clickAndInspectExportResponse(page, format, featureName) {
  const formatLabel = format.toUpperCase();
  const endpoint = `/engineering-config/compare/export/${format}`;
  const button = await waitForDeckExportButton(page, formatLabel);
  const previousDownloadCount = await page.evaluate(() => window.__productConfigDownloads?.length ?? 0);
  const responsePromise = page.waitForResponse((response) => (
    response.url().includes(endpoint) && response.request().method() === "POST"
  ), { timeout: 45000 });
  await button.click({ timeout: 15000 });
  const response = await responsePromise;
  const payload = response.request().postDataJSON();
  const networkBody = await response.body();
  await page.waitForFunction((count) => {
    const downloads = window.__productConfigDownloads ?? [];
    const item = downloads[count];
    return Boolean(item && item.ready);
  }, previousDownloadCount, { timeout: 15000 });
  const download = await page.evaluate((count) => window.__productConfigDownloads?.[count] ?? null, previousDownloadCount);
  const firstBytes = Array.isArray(download?.firstBytes) ? download.firstBytes : [];
  const signatureOk = format === "xlsx"
    ? firstBytes.length >= 2 && firstBytes[0] === 0x50 && firstBytes[1] === 0x4b
    : firstBytes.slice(0, 4).map((value) => String.fromCharCode(value)).join("") === "%PDF";
  const editedValue = editedReviewValueFromExportPayload(payload, featureName);
  return {
    format,
    endpoint,
    status: response.status(),
    contentType: response.headers()["content-type"] ?? "",
    networkBytes: networkBody.length,
    downloadType: download?.type ?? "",
    downloadBytes: download?.size ?? 0,
    firstBytes,
    signatureOk,
    editedValue,
    editedValueInPayload: editedValue.foundRow
      && editedValue.targetRawValue === "O"
      && editedValue.targetDisplayValue === "选装",
  };
}

async function runSmoke(options) {
  if (!options.write) {
    throw new Error("Refusing to run write-path smoke. Pass --write to create temporary source/config-column rows.");
  }
  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const runId = nowStamp();
  const stamp = runId.toLowerCase().replace(/[^a-z0-9]+/g, "").slice(0, 14);
  const imageFormat = normaliseImageFormat(options.imageFormat);
  if (!options.country) options.country = `Review Row Smoke Country ${stamp}`;
  if (!options.model) options.model = `Review Row Browser Smoke ${stamp}`;
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-review-row-smoke", runId);
  await mkdir(artifactDir, { recursive: true });
  const fileName = `review-row-source-${stamp}.${imageFormat.extension}`;
  const imagePath = path.join(artifactDir, fileName);
  await generateReviewRowImage(imagePath, stamp, imageFormat);

  let sourceId = null;
  let trimIds = [];
  const observed = { api: [], ui: [], cleanup: null };
  let browser = null;
  let page = null;
  try {
    const upload = await uploadImageSource(options, imagePath, fileName, imageFormat);
    sourceId = upload.sourceId;
    const digestInfo = extractDigestInfo(upload.complete);
    observed.api.push({
      step: "source_uploaded",
      sourceId,
      uploadId: upload.uploadId,
      fileName,
      imageFormat: imageFormat.label,
      mimeType: imageFormat.mimeType,
      groupId: digestInfo.groupId,
      reviewRowCount: digestInfo.reviewRows.length,
      selectedReviewFeature: digestInfo.selectedReviewFeature,
      temporaryTrimCount: digestInfo.trims.length,
      sourceDigestStatus: upload.complete?.sourceDigestStatus || upload.complete?.source_digest_status,
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
    const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
    await context.addInitScript(({ userName }) => {
      window.localStorage.setItem("jato_user_name", userName);
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
    }, { userName: options.userName });
    page = await context.newPage();
    await page.goto(targetUrl.toString(), { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => undefined);

    await openFloatingDeck(page);
    await page.getByRole("tab", { name: /来源\s*\/\s*上传[\s\S]*配置表[\s\S]*价格单|Source Digest/ }).click({ timeout: 15000 });
    const sourceSearch = page.getByRole("combobox", { name: /搜索来源\s*\/\s*(?:车型|Model)\s*\/\s*配置列|搜索 Source Digest 可比组/ });
    await sourceSearch.fill(fileName);
    const sourceHit = page.locator(".product-config-source-snapshot-hint").filter({ hasText: fileName }).first();
    await sourceHit.waitFor({ state: "visible", timeout: options.timeoutMs });
    await sourceHit.click({ timeout: 15000 }).catch(() => undefined);
    await openSourceDigestDetail(page);

    const digestPanel = page.getByLabel("Source Digest 按来源和品牌浏览").first();
    await digestPanel.waitFor({ state: "visible", timeout: options.timeoutMs });
    await page.getByText(/OCR 临时列|临时 OCR 列身份/).first().waitFor({ state: "visible", timeout: options.timeoutMs });
    await page.getByText(/需核对\s*2\s*行|需核对 2 行/).first().waitFor({ state: "visible", timeout: options.timeoutMs });

    const reviewRows = page.getByLabel(new RegExp(`${escapeRegExp(options.model)}.*需核对配置行定位|需核对配置行定位`)).first();
    await reviewRows.waitFor({ state: "visible", timeout: options.timeoutMs });
    await reviewRows.getByText("需核对行 2").click({ timeout: 15000 });
    const selectReviewButton = reviewRows.getByRole("button", {
      name: new RegExp(`建列后定位此行：${escapeRegExp(digestInfo.selectedReviewFeature)}`),
    });
    await selectReviewButton.click({ timeout: 15000 });
    await reviewRows.getByRole("button", {
      name: new RegExp(`已设为建列后定位：${escapeRegExp(digestInfo.selectedReviewFeature)}`),
    }).waitFor({ state: "visible", timeout: 15000 });
    observed.ui.push({ step: "selected_review_row", feature: digestInfo.selectedReviewFeature });

    const trimNames = ["Review Basic", "Review Comfort", "Review Premium", "Review Elite"];
    for (let index = 0; index < Math.min(digestInfo.trims.length, trimNames.length); index += 1) {
      await page.getByLabel(`OCR Column ${index + 1} 配置列`).fill(trimNames[index]);
    }

    const groupButton = page.locator('button[aria-label^="选择 Source Digest 可比组："]').filter({ hasText: /OCR 临时列|临时 OCR 列身份/ }).first();
    await page.waitForFunction(() => {
      const button = Array.from(document.querySelectorAll('button[aria-label^="选择 Source Digest 可比组："]'))
        .find((element) => /OCR 临时列|临时 OCR 列身份/.test(element.textContent || ""));
      return Boolean(button && !button.disabled);
    }, null, { timeout: options.timeoutMs });

    const draftResponsePromise = page.waitForResponse((response) => (
      response.url().includes("/engineering-config/source/snapshots/")
      && response.url().includes("/digest-groups/")
      && response.url().includes("/draft")
      && response.request().method() === "POST"
    ), { timeout: options.timeoutMs });
    await groupButton.click({ timeout: 15000 });
    const draftResponse = await draftResponsePromise;
    const draftPayload = await draftResponse.json();
    trimIds = Array.isArray(draftPayload.compareTrimIds)
      ? draftPayload.compareTrimIds
      : Array.isArray(draftPayload.trimIds)
        ? draftPayload.trimIds
        : [];
    observed.api.push({ step: "draft_created", status: draftResponse.status(), trimIds });

    await page.getByLabel(/Source Digest 建列成功|来源建列成功/).waitFor({ state: "visible", timeout: options.timeoutMs });
    await page.getByText(new RegExp(`定位到需核对行：${escapeRegExp(digestInfo.selectedReviewFeature)}`)).waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });
    await page.waitForFunction((featureName) => {
      const activeRows = Array.from(document.querySelectorAll(".compare-row-active[aria-selected='true']"));
      return activeRows.some((row) => (row.textContent || "").includes(featureName));
    }, digestInfo.selectedReviewFeature, { timeout: options.timeoutMs });
    const successCard = page.getByLabel(/Source Digest 建列成功|来源建列成功/);
    await successCard.getByRole("button", { name: "跳到需核对行" }).click({ timeout: 15000 });
    observed.ui.push({ step: "formal_row_highlighted", feature: digestInfo.selectedReviewFeature });

    const editResult = await editSelectedReviewFeature(page, digestInfo.selectedReviewFeature, options);
    observed.ui.push({ step: "edited_selected_review_feature", ...editResult });
    const xlsx = await clickAndInspectExportResponse(page, "xlsx", digestInfo.selectedReviewFeature);
    const pdf = await clickAndInspectExportResponse(page, "pdf", digestInfo.selectedReviewFeature);
    observed.ui.push({ step: "export_xlsx_after_review_edit", ...xlsx });
    observed.ui.push({ step: "export_pdf_after_review_edit", ...pdf });

    const screenshotPath = path.join(artifactDir, "product_config_review_row_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: false });
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds });
    const summary = {
      createdAt: new Date().toISOString(),
      targetUrl: targetUrl.toString(),
      artifactDir,
      screenshotPath,
      imagePath,
      imageFormat: imageFormat.label,
      mimeType: imageFormat.mimeType,
      pngPath: imageFormat.extension === "png" ? imagePath : null,
      fileName,
      sourceId,
      trimIds,
      selectedReviewFeature: digestInfo.selectedReviewFeature,
      reviewRowCount: digestInfo.reviewRows.length,
      cleanup: observed.cleanup,
      observed,
      passed: trimIds.length >= 2
        && observed.ui.some((item) => item.step === "selected_review_row")
        && observed.ui.some((item) => item.step === "formal_row_highlighted")
        && editResult.savedAsOptional
        && xlsx.status === 200
        && xlsx.signatureOk
        && xlsx.editedValueInPayload
        && String(xlsx.contentType).includes("spreadsheetml.sheet")
        && pdf.status === 200
        && pdf.signatureOk
        && pdf.editedValueInPayload
        && String(pdf.contentType).includes("application/pdf")
        && (!observed.cleanup || observed.cleanup.errors.length === 0),
    };
    const summaryPath = path.join(artifactDir, "product_config_review_row_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ summaryPath, ...summary }, null, 2));
    if (!summary.passed) process.exitCode = 1;
  } catch (error) {
    const failureScreenshot = path.join(artifactDir, "failure.png");
    if (page) await page.screenshot({ path: failureScreenshot, fullPage: false }).catch(() => undefined);
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds }).catch((cleanupError) => ({
      enabled: options.cleanup,
      sourceId,
      trimIds,
      errors: [{ step: "cleanup_after_failure", message: cleanupError instanceof Error ? cleanupError.message : String(cleanupError) }],
    }));
    const summaryPath = path.join(artifactDir, "product_config_review_row_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      artifactDir,
      failureScreenshot,
      imagePath,
      imageFormat: imageFormat.label,
      mimeType: imageFormat.mimeType,
      pngPath: imageFormat.extension === "png" ? imagePath : null,
      fileName,
      sourceId,
      trimIds,
      cleanup: observed.cleanup,
      observed,
      error: error instanceof Error ? error.message : String(error),
      passed: false,
    }, null, 2)}\n`, "utf8");
    console.error(`Product config review-row smoke failed. Summary: ${summaryPath}`);
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
