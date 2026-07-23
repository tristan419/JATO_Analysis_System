import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { chromium } from "playwright";
import { access, mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);

function parseArgs(argv) {
  const options = {
    baseUrl: process.env.PRODUCT_CONFIG_SMOKE_BASE_URL || "http://127.0.0.1:5177",
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "http://127.0.0.1:8004/v1",
    country: process.env.PRODUCT_CONFIG_EDIT_EXPORT_COUNTRY || "",
    model: process.env.PRODUCT_CONFIG_EDIT_EXPORT_MODEL || "",
    brand: process.env.PRODUCT_CONFIG_EDIT_EXPORT_BRAND || "EditExportSmoke",
    powertrain: process.env.PRODUCT_CONFIG_EDIT_EXPORT_POWERTRAIN || "ICE",
    segment: process.env.PRODUCT_CONFIG_EDIT_EXPORT_SEGMENT || "Smoke Segment",
    sourceFormat: process.env.PRODUCT_CONFIG_EDIT_EXPORT_SOURCE_FORMAT || "csv",
    userName: process.env.PRODUCT_CONFIG_SMOKE_USER || "product-config-edit-export-smoke",
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
    else if (arg.startsWith("--source-format=")) options.sourceFormat = arg.slice("--source-format=".length);
    else if (arg.startsWith("--user-name=")) options.userName = arg.slice("--user-name=".length);
    else if (arg.startsWith("--channel=")) options.browserChannel = arg.slice("--channel=".length);
    else if (arg.startsWith("--timeout-ms=")) options.timeoutMs = Math.max(30000, Number(arg.slice("--timeout-ms=".length)) || options.timeoutMs);
  }
  options.baseUrl = options.baseUrl.replace(/\/+$/, "");
  options.apiBase = options.apiBase.replace(/\/+$/, "");
  options.sourceFormat = normaliseSourceFormat(options.sourceFormat);
  return options;
}

function usage() {
  return [
    "Visible Product Config edit-then-export smoke.",
    "",
    "Creates temporary editable config columns from a Source Digest CSV/XLSX/PDF/price list, opens the real Product Compare UI,",
    "enables guarded online editing from FloatingDeck, edits one formal config cell, exports XLSX/PDF,",
    "and verifies both export request payloads include the edited value before cleanup.",
    "",
    "Run only against local/staging data:",
    "",
    "  node scripts/product_config_edit_export_smoke.mjs --write \\",
    "    --base-url=http://127.0.0.1:5177 \\",
    "    --api-base=http://127.0.0.1:8004/v1",
    "",
    "Options:",
    "  --write                 Required. Acknowledges temporary source/config-column writes.",
    "  --skip-cleanup          Keep generated source/config columns for manual inspection.",
    "  --headed                Show the browser.",
    "  --channel=chrome        Use an installed browser channel instead of Playwright's bundled browser.",
    "  --source-format=xlsx     Upload a generated workbook source instead of CSV. Defaults to csv.",
    "  --source-format=pdf-text Upload a generated text PDF source instead of CSV. Defaults to csv.",
    "  --source-format=price-list-csv Upload a generated competitor price-list CSV source.",
    "  --country='Smoke ...'   Country/market. Defaults to a unique smoke country.",
    "  --model='Smoke ...'     Model context. Defaults to a unique smoke model.",
  ].join("\n");
}

function normaliseSourceFormat(value) {
  const raw = String(value || "csv").trim().toLowerCase().replace(/_/g, "-");
  if (raw === "csv") return "csv";
  if (raw === "xlsx" || raw === "workbook" || raw === "excel") return "xlsx";
  if (raw === "pdf" || raw === "pdf-text" || raw === "text-pdf") return "pdf-text";
  if (raw === "price-list" || raw === "price-list-csv" || raw === "price-csv") return "price-list-csv";
  throw new Error(`Unsupported source format "${value}". Use csv, xlsx, pdf-text, or price-list-csv.`);
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function csvCell(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
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

function sourceRows(options, stamp) {
  return [
    ["Feature", "Edit Basic", "Edit Premium"],
    ["Brand / 品牌", options.brand, options.brand],
    ["Model / 车型", options.model, options.model],
    ["Country / 国家", options.country, options.country],
    ["Model year / 年款", "2026", "2026"],
    ["Powertrain / 动力", options.powertrain, options.powertrain],
    ["Segment / 级别", options.segment, options.segment],
    ["Configuration version / 配置版型", "Edit Basic", "Edit Premium"],
    ["Material No. / 物料号", "", ""],
    ["Rear Visual parking assist / 动态辅助线倒车影像", "-", "●"],
    ["360 round view camera / 360度高清全景影像", "-", "●"],
    ["Power sunroof / 电动天窗", "-", "O"],
    ["Wireless charging / 手机无线充电", "-", "●"],
    ["SONY 8 speakers / SONY 8扬声器", "-", "●"],
    ["Edit smoke marker / 编辑验收标记", stamp, stamp],
  ];
}

function sourcePdfRows(options, stamp) {
  return [
    ["Feature", "Edit Basic", "Edit Premium"],
    ["Brand", options.brand, options.brand],
    ["Model", options.model, options.model],
    ["Country", options.country, options.country],
    ["Model year", "2026", "2026"],
    ["Powertrain", options.powertrain, options.powertrain],
    ["Segment", options.segment, options.segment],
    ["Configuration version", "Edit Basic", "Edit Premium"],
    ["Material No.", "", ""],
    ["Rear Visual parking assist", "-", "S"],
    ["360 round view camera", "-", "S"],
    ["Power sunroof", "-", "O"],
    ["Wireless charging", "-", "S"],
    ["SONY 8 speakers", "-", "S"],
    ["Edit smoke marker", stamp, stamp],
  ];
}

function priceListRows(options) {
  return [
    ["Brand", "Model", "Trim", "Market", "Model Year", "Powertrain", "MSRP", "Currency"],
    [options.brand, options.model, "Edit Basic", options.country, "2026", options.powertrain, "23000", "EUR"],
    [options.brand, options.model, "Edit Premium", options.country, "2026", options.powertrain, "28000", "EUR"],
  ];
}

function sourceCsvFromRows(rows) {
  return `${rows.map((row) => row.map(csvCell).join(",")).join("\n")}\n`;
}

async function fileExists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function pythonExecutable(frontendRoot) {
  if (process.env.PRODUCT_CONFIG_SMOKE_PYTHON) return process.env.PRODUCT_CONFIG_SMOKE_PYTHON;
  const repoRoot = path.resolve(frontendRoot, "../..");
  const venvPython = path.join(repoRoot, ".venv", "bin", "python");
  if (await fileExists(venvPython)) return venvPython;
  return "python3";
}

async function sourceXlsxBufferFromRows(rows, frontendRoot) {
  const script = String.raw`
import base64
from io import BytesIO
import json
import sys

from openpyxl import Workbook

rows = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
sheet_name = str(sys.argv[2] or "Config")[:31] or "Config"
workbook = Workbook()
sheet = workbook.active
sheet.title = sheet_name
for row in rows:
    sheet.append(row)
buffer = BytesIO()
workbook.save(buffer)
workbook.close()
sys.stdout.buffer.write(buffer.getvalue())
`;
  const rowsArg = Buffer.from(JSON.stringify(rows), "utf8").toString("base64");
  const { stdout } = await execFileAsync(
    await pythonExecutable(frontendRoot),
    ["-c", script, rowsArg, "Config"],
    { encoding: "buffer", maxBuffer: 5 * 1024 * 1024, timeout: 30000 },
  );
  return stdout;
}

function sourcePdfTextFromRows(rows) {
  return `${rows.map((row) => row.map((value) => String(value ?? "")).join(" | ")).join("\n")}\n`;
}

function pdfText(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
}

function textPdfBuffer(lines) {
  const commands = ["BT /F1 12 Tf 14 TL 72 720 Td"];
  lines.forEach((line, index) => {
    if (index > 0) commands.push("T*");
    commands.push(`(${pdfText(line)}) Tj`);
  });
  commands.push("ET");
  const stream = Buffer.from(commands.join(" "), "utf8");
  const objects = [
    Buffer.from("1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n", "ascii"),
    Buffer.from("2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n", "ascii"),
    Buffer.from("3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n", "ascii"),
    Buffer.from("4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n", "ascii"),
    Buffer.concat([
      Buffer.from(`5 0 obj << /Length ${stream.length} >> stream\n`, "ascii"),
      stream,
      Buffer.from("\nendstream endobj\n", "ascii"),
    ]),
  ];
  const chunks = [Buffer.from("%PDF-1.4\n", "ascii")];
  const offsets = [];
  for (const item of objects) {
    offsets.push(chunks.reduce((total, chunk) => total + chunk.length, 0));
    chunks.push(item);
  }
  const xrefOffset = chunks.reduce((total, chunk) => total + chunk.length, 0);
  chunks.push(Buffer.from(`xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`, "ascii"));
  for (const offset of offsets) {
    chunks.push(Buffer.from(`${String(offset).padStart(10, "0")} 00000 n \n`, "ascii"));
  }
  chunks.push(Buffer.from(`trailer << /Root 1 0 R /Size ${objects.length + 1} >>\nstartxref\n${xrefOffset}\n%%EOF`, "ascii"));
  return Buffer.concat(chunks);
}

async function sourcePayload(options, stamp, frontendRoot) {
  if (options.sourceFormat === "xlsx") {
    const rows = sourceRows(options, stamp);
    return {
      bytes: await sourceXlsxBufferFromRows(rows, frontendRoot),
      fileName: `edit-export-source-${stamp}.xlsx`,
      contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      sourceText: sourceCsvFromRows(rows),
    };
  }
  if (options.sourceFormat === "pdf-text") {
    const rows = sourcePdfRows(options, stamp);
    const text = sourcePdfTextFromRows(rows);
    return {
      bytes: textPdfBuffer(text.split(/\r?\n/).filter((line) => line.length > 0)),
      fileName: `edit-export-source-${stamp}.pdf`,
      contentType: "application/pdf",
      sourceText: text,
    };
  }
  if (options.sourceFormat === "price-list-csv") {
    const csv = sourceCsvFromRows(priceListRows(options));
    return {
      bytes: Buffer.from(csv, "utf8"),
      fileName: `edit-export-price-list-${stamp}.csv`,
      contentType: "text/csv",
      sourceText: csv,
    };
  }
  const rows = sourceRows(options, stamp);
  const csv = sourceCsvFromRows(rows);
  return {
    bytes: Buffer.from(csv, "utf8"),
    fileName: `edit-export-source-${stamp}.csv`,
    contentType: "text/csv",
    sourceText: csv,
  };
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

async function uploadSource(options, source) {
  const bytes = source.bytes;
  const params = new URLSearchParams({
    file_name: source.fileName,
    total_size: String(bytes.length),
    mime_type: source.contentType,
  });
  const initiate = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/initiate?${params.toString()}`);
  const uploadId = initiate?.uploadId || initiate?.upload_id;
  if (!uploadId) throw new Error(`Source upload initiate did not return uploadId: ${JSON.stringify(initiate)}`);
  await apiBinaryRequest(options, "PUT", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/parts/0`, bytes, source.contentType);
  const relatedContext = {
    brand: options.brand,
    model: options.model,
    market: options.country,
    country: options.country,
    modelYear: "2026",
    powertrain: options.powertrain,
    segment: options.segment,
    contextType: "model_trim_compare_target",
    scenario: "edit_export_smoke",
  };
  const complete = await apiJsonRequest(options, "POST", `/engineering-config/source/upload/${encodeURIComponent(uploadId)}/complete`, { relatedContext });
  const sourceId = complete?.sourceId || complete?.source_id || complete?.importBatchId || complete?.import_batch_id;
  if (!sourceId) throw new Error(`Source upload complete did not return sourceId: ${JSON.stringify(complete)}`);
  return { uploadId, sourceId, complete, relatedContext, digestInfo: extractDigestInfo(complete) };
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
    } catch (error) {
      cleanup.errors.push({ step: "trash_source", sourceId, message: error instanceof Error ? error.message : String(error) });
    }
    try {
      const result = await apiJsonRequest(options, "DELETE", `/engineering-config/source/trash?country=${encodeURIComponent(options.country)}`);
      cleanup.sourceTrashCleared = result?.cleared ?? null;
    } catch (error) {
      cleanup.errors.push({ step: "clear_source_trash", message: error instanceof Error ? error.message : String(error) });
    }
    try {
      await apiJsonRequest(options, "DELETE", `/engineering-config/source/snapshots/${encodeURIComponent(sourceId)}`);
      cleanup.sourceGloballyTrashed = true;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      cleanup.sourceGloballyTrashed = false;
      if (!message.includes("HTTP 404")) {
        cleanup.errors.push({ step: "trash_source_global", sourceId, message });
      }
    }
  }
  return cleanup;
}

async function openFloatingDeck(page) {
  const panel = page.locator(".engineering-config-control-panel.deck-floating-panel");
  if (await panel.isVisible().catch(() => false)) return;
  await page.getByRole("button", { name: /添加配置列\s*\/\s*显示|添加 TRIM\s*\/\s*显示|编辑已开启\s*\/\s*显示|打开编辑控制|打开控制/ }).click({ timeout: 15000 });
  await panel.waitFor({ state: "visible", timeout: 15000 });
}

async function openDisplayPanel(page) {
  await openFloatingDeck(page);
  await page.getByRole("tab", { name: /显示\s*\/\s*编辑|显示模式/ }).click({ timeout: 15000 });
}

function editScenario(options) {
  if (options.sourceFormat === "price-list-csv") {
    return {
      name: "msrp",
      step: "edited_msrp_premium",
      rowPattern: /MSRP/i,
      expectedRowText: "28500",
      inputValue: "28500",
      expectedRawValue: "28500",
      expectedDisplayValue: "28500",
      expectedAvailability: "VALUE",
      featurePattern: /MSRP/i,
      featureCodePattern: /price.*msrp|msrp/i,
    };
  }
  return {
    name: "wireless_charging",
    step: "edited_wireless_charging_premium",
    rowPattern: /Wireless charging|手机无线充电/i,
    expectedRowText: "选装",
    inputValue: "O",
    expectedRawValue: "O",
    expectedDisplayValue: "选装",
    expectedAvailability: "OPTIONAL",
    featurePattern: /Wireless charging|手机无线充电/i,
    featureCodePattern: /wireless/i,
  };
}

async function editPremiumCell(page, options) {
  const scenario = editScenario(options);
  await openDisplayPanel(page);
  const editControl = page.locator('.comparison-drawer-view-mode[aria-label="在线编辑控制"]');
  await editControl.waitFor({ state: "visible", timeout: 15000 });
  await editControl.getByRole("button", { name: "开启在线编辑" }).click({ timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll(".compare-cell--editable").length > 0, null, { timeout: 15000 });
  const deckPanel = page.locator(".engineering-config-control-panel");
  if (await deckPanel.isVisible().catch(() => false)) {
    await deckPanel.getByRole("button", { name: "关闭", exact: true }).click({ timeout: 15000 });
    await deckPanel.waitFor({ state: "hidden", timeout: 15000 });
  }

  const row = page.locator("tr").filter({ hasText: scenario.rowPattern }).first();
  await row.waitFor({ state: "visible", timeout: options.timeoutMs });
  const premiumCell = row.locator(".compare-cell--editable").nth(1);
  const beforeText = await premiumCell.innerText({ timeout: 15000 });
  await premiumCell.click({ timeout: 15000 });
  const input = page.locator(".compare-cell-edit-input");
  await input.waitFor({ state: "visible", timeout: 15000 });
  await input.fill(scenario.inputValue);
  const saveResponsePromise = page.waitForResponse((response) => (
    response.url().includes("/engineering-config/values/")
    && response.request().method() === "PATCH"
  ), { timeout: options.timeoutMs });
  await page.locator(".compare-cell-edit-save").click({ timeout: 15000 });
  const saveResponse = await saveResponsePromise;
  const savePayload = await saveResponse.json();
  await page.waitForFunction(({ rowPatternText, expectedRowText }) => {
    const rowPattern = new RegExp(rowPatternText, "i");
    const rows = Array.from(document.querySelectorAll("tr"));
    return rows.some((item) => {
      const text = item.textContent || "";
      return rowPattern.test(text) && text.includes(expectedRowText);
    });
  }, { rowPatternText: scenario.rowPattern.source, expectedRowText: scenario.expectedRowText }, { timeout: options.timeoutMs });
  const afterText = await row.innerText({ timeout: 15000 });
  const savedAsExpected = afterText.includes(scenario.expectedRowText)
    && savePayload?.rawValue === scenario.expectedRawValue
    && savePayload?.displayValue === scenario.expectedDisplayValue;
  return {
    scenario: scenario.name,
    beforeText: beforeText.replace(/\s+/g, " ").trim(),
    afterText: afterText.replace(/\s+/g, " ").trim(),
    saveStatus: saveResponse.status(),
    savePayload,
    savedAsOptional: scenario.name === "wireless_charging" && savedAsExpected,
    savedAsExpected,
  };
}

function editedValueFromExportPayload(payload, options) {
  const scenario = editScenario(options);
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  const row = rows.find((item) => {
    const featureName = String(item?.featureName || item?.feature_name || "");
    const featureCode = String(item?.featureCode || item?.feature_code || "");
    return scenario.featurePattern.test(featureName) || scenario.featureCodePattern.test(featureCode);
  });
  const values = Array.isArray(row?.values) ? row.values : [];
  const targetValue = values[1] || null;
  return {
    foundRow: Boolean(row),
    featureName: row?.featureName || row?.feature_name || null,
    targetRawValue: targetValue?.rawValue ?? targetValue?.raw_value ?? null,
    targetDisplayValue: targetValue?.displayValue ?? targetValue?.display_value ?? null,
    targetAvailability: targetValue?.availability ?? null,
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

async function clickAndInspectExportResponse(page, format, options) {
  const scenario = editScenario(options);
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
  const editedValue = editedValueFromExportPayload(payload, options);
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
      && editedValue.targetRawValue === scenario.expectedRawValue
      && editedValue.targetDisplayValue === scenario.expectedDisplayValue,
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
  if (!options.country) options.country = `Edit Export Smoke Country ${stamp}`;
  if (!options.model) options.model = `Edit Export Smoke Model ${stamp}`;
  const artifactDir = path.join(frontendRoot, "artifacts", "product-config-edit-export-smoke", runId);
  await mkdir(artifactDir, { recursive: true });
  const source = await sourcePayload(options, stamp, frontendRoot);
  const sourcePath = path.join(artifactDir, source.fileName);
  await writeFile(sourcePath, source.bytes);
  const sourceTextPath = path.join(artifactDir, `${source.fileName}.source.txt`);
  await writeFile(sourceTextPath, source.sourceText, "utf8");

  const observed = { api: [], ui: [], responses: [], cleanup: null };
  let sourceId = null;
  let trimIds = [];
  let browser = null;
  let page = null;
  try {
    const upload = await uploadSource(options, source);
    sourceId = upload.sourceId;
    observed.api.push({
      step: "source_uploaded",
      sourceId,
      uploadId: upload.uploadId,
      fileName: source.fileName,
      sourceFormat: options.sourceFormat,
      contentType: source.contentType,
      groupId: upload.digestInfo.groupId,
      digestTrimIds: upload.digestInfo.trimIds,
      sourceDigestStatus: upload.complete?.sourceDigestStatus || upload.complete?.source_digest_status,
    });
    const draft = await createDraftFromDigest(options, upload);
    trimIds = draft.compareTrimIds.slice(0, 2);
    observed.api.push({
      step: "draft_created",
      sourceId,
      trimIds,
      createdTrimCount: draft.result?.createdTrimCount ?? draft.result?.created_trim_count ?? null,
      reusedTrimCount: draft.result?.reusedTrimCount ?? draft.result?.reused_trim_count ?? null,
    });

    const targetUrl = new URL(`${options.baseUrl}/product/compare/config`);
    targetUrl.searchParams.set("trimIds", trimIds.join(","));
    targetUrl.searchParams.set("baseTrimId", trimIds[0]);
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
    await page.getByText(/当前展示 \d+\/\d+ 配置行/).waitFor({ state: "visible", timeout: options.timeoutMs });

    const editResult = await editPremiumCell(page, options);
    observed.ui.push({ step: editResult.step || editScenario(options).step, ...editResult });
    const xlsx = await clickAndInspectExportResponse(page, "xlsx", options);
    const pdf = await clickAndInspectExportResponse(page, "pdf", options);
    observed.responses.push({ step: "export_xlsx_after_edit", ...xlsx });
    observed.responses.push({ step: "export_pdf_after_edit", ...pdf });

    const screenshotPath = path.join(artifactDir, "product_config_edit_export_smoke.png");
    await page.screenshot({ path: screenshotPath, fullPage: true });
    observed.cleanup = await cleanupArtifacts(options, { sourceId, trimIds });
    const summary = {
      createdAt: new Date().toISOString(),
      targetUrl: targetUrl.toString(),
      artifactDir,
      screenshotPath,
      sourcePath,
      sourceTextPath,
      fileName: source.fileName,
      sourceFormat: options.sourceFormat,
      contentType: source.contentType,
      sourceId,
      trimIds,
      editResult,
      exports: { xlsx, pdf },
      cleanup: observed.cleanup,
      observed,
      passed: (
        Boolean(sourceId)
        && trimIds.length >= 2
        && editResult.savedAsExpected
        && xlsx.status === 200
        && xlsx.signatureOk
        && xlsx.editedValueInPayload
        && String(xlsx.contentType).includes("spreadsheetml.sheet")
        && pdf.status === 200
        && pdf.signatureOk
        && pdf.editedValueInPayload
        && String(pdf.contentType).includes("application/pdf")
        && (!observed.cleanup || observed.cleanup.errors.length === 0)
      ),
    };
    const summaryPath = path.join(artifactDir, "product_config_edit_export_smoke.json");
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
    const summaryPath = path.join(artifactDir, "product_config_edit_export_smoke.json");
    await writeFile(summaryPath, `${JSON.stringify({
      createdAt: new Date().toISOString(),
      artifactDir,
      failureScreenshot,
      sourcePath,
      sourceTextPath,
      fileName: source.fileName,
      sourceFormat: options.sourceFormat,
      contentType: source.contentType,
      sourceId,
      trimIds,
      cleanup: observed.cleanup,
      observed,
      error: error instanceof Error ? error.message : String(error),
      passed: false,
    }, null, 2)}\n`, "utf8");
    console.error(`Product config edit-export smoke failed. Summary: ${summaryPath}`);
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
