import type {
  CountryChatGroundingLayer,
  CountryChatMetadataResponse,
  CountryChatModelOption,
} from "../types/countryChat";

export interface CountryChatLoadingPlan {
  label: string;
  steps: string[];
}

export type CountryChatDeckLens = "all" | "workbench" | "market" | "trend" | "intelligence";

export interface CountryChatDeckScope {
  defaultLens: CountryChatDeckLens;
  visibleLenses: CountryChatDeckLens[];
}

export interface CountryChatHandoffPayload {
  country: string;
  chatModel: string;
  question: string;
}

export interface CountryChatAnswerPath {
  routeLabel: string;
  outputLabel: string;
  focusTags: string[];
  steps: string[];
}

export interface CountryChatAnswerSections {
  lead: string;
  detailParagraphs: string[];
  reasoningNotes: string[];
}

export function getCountryChatHandoffSearch(
  pathname: string,
  search: string,
): string {
  if (String(pathname).trim() !== "/copilot") {
    return "";
  }
  return String(search ?? "").trim();
}

export function isCountryChatMobileAccess(
  viewportWidth: number,
  hasCoarsePointer: boolean,
  maxWidth = 720,
): boolean {
  return hasCoarsePointer && viewportWidth <= maxWidth;
}

function includesAny(text: string, keywords: string[]): boolean {
  return keywords.some((keyword) => text.includes(keyword));
}

export function buildCountryChatLoadingPlan(question: string): CountryChatLoadingPlan {
  const normalized = question.trim().toLowerCase();
  if (!normalized) {
    return {
      label: "正在准备回答",
      steps: ["判断问题意图", "准备证据上下文", "调用模型生成回答", "必要时用证据兜底"],
    };
  }

  if (
    /(?:车长|长度|length)\s*\d{3,5}|\d{3,5}\s*(?:mm|毫米|的车|长)/i.test(question)
    || includesAny(normalized, ["价格", "售价", "定价", "msrp", "price"])
  ) {
    return {
      label: "正在做定位分析",
      steps: [
        "解析车长/价格条件",
        "匹配同尺寸车型与 segment",
        "汇总该 segment 的燃料、渠道和驱动结构",
        "整理价格与竞品结论",
      ],
    };
  }

  if (includesAny(normalized, ["为什么卖得好", "为什么卖的好", "为什么好卖", "为何卖得好", "为何卖的好", "why sells well", "why sell well"])) {
    return {
      label: "正在做车型胜因分析",
      steps: [
        "锁定车型与细分页 scope",
        "读取 Market Scan 排名与份额",
        "补齐渠道 / 驱动 / 版本结构",
        "关联最新市场信号并生成结论",
      ],
    };
  }

  if (includesAny(normalized, ["政策", "补贴", "关税", "法规", "新闻", "热点", "tariff", "policy", "news"])) {
    return {
      label: "正在做市场情报分析",
      steps: [
        "提取国家 / 品牌 / 车型线索",
        "检索新闻快照与本地 profile",
        "调用模型生成针对性回答",
        "模型超时时用证据摘要兜底",
      ],
    };
  }

  if (includesAny(normalized, ["bev", "phev", "hev", "纯电", "插混", "混动", "动力", "fuel"])) {
    return {
      label: "正在做动力结构分析",
      steps: [
        "识别动力类型条件",
        "准备国家快照上下文",
        "补齐燃料排名与结构数据",
        "调用模型生成回答",
      ],
    };
  }

  return {
    label: "正在组织国家回答",
    steps: [
      "判断问题意图",
      "准备国家快照与图表证据",
      "调用模型生成回答",
      "模型超时时用证据摘要兜底",
    ],
  };
}

function countryChatRouteLabel(intentRoute: string | null | undefined): string {
  switch ((intentRoute ?? "").trim()) {
    case "precise-lookup":
      return "精准查询";
    case "positioning-focus":
      return "定位分析";
    case "segment-fuel-focus":
      return "细分动力";
    case "market-scan-scope":
      return "细分页面";
    case "market-context":
      return "市场情报";
    default:
      return "国家回答";
  }
}

function countryChatRouteStep(intentRoute: string | null | undefined): string {
  switch ((intentRoute ?? "").trim()) {
    case "precise-lookup":
      return "锁定具体车型 / trim / 价格条件";
    case "positioning-focus":
      return "锁定长度 / 价格 / 竞品定位范围";
    case "segment-fuel-focus":
      return "锁定 segment × fuel 范围";
    case "market-scan-scope":
      return "锁定榜单、细分页面与对比范围";
    case "market-context":
      return "锁定政策 / 新闻 / 市场事件范围";
    default:
      return "识别问题并确定国家回答范围";
  }
}

function countryChatOutputLabel(answerMode: string | null | undefined): string {
  switch (answerMode) {
    case "grounded-direct":
      return "直接组装";
    case "grounded-model":
      return "模型润色";
    case "grounded-fallback":
      return "证据兜底";
    default:
      return "综合回答";
  }
}

function normalizeAnswerSectionText(value: string | null | undefined): string {
  return String(value ?? "").replace(/\r\n/g, "\n").trim();
}

function normalizeAnswerSectionKey(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function splitCountryChatAnswerParagraphs(content: string): string[] {
  const paragraphBlocks = content
    .split(/\n{2,}/)
    .map((block) => block
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .join("\n"))
    .filter(Boolean);
  if (paragraphBlocks.length !== 1 || paragraphBlocks[0].includes("\n")) {
    return paragraphBlocks;
  }
  const sentenceBlocks = paragraphBlocks[0]
    .match(/[^。！？!?]+[。！？!?]?/g)
    ?.map((item) => item.trim())
    .filter(Boolean)
    ?? [];
  if (sentenceBlocks.length > 1) {
    return sentenceBlocks;
  }
  return paragraphBlocks;
}

export function buildCountryChatAnswerSections({
  content,
  summary,
  reasoningNotes,
}: {
  content?: string | null;
  summary?: string | null;
  reasoningNotes?: string[] | null;
}): CountryChatAnswerSections {
  const normalizedContent = normalizeAnswerSectionText(content);
  const contentBlocks = normalizedContent
    ? splitCountryChatAnswerParagraphs(normalizedContent)
    : [];
  const lead = contentBlocks[0] ?? "";
  const detailParagraphs = contentBlocks.slice(1);
  const normalizedSummary = normalizeAnswerSectionText(summary);

  if (!lead && normalizedSummary) {
    return {
      lead: normalizedSummary,
      detailParagraphs: [],
      reasoningNotes: (reasoningNotes ?? []).map(normalizeAnswerSectionText).filter(Boolean),
    };
  }

  const answerReasoningNotes: string[] = [];
  const seen = new Set<string>(lead ? [normalizeAnswerSectionKey(lead)] : []);
  const explicitReasoningNotes = Array.isArray(reasoningNotes)
    ? reasoningNotes.map(normalizeAnswerSectionText).filter(Boolean)
    : [];
  const reasoningCandidates = explicitReasoningNotes.length > 0
    ? [...explicitReasoningNotes, normalizedSummary]
    : [normalizedSummary, ...detailParagraphs];
  for (const candidate of reasoningCandidates) {
    if (!candidate) {
      continue;
    }
    const normalizedKey = normalizeAnswerSectionKey(candidate);
    if (!normalizedKey || seen.has(normalizedKey)) {
      continue;
    }
    seen.add(normalizedKey);
    answerReasoningNotes.push(candidate);
  }

  return {
    lead,
    detailParagraphs,
    reasoningNotes: answerReasoningNotes,
  };
}

function normalizeAnswerPathTag(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (Array.isArray(value)) {
    return normalizeAnswerPathTag(value[0]);
  }
  if (value && typeof value === "object") {
    for (const key of ["label", "value", "name", "model", "trim", "version"]) {
      if (key in value) {
        const nested = normalizeAnswerPathTag((value as Record<string, unknown>)[key]);
        if (nested) {
          return nested;
        }
      }
    }
  }
  return "";
}

function countryChatReasoningClueStep(
  intentRoute: string | null | undefined,
  extractedParams: Record<string, unknown>,
): string {
  const msrp = normalizeAnswerPathTag(extractedParams.msrp);
  const length = normalizeAnswerPathTag(extractedParams.length);
  const model = normalizeAnswerPathTag(
    extractedParams.subjectModel ?? extractedParams.targetModel ?? extractedParams.model,
  );
  const compareModel = normalizeAnswerPathTag(extractedParams.compareModel);
  const segment = normalizeAnswerPathTag(extractedParams.segment);
  const fuelType = normalizeAnswerPathTag(extractedParams.fuelType ?? extractedParams.fuel_type ?? extractedParams.powertrain);

  switch ((intentRoute ?? "").trim()) {
    case "precise-lookup":
      if (model && msrp) {
        return `把 ${model} 与 ${msrp} 价格线索绑定到具体版型查询。`;
      }
      if (model && compareModel) {
        return `把 ${model} 与 ${compareModel} 绑定到同一对比问题。`;
      }
      break;
    case "positioning-focus":
      if (length && msrp) {
        return `把 ${length} 车长与 ${msrp} 价格线索绑定到同尺寸竞品带。`;
      }
      if (length) {
        return `把 ${length} 车长先映射到邻近 segment 与竞品带。`;
      }
      break;
    case "segment-fuel-focus":
      if (segment && fuelType) {
        return `把 ${segment} 与 ${fuelType} 绑定到对应动力排名。`;
      }
      break;
    case "market-scan-scope":
      if (model) {
        return `把 ${model} 绑定到对应榜单页与销量结构。`;
      }
      break;
    case "market-context":
      if (segment) {
        return `把 ${segment} 相关政策 / 新闻线索绑定到当前市场背景。`;
      }
      break;
    default:
      break;
  }
  return "";
}

export function buildCountryChatAnswerPath({
  intentRoute,
  answerMode,
  layers,
  extractedParams,
  country,
}: {
  intentRoute?: string | null;
  answerMode?: string | null;
  layers?: CountryChatGroundingLayer[];
  extractedParams?: Record<string, unknown> | null;
  country?: string | null;
}): CountryChatAnswerPath {
  const layerLabels = Array.isArray(layers)
    ? layers.map((layer) => String(layer.label ?? "").trim()).filter(Boolean)
    : [];
  const params = extractedParams && typeof extractedParams === "object"
    ? extractedParams
    : {};
  const focusKeys = [
    "brand",
    "model",
    "subjectModel",
    "targetModel",
    "compareModel",
    "trim",
    "version",
    "powertrain",
    "fuelType",
    "fuel_type",
    "segment",
  ];
  const focusTags: string[] = [];

  for (const key of focusKeys) {
    const normalized = normalizeAnswerPathTag(params[key]);
    if (normalized && !focusTags.includes(normalized)) {
      focusTags.push(normalized);
    }
  }
  if (country) {
    const normalizedCountry = normalizeAnswerPathTag(country);
    if (normalizedCountry && !focusTags.includes(normalizedCountry)) {
      focusTags.unshift(normalizedCountry);
    }
  }

  const routeLabel = countryChatRouteLabel(intentRoute);
  const outputLabel = countryChatOutputLabel(answerMode);
  const layerSummary = layerLabels.length > 0
    ? layerLabels.slice(0, 3).join(" / ")
    : "国家快照与已命中证据";
  const reasoningClueStep = countryChatReasoningClueStep(intentRoute, params);
  const steps = [
    countryChatRouteStep(intentRoute),
    reasoningClueStep,
    `读取 ${layerSummary}`,
    `按${outputLabel}方式生成答案`,
  ].filter(Boolean);

  return {
    routeLabel,
    outputLabel,
    focusTags: focusTags.slice(0, 4),
    steps,
  };
}

export function resolveCountryChatDeckScope(intentRoute: string | null | undefined): CountryChatDeckScope {
  switch ((intentRoute ?? "").trim()) {
    case "precise-lookup":
      return {
        defaultLens: "workbench",
        visibleLenses: ["workbench", "intelligence"],
      };
    case "positioning-focus":
      return {
        defaultLens: "workbench",
        visibleLenses: ["workbench", "market", "intelligence"],
      };
    case "segment-fuel-focus":
      return {
        defaultLens: "market",
        visibleLenses: ["market", "intelligence"],
      };
    case "market-scan-scope":
      return {
        defaultLens: "market",
        visibleLenses: ["market", "workbench", "trend", "intelligence"],
      };
    case "market-context":
      return {
        defaultLens: "intelligence",
        visibleLenses: ["intelligence", "trend"],
      };
    default:
      return {
        defaultLens: "all",
        visibleLenses: ["all", "workbench", "market", "intelligence", "trend"],
      };
  }
}

export function isKnownCountryValue(
  metadata: CountryChatMetadataResponse | null,
  country: string,
): boolean {
  const countries = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];
  if (countries.length === 0 || !country) {
    return false;
  }
  return countries.some((item) => item.value === country);
}

export function resolveCountrySelection({
  metadata,
  preferredCountry,
  selectedCountry,
  userPicked,
}: {
  metadata: CountryChatMetadataResponse | null;
  preferredCountry: string;
  selectedCountry: string;
  userPicked: boolean;
}): string {
  const availableCountries = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];

  if (selectedCountry && isKnownCountryValue(metadata, selectedCountry)) {
    return selectedCountry;
  }
  if (!userPicked && preferredCountry && isKnownCountryValue(metadata, preferredCountry)) {
    return preferredCountry;
  }
  return availableCountries[0]?.value ?? "";
}

export function availableChatModels(
  metadata: CountryChatMetadataResponse | null,
): CountryChatModelOption[] {
  return Array.isArray(metadata?.availableChatModels)
    ? metadata.availableChatModels.filter((item) => item?.available !== false)
    : [];
}

export function isKnownChatModelValue(
  metadata: CountryChatMetadataResponse | null,
  chatModel: string,
): boolean {
  const models = availableChatModels(metadata);
  if (models.length === 0 || !chatModel) {
    return false;
  }
  return models.some((item) => item.id === chatModel);
}

export function resolveChatModelSelection({
  metadata,
  selectedChatModel,
}: {
  metadata: CountryChatMetadataResponse | null;
  selectedChatModel: string;
}): string {
  if (selectedChatModel && isKnownChatModelValue(metadata, selectedChatModel)) {
    return selectedChatModel;
  }
  const preferred = String(metadata?.defaultChatModel ?? "").trim();
  if (preferred && isKnownChatModelValue(metadata, preferred)) {
    return preferred;
  }
  return availableChatModels(metadata)[0]?.id ?? "";
}

export function buildCountryChatSessionKey(
  country: string,
  chatModel: string,
): string {
  return `${country}::${chatModel || "auto"}`;
}

export function getChatModelLabel(
  metadata: CountryChatMetadataResponse | null,
  chatModel: string,
): string {
  const match = availableChatModels(metadata).find((item) => item.id === chatModel);
  return match?.label ?? chatModel;
}

export function buildCountryChatHandoffSearch(payload: Partial<CountryChatHandoffPayload>): string {
  const params = new URLSearchParams();
  const country = String(payload.country ?? "").trim();
  const chatModel = String(payload.chatModel ?? "").trim();
  const question = String(payload.question ?? "").trim();

  if (country) {
    params.set("cc_country", country);
  }
  if (chatModel) {
    params.set("cc_model", chatModel);
  }
  if (question) {
    params.set("cc_q", question);
  }

  const search = params.toString();
  return search ? `?${search}` : "";
}

export function parseCountryChatHandoffSearch(search: string): CountryChatHandoffPayload {
  const params = new URLSearchParams(search);
  return {
    country: String(params.get("cc_country") ?? "").trim(),
    chatModel: String(params.get("cc_model") ?? "").trim(),
    question: String(params.get("cc_q") ?? "").trim(),
  };
}

/**
 * Security note:
 * renderMarkdown intentionally escapes raw HTML BEFORE applying markdown
 * replacements. This prevents LLM-generated HTML such as <script> or
 * <img onerror=...> from becoming executable. Do not move escapeHtml after
 * markdown rendering unless a trusted sanitizer such as DOMPurify is added.
 *
 * Supported syntax: h1-h4, bold, italic, inline code, hr, unordered/ordered
 * lists, markdown tables (GFM-style), line breaks.
 */

function escapeHtml(input: string): string {
  return input
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function isTableRow(line: string): boolean {
  return /^\s*\|.+\|\s*$/.test(line);
}

function isTableSeparator(line: string): boolean {
  return /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(line);
}

function splitTableRow(line: string): string[] {
  return line
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((cell) => cell.trim());
}

function renderTableBlock(lines: string[]): string {
  const header = splitTableRow(lines[0]);
  const bodyRows = lines.slice(2).map(splitTableRow);
  const thead = `<thead><tr>${header.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${bodyRows
    .map((row) => `<tr>${row.map((c) => `<td>${c}</td>`).join("")}</tr>`)
    .join("")}</tbody>`;
  return `<div class="markdown-table-wrap"><table class="markdown-table">${thead}${tbody}</table></div>`;
}

function renderTablesInMarkdown(md: string): string {
  const lines = md.split("\n");
  const output: string[] = [];
  let i = 0;
  while (i < lines.length) {
    const current = lines[i];
    const next = i + 1 < lines.length ? lines[i + 1] : "";
    if (isTableRow(current) && isTableSeparator(next)) {
      const tableLines = [current, next];
      i += 2;
      while (i < lines.length && isTableRow(lines[i])) {
        tableLines.push(lines[i]);
        i += 1;
      }
      output.push(renderTableBlock(tableLines));
      continue;
    }
    output.push(current);
    i += 1;
  }
  return output.join("\n");
}

export function renderMarkdown(md: string): string {
  // Security-critical: escape HTML first, then generate only controlled tags
  let html = escapeHtml(md);
  html = renderTablesInMarkdown(html);
  html = html
    .replace(/^#### (.+)$/gm, "<h4>$1</h4>")
    .replace(/^### (.+)$/gm, "<h3>$1</h3>")
    .replace(/^## (.+)$/gm, "<h2>$1</h2>")
    .replace(/^# (.+)$/gm, "<h1>$1</h1>")
    .replace(/\*\*\*(.+?)\*\*\*/g, "<strong><em>$1</em></strong>")
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.+?)\*/g, "<em>$1</em>")
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/^---$/gm, "<hr>")
    .replace(/^[\s]*[-*+] (.+)$/gm, "<li>$1</li>")
    .replace(/^[\s]*\d+\. (.+)$/gm, "<li>$1</li>")
    .replace(/\n\n/g, "</p><p>")
    .replace(/\n/g, "<br>");
  html = html.replace(/(<li>.*?<\/li>(\s*<br>\s*)?)+/g, "<ul>$&</ul>");
  return `<p>${html}</p>`;
}
