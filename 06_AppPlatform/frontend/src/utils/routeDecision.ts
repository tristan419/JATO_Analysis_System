export type RouteTarget = "cn" | "intl";
export type RouteDecisionSource = "manual" | "auto";
export type ProbeStatus = "idle" | "running" | "ok" | "failed";

export interface RouteDecision {
  target: RouteTarget;
  expiresAt: number;
  createdAt?: number;
  source?: RouteDecisionSource;
  reason?: string;
  cnOk?: boolean;
  intlOk?: boolean;
  cnMs?: number;
  intlMs?: number;
  marginMs?: number;
}

export interface ProbeResult {
  target: RouteTarget;
  host: string;
  status: ProbeStatus;
  ms: number | null;
  checkedAt: string;
}

export interface ClientRouteProfile {
  timeZone: string;
  languages: string[];
  prefersChinaRoute: boolean;
  reason: string;
}

interface ClientRouteProfileInput {
  timeZone?: string;
  language?: string;
  languages?: readonly string[];
}

interface RouteLocationLike {
  hostname?: string;
  pathname: string;
  search: string;
  hash: string;
}

const TRANSFER_TARGET_PARAM = "jatoRouteTarget";
const TRANSFER_EXPIRES_PARAM = "jatoRouteExpires";
const TRANSFER_CREATED_PARAM = "jatoRouteCreated";
const TRANSFER_SOURCE_PARAM = "jatoRouteSource";
const TRANSFER_REASON_PARAM = "jatoRouteReason";
const TRANSFER_CN_OK_PARAM = "jatoRouteCnOk";
const TRANSFER_INTL_OK_PARAM = "jatoRouteIntlOk";
const TRANSFER_CN_MS_PARAM = "jatoRouteCnMs";
const TRANSFER_INTL_MS_PARAM = "jatoRouteIntlMs";
const TRANSFER_MARGIN_MS_PARAM = "jatoRouteMarginMs";

const TRANSFER_PARAMS = [
  TRANSFER_TARGET_PARAM,
  TRANSFER_EXPIRES_PARAM,
  TRANSFER_CREATED_PARAM,
  TRANSFER_SOURCE_PARAM,
  TRANSFER_REASON_PARAM,
  TRANSFER_CN_OK_PARAM,
  TRANSFER_INTL_OK_PARAM,
  TRANSFER_CN_MS_PARAM,
  TRANSFER_INTL_MS_PARAM,
  TRANSFER_MARGIN_MS_PARAM,
];

export const ROUTE_HOSTS: Record<RouteTarget, string> = {
  cn: "www.ojeur.cloud",
  intl: "intl.ojeur.cloud",
};
export const DECISION_KEY = "jato_route_decision_v2";
const LEGACY_DECISION_KEYS = ["jato_route_decision_v1"];
export const MANUAL_KEY = "jato_route_manual_v1";
export const PROBE_INFLIGHT_KEY = "jato_route_probe_inflight_v1";
export const PROBE_TIMEOUT_MS = 1_800;
export const REDIRECT_MARGIN_MS = 450;
export const AUTO_DECISION_TTL_MS = 2 * 60 * 60 * 1000;
export const MANUAL_DECISION_TTL_MS = 24 * 60 * 60 * 1000;
export const PROBE_INFLIGHT_TTL_MS = PROBE_TIMEOUT_MS + 700;
const CHINA_LOCAL_TIME_ZONES = new Set([
  "Asia/Shanghai",
  "Asia/Chongqing",
  "Asia/Harbin",
  "Asia/Urumqi",
]);

function isMainlandChineseLanguage(language: string): boolean {
  const normalized = language.toLowerCase();
  return normalized === "zh-cn"
    || normalized.startsWith("zh-cn-")
    || normalized === "zh-hans"
    || normalized.startsWith("zh-hans-");
}

function normalizeLanguages(input: ClientRouteProfileInput): string[] {
  const values = [
    ...(input.languages ?? []),
    input.language ?? "",
  ];
  const seen = new Set<string>();
  const languages: string[] = [];
  values.forEach((value) => {
    const normalized = value.trim();
    if (!normalized || seen.has(normalized.toLowerCase())) return;
    seen.add(normalized.toLowerCase());
    languages.push(normalized);
  });
  return languages;
}

export function createClientRouteProfile(input: ClientRouteProfileInput): ClientRouteProfile {
  const timeZone = input.timeZone?.trim() ?? "";
  const languages = normalizeLanguages(input);
  const chinaTimeZone = CHINA_LOCAL_TIME_ZONES.has(timeZone);
  const mainlandChineseLanguage = languages.some(isMainlandChineseLanguage);
  const signals: string[] = [];
  if (chinaTimeZone) signals.push(`time zone ${timeZone}`);
  if (mainlandChineseLanguage) signals.push(`language ${languages.join(", ")}`);
  return {
    timeZone,
    languages,
    prefersChinaRoute: chinaTimeZone || mainlandChineseLanguage,
    reason: signals.length
      ? `China-local browser signal: ${signals.join("; ")}`
      : "No China-local browser signal",
  };
}

export function detectClientRouteProfile(): ClientRouteProfile {
  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone ?? "";
  return createClientRouteProfile({
    timeZone,
    language: navigator.language,
    languages: navigator.languages,
  });
}

export function routeLabel(target: RouteTarget): string {
  return target === "cn" ? "www" : "intl";
}

export function formatTarget(target: RouteTarget | null): string {
  if (!target) return "-";
  return ROUTE_HOSTS[target];
}

export function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export function booleanOrUndefined(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function parseFiniteNumber(value: string | null): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseBoolean(value: string | null): boolean | undefined {
  if (value === "true") return true;
  if (value === "false") return false;
  return undefined;
}

function parseTarget(value: unknown): RouteTarget | null {
  return value === "cn" || value === "intl" ? value : null;
}

function parseSource(value: unknown): RouteDecisionSource | undefined {
  return value === "manual" || value === "auto" ? value : undefined;
}

function storageKeyForDecision(decision: RouteDecision): string {
  return decision.source === "manual" ? MANUAL_KEY : DECISION_KEY;
}

function cleanPath(location: RouteLocationLike, params: URLSearchParams): string {
  const query = params.toString();
  return `${location.pathname || "/"}${query ? `?${query}` : ""}${location.hash || ""}`;
}

export function currentRouteTarget(host: string): RouteTarget | null {
  if (host === ROUTE_HOSTS.cn || host === "ojeur.cloud") return "cn";
  if (host === ROUTE_HOSTS.intl) return "intl";
  return null;
}

export function readRouteDecision(
  storage: Storage,
  key: string,
  now = Date.now(),
): RouteDecision | null {
  try {
    const raw = storage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<RouteDecision>;
    const target = parseTarget(parsed.target);
    if (!target) return null;
    if (typeof parsed.expiresAt !== "number" || parsed.expiresAt < now) {
      storage.removeItem(key);
      return null;
    }
    return {
      target,
      expiresAt: parsed.expiresAt,
      createdAt: numberOrUndefined(parsed.createdAt),
      source: parseSource(parsed.source),
      reason: typeof parsed.reason === "string" ? parsed.reason : undefined,
      cnOk: booleanOrUndefined(parsed.cnOk),
      intlOk: booleanOrUndefined(parsed.intlOk),
      cnMs: numberOrUndefined(parsed.cnMs),
      intlMs: numberOrUndefined(parsed.intlMs),
      marginMs: numberOrUndefined(parsed.marginMs),
    };
  } catch {
    return null;
  }
}

export function saveRouteDecision(storage: Storage, decision: RouteDecision): void {
  storage.setItem(storageKeyForDecision(decision), JSON.stringify(decision));
}

export function clearRouteDecisions(storage: Storage): void {
  storage.removeItem(MANUAL_KEY);
  storage.removeItem(DECISION_KEY);
  LEGACY_DECISION_KEYS.forEach((key) => storage.removeItem(key));
}

export function isRouteProbeInFlight(storage: Storage, now = Date.now()): boolean {
  const raw = storage.getItem(PROBE_INFLIGHT_KEY);
  if (!raw) return false;
  const startedAt = Number(raw);
  if (!Number.isFinite(startedAt) || now - startedAt > PROBE_INFLIGHT_TTL_MS) {
    storage.removeItem(PROBE_INFLIGHT_KEY);
    return false;
  }
  return true;
}

export function consumeRouteDecisionTransfer(
  location: RouteLocationLike,
  storage: Storage,
  now = Date.now(),
): { decision: RouteDecision | null; cleanPath: string | null } {
  const params = new URLSearchParams(location.search);
  const hadTransferParams = TRANSFER_PARAMS.some((key) => params.has(key));
  if (!hadTransferParams) {
    return { decision: null, cleanPath: null };
  }

  const target = parseTarget(params.get(TRANSFER_TARGET_PARAM));
  const expiresAt = parseFiniteNumber(params.get(TRANSFER_EXPIRES_PARAM));
  const source = parseSource(params.get(TRANSFER_SOURCE_PARAM)) ?? "auto";
  let decision: RouteDecision | null = null;

  if (target && expiresAt !== undefined && expiresAt >= now) {
    decision = {
      target,
      expiresAt,
      createdAt: parseFiniteNumber(params.get(TRANSFER_CREATED_PARAM)),
      source,
      reason: params.get(TRANSFER_REASON_PARAM) || undefined,
      cnOk: parseBoolean(params.get(TRANSFER_CN_OK_PARAM)),
      intlOk: parseBoolean(params.get(TRANSFER_INTL_OK_PARAM)),
      cnMs: parseFiniteNumber(params.get(TRANSFER_CN_MS_PARAM)),
      intlMs: parseFiniteNumber(params.get(TRANSFER_INTL_MS_PARAM)),
      marginMs: parseFiniteNumber(params.get(TRANSFER_MARGIN_MS_PARAM)),
    };
    saveRouteDecision(storage, decision);
  }

  TRANSFER_PARAMS.forEach((key) => params.delete(key));
  return {
    decision,
    cleanPath: cleanPath(location, params),
  };
}

export function shouldSkipSmartRoute(location: RouteLocationLike): boolean {
  if (!location.hostname || !currentRouteTarget(location.hostname)) return true;
  if (location.pathname === "/route-diagnostics") return true;
  const params = new URLSearchParams(location.search);
  return params.has("token") || params.has("code") || params.has("state");
}

export function makeInitialProbe(target: RouteTarget): ProbeResult {
  return {
    target,
    host: ROUTE_HOSTS[target],
    status: "idle",
    ms: null,
    checkedAt: "-",
  };
}

export async function probeRoute(target: RouteTarget): Promise<ProbeResult> {
  const controller = new AbortController();
  const startedAt = performance.now();
  const timeout = window.setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
  const checkedAt = new Date().toLocaleTimeString();
  try {
    await fetch(`https://${ROUTE_HOSTS[target]}/route-probe.txt?ts=${Date.now()}`, {
      cache: "no-store",
      credentials: "omit",
      mode: "no-cors",
      signal: controller.signal,
    });
    return {
      target,
      host: ROUTE_HOSTS[target],
      status: "ok",
      ms: Math.round(performance.now() - startedAt),
      checkedAt,
    };
  } catch {
    return {
      target,
      host: ROUTE_HOSTS[target],
      status: "failed",
      ms: Math.round(performance.now() - startedAt),
      checkedAt,
    };
  } finally {
    window.clearTimeout(timeout);
  }
}

export function chooseAutoRoute(
  results: Record<RouteTarget, ProbeResult>,
  currentTarget: RouteTarget | null,
  profile?: ClientRouteProfile | null,
): { target: RouteTarget; reason: string } | null {
  const cnOk = results.cn.status === "ok";
  const intlOk = results.intl.status === "ok";
  const marginMs = REDIRECT_MARGIN_MS;
  if (!cnOk && !intlOk) {
    if (results.cn.status === "running" || results.intl.status === "running") return null;
    return {
      target: currentTarget ?? "cn",
      reason: "Both probes failed; stay on the current host.",
    };
  }
  if (cnOk && !intlOk) {
    return {
      target: "cn",
      reason: "intl probe failed and www probe succeeded.",
    };
  }
  if (intlOk && !cnOk) {
    return {
      target: "intl",
      reason: "www probe failed and intl probe succeeded.",
    };
  }
  const cnMs = results.cn.ms ?? PROBE_TIMEOUT_MS;
  const intlMs = results.intl.ms ?? PROBE_TIMEOUT_MS;
  const deltaMs = Math.abs(cnMs - intlMs);
  if (deltaMs <= marginMs) {
    if (profile?.prefersChinaRoute) {
      return {
        target: "cn",
        reason: `www is preferred for China-local browser signals because both probes are within ${marginMs} ms (${profile.reason}).`,
      };
    }
    return {
      target: currentTarget ?? "cn",
      reason: `Both probes are within ${marginMs} ms; keep ${routeLabel(currentTarget ?? "cn")} to avoid route churn.`,
    };
  }
  if (intlMs < cnMs) {
    return {
      target: "intl",
      reason: `intl is faster by ${cnMs - intlMs} ms, above the ${marginMs} ms measured-route margin.`,
    };
  }
  return {
    target: "cn",
    reason: `www is faster by ${intlMs - cnMs} ms, above the ${marginMs} ms measured-route margin.`,
  };
}

export function createAutoRouteDecision(
  results: Record<RouteTarget, ProbeResult>,
  currentTarget: RouteTarget | null,
  profile?: ClientRouteProfile | null,
  now = Date.now(),
): RouteDecision | null {
  const recommendation = chooseAutoRoute(results, currentTarget, profile);
  if (!recommendation) return null;
  return {
    target: recommendation.target,
    source: "auto",
    reason: recommendation.reason,
    createdAt: now,
    expiresAt: now + AUTO_DECISION_TTL_MS,
    cnOk: results.cn.status === "ok",
    intlOk: results.intl.status === "ok",
    cnMs: results.cn.ms ?? undefined,
    intlMs: results.intl.ms ?? undefined,
    marginMs: REDIRECT_MARGIN_MS,
  };
}

export function createManualRouteDecision(
  target: RouteTarget,
  now = Date.now(),
): RouteDecision {
  return {
    target,
    source: "manual",
    reason: "Manual override from route diagnostics",
    createdAt: now,
    expiresAt: now + MANUAL_DECISION_TTL_MS,
  };
}

export function buildRouteRedirectUrl(
  decision: RouteDecision,
  location: RouteLocationLike,
): string {
  const url = new URL(
    `${location.pathname || "/"}${location.search || ""}${location.hash || ""}`,
    `https://${ROUTE_HOSTS[decision.target]}`,
  );
  url.searchParams.set(TRANSFER_TARGET_PARAM, decision.target);
  url.searchParams.set(TRANSFER_EXPIRES_PARAM, String(decision.expiresAt));
  if (decision.createdAt !== undefined) {
    url.searchParams.set(TRANSFER_CREATED_PARAM, String(decision.createdAt));
  }
  if (decision.source) {
    url.searchParams.set(TRANSFER_SOURCE_PARAM, decision.source);
  }
  if (decision.reason) {
    url.searchParams.set(TRANSFER_REASON_PARAM, decision.reason);
  }
  if (decision.cnOk !== undefined) {
    url.searchParams.set(TRANSFER_CN_OK_PARAM, String(decision.cnOk));
  }
  if (decision.intlOk !== undefined) {
    url.searchParams.set(TRANSFER_INTL_OK_PARAM, String(decision.intlOk));
  }
  if (decision.cnMs !== undefined) {
    url.searchParams.set(TRANSFER_CN_MS_PARAM, String(decision.cnMs));
  }
  if (decision.intlMs !== undefined) {
    url.searchParams.set(TRANSFER_INTL_MS_PARAM, String(decision.intlMs));
  }
  if (decision.marginMs !== undefined) {
    url.searchParams.set(TRANSFER_MARGIN_MS_PARAM, String(decision.marginMs));
  }
  return url.toString();
}
