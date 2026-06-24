#!/usr/bin/env node

const DEFAULT_ORIGIN = "https://intl.ojeur.cloud";
const DEFAULT_ORIGIN_API_BASE = "/v1";
const DEFAULT_TIMEOUT_MS = 20_000;
const DEFAULT_REPETITIONS = 2;
const DEFAULT_ROLE = "viewer";
const DEFAULT_ROLES = ["viewer", "order_filler", "editor", "admin"];
const DEFAULT_USER = "edge-prewarm";
const DEFAULT_POWERTRAINS = ["ICE", "HEV", "BEV", "MHEV", "PHEV"];
const FALLBACK_COUNTRIES = [
  "丹麦",
  "克罗地亚",
  "匈牙利",
  "奥地利",
  "希腊",
  "德国",
  "意大利",
  "挪威",
  "捷克",
  "斯洛伐克",
  "斯洛文尼亚",
  "比利时",
  "法国",
  "波兰",
  "瑞典",
  "瑞士",
  "罗马尼亚",
  "芬兰",
  "荷兰",
  "葡萄牙",
  "西班牙",
];
const FILTER_COLUMN_ALIASES = {
  body_type: ["Body type", "Body Type", "body type", "车身形式"],
  country: ["国家", "Country", "country"],
  make: ["Make", "品牌", "make"],
  model: ["Model", "model"],
  powertrain: ["动总规整", "powertrain", "Powertrain"],
  segment: ["细分市场（按车长）", "细分市场", "segment"],
  version: ["Version name", "version name", "Version Name"],
};
const FILTER_ORDER_KEYS = [
  "country",
  "body_type",
  "segment",
  "powertrain",
  "make",
  "model",
  "version",
];

function getArg(name) {
  const prefix = `--${name}=`;
  const match = process.argv.slice(2).find((item) => item.startsWith(prefix));
  return match ? match.slice(prefix.length) : "";
}

function hasFlag(name) {
  return process.argv.slice(2).includes(`--${name}`);
}

function parseList(raw, fallback) {
  const value = String(raw || "").trim();
  if (!value) return fallback;
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function uniqueList(values) {
  return [...new Set(values.map((item) => String(item || "").trim()).filter(Boolean))];
}

function resolveWarmupRoles({ configuredRoles, explicitRole, loginRole, token }) {
  const roles = uniqueList(configuredRoles || []);
  if (roles.length > 0) return roles;
  if (explicitRole) return [explicitRole];
  if (token && loginRole) return [loginRole];
  return DEFAULT_ROLES;
}

function parseInteger(raw, fallback) {
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeOrigin(raw) {
  const url = new URL(raw || DEFAULT_ORIGIN);
  return url.origin;
}

function joinApiPath(origin, path) {
  const cleanPath = path.startsWith("/") ? path : `/${path}`;
  return new URL(`${DEFAULT_ORIGIN_API_BASE}${cleanPath}`, origin).toString();
}

function abortSignal(timeoutMs) {
  if (typeof AbortSignal.timeout === "function") {
    return AbortSignal.timeout(timeoutMs);
  }
  const controller = new AbortController();
  setTimeout(() => controller.abort(), timeoutMs).unref?.();
  return controller.signal;
}

function resolveColumn(columns, aliases) {
  const normalized = new Map(
    columns.map((column) => [String(column).trim().toLowerCase(), column]),
  );
  for (const alias of aliases) {
    const hit = normalized.get(alias.toLowerCase());
    if (hit) return hit;
  }
  return "";
}

function resolveColumns(snapshot) {
  const columns = Array.isArray(snapshot?.columns) ? snapshot.columns : [];
  return {
    body_type: resolveColumn(columns, FILTER_COLUMN_ALIASES.body_type),
    country: resolveColumn(columns, FILTER_COLUMN_ALIASES.country),
    make: resolveColumn(columns, FILTER_COLUMN_ALIASES.make),
    model: resolveColumn(columns, FILTER_COLUMN_ALIASES.model),
    powertrain: resolveColumn(columns, FILTER_COLUMN_ALIASES.powertrain),
    segment: resolveColumn(columns, FILTER_COLUMN_ALIASES.segment),
    version: resolveColumn(columns, FILTER_COLUMN_ALIASES.version),
  };
}

function valuesFromSnapshot(snapshot, column, fallback) {
  if (!column || !snapshot || typeof snapshot !== "object") return fallback;
  const optionsByColumn = snapshot.options;
  if (!optionsByColumn || typeof optionsByColumn !== "object") return fallback;
  const options = optionsByColumn[column];
  return Array.isArray(options) && options.length > 0 ? options : fallback;
}

function buildDefaultFilterPayload(snapshot, configuredCountries, configuredPowertrains) {
  const columns = resolveColumns(snapshot);
  const countries = configuredCountries.length > 0
    ? configuredCountries
    : valuesFromSnapshot(snapshot, columns.country, FALLBACK_COUNTRIES);
  const powertrains = configuredPowertrains.length > 0
    ? configuredPowertrains
    : valuesFromSnapshot(snapshot, columns.powertrain, DEFAULT_POWERTRAINS)
      .filter((item) => DEFAULT_POWERTRAINS.includes(String(item).toUpperCase()));
  const filters = {};
  if (columns.country && countries.length > 0) filters[columns.country] = countries;
  if (columns.powertrain && powertrains.length > 0) filters[columns.powertrain] = powertrains;
  return { columns, filters };
}

function buildDefaultSelections(snapshot, configuredCountries, configuredPowertrains) {
  const { columns, filters } = buildDefaultFilterPayload(
    snapshot,
    configuredCountries,
    configuredPowertrains,
  );
  return {
    columns,
    selections: {
      body_type: [],
      country: filters[columns.country] ?? [],
      make: [],
      model: [],
      powertrain: filters[columns.powertrain] ?? [],
      segment: [],
      version: [],
    },
    topLevelOptions: {
      body_type: valuesFromSnapshot(snapshot, columns.body_type, []),
      country: valuesFromSnapshot(snapshot, columns.country, []),
      powertrain: valuesFromSnapshot(snapshot, columns.powertrain, []),
      segment: valuesFromSnapshot(snapshot, columns.segment, []),
    },
  };
}

function selectionCoversAllOptions(values, options) {
  if (values.length === 0) return true;
  if (options.length === 0) return false;
  const selected = new Set(values);
  if (selected.size !== options.length) return false;
  return options.every((option) => selected.has(option));
}

function initialCascadeStartIndex(selections, topLevelOptions) {
  const powertrainIndex = FILTER_ORDER_KEYS.indexOf("powertrain");
  for (let index = 0; index < powertrainIndex; index += 1) {
    const key = FILTER_ORDER_KEYS[index];
    if (!selectionCoversAllOptions(selections[key] ?? [], topLevelOptions[key] ?? [])) {
      return powertrainIndex;
    }
  }
  return powertrainIndex + 1;
}

function buildDefaultCascadePayloads(snapshot, configuredCountries, configuredPowertrains) {
  const { columns, selections, topLevelOptions } = buildDefaultSelections(
    snapshot,
    configuredCountries,
    configuredPowertrains,
  );
  const startIndex = initialCascadeStartIndex(selections, topLevelOptions);
  const prefixFilters = {};
  for (let index = 0; index < startIndex; index += 1) {
    const key = FILTER_ORDER_KEYS[index];
    const column = columns[key];
    const values = selections[key] ?? [];
    if (column && values.length > 0) {
      prefixFilters[column] = values;
    }
  }

  const payloads = [];
  for (let index = startIndex; index < FILTER_ORDER_KEYS.length; index += 1) {
    const key = FILTER_ORDER_KEYS[index];
    const column = columns[key];
    if (!column) continue;
    payloads.push({
      column,
      filters: { ...prefixFilters },
    });
    if ((selections[key] ?? []).length === 0) break;
    prefixFilters[column] = selections[key];
  }
  return payloads;
}

function commonHeaders({ dataVersion, role, token, user }) {
  const headers = {
    accept: "application/json",
    "x-user-name": user,
    "x-user-role": role,
  };
  if (dataVersion) headers["x-jato-data-version"] = dataVersion;
  if (token) headers["x-auth-token"] = token;
  return headers;
}

async function readJsonSafe(response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text.slice(0, 240);
  }
}

async function login(origin, username, password, timeoutMs) {
  if (!username || !password) return null;
  const response = await fetch(joinApiPath(origin, "/auth/login"), {
    body: JSON.stringify({ password, username }),
    headers: { "content-type": "application/json" },
    method: "POST",
    signal: abortSignal(timeoutMs),
  });
  const payload = await readJsonSafe(response);
  if (!response.ok) {
    throw new Error(`login failed ${response.status}: ${JSON.stringify(payload).slice(0, 240)}`);
  }
  const token = String(payload?.token || "");
  if (!token) throw new Error("login response did not include token");
  return {
    role: String(payload?.role || DEFAULT_ROLE),
    token,
    user: String(payload?.username || username),
  };
}

async function callPrewarm(origin, requestDef, auth, timeoutMs) {
  const startedAt = performance.now();
  const headers = commonHeaders(auth);
  let body;
  if (requestDef.body !== undefined) {
    headers["content-type"] = "application/json";
    body = JSON.stringify(requestDef.body);
  }
  const response = await fetch(joinApiPath(origin, requestDef.path), {
    body,
    headers,
    method: requestDef.method,
    signal: abortSignal(timeoutMs),
  });
  const elapsedMs = Math.round(performance.now() - startedAt);
  const cache = response.headers.get("x-jato-edge-cache") || "";
  const endpoint = response.headers.get("x-jato-edge-cache-endpoint") || "";
  if (!response.ok) {
    const payload = await readJsonSafe(response);
    throw new Error(`${requestDef.label} ${response.status}: ${JSON.stringify(payload).slice(0, 240)}`);
  }
  const payload = requestDef.captureJson ? await readJsonSafe(response) : null;
  return {
    cache,
    elapsedMs,
    endpoint,
    label: requestDef.label,
    payload,
    status: response.status,
  };
}

function buildWarmupRequests(snapshot, configuredCountries, configuredPowertrains) {
  const { columns, filters } = buildDefaultFilterPayload(
    snapshot,
    configuredCountries,
    configuredPowertrains,
  );
  const topLevelColumns = [
    columns.country,
    columns.body_type,
    columns.segment,
    columns.powertrain,
  ].filter(Boolean);
  const cascadePayloads = buildDefaultCascadePayloads(
    snapshot,
    configuredCountries,
    configuredPowertrains,
  );
  const groupBy = columns.country || "国家";
  return [
    ...(columns.segment ? [{
      body: {
        items: [{ column: columns.segment, filters: {} }],
      },
      label: "filters-options-dashboard-segment",
      method: "POST",
      path: "/filters/options/batch",
    }] : []),
    {
      body: {
        items: topLevelColumns.map((column) => ({ column, filters: {} })),
      },
      label: "filters-options-batch",
      method: "POST",
      path: "/filters/options/batch",
    },
    ...(cascadePayloads.length > 0 ? [{
      body: {
        items: cascadePayloads,
      },
      label: "filters-options-default-cascade",
      method: "POST",
      path: "/filters/options/batch",
    }] : []),
    {
      body: {
        filters,
        prefer_precomputed: true,
        top_n: 120,
      },
      label: "analysis-overview-default",
      method: "POST",
      path: "/analysis/overview",
    },
    {
      body: {
        filters,
        grain: "month",
        group_by: groupBy,
        include_others: false,
        top_n: 10,
      },
      label: "time-series-grouped-month-country",
      method: "POST",
      path: "/analysis/time-series-grouped",
    },
    {
      body: {
        filters,
        grain: "year",
        group_by: groupBy,
        include_others: false,
        top_n: 10,
      },
      label: "time-series-grouped-year-country",
      method: "POST",
      path: "/analysis/time-series-grouped",
    },
  ];
}

function formatResult(result, round) {
  const cache = result.cache || "-";
  const endpoint = result.endpoint || "-";
  return [
    `round=${round}`,
    `role=${result.role}`,
    `label=${result.label}`,
    `status=${result.status}`,
    `cache=${cache}`,
    `ms=${result.elapsedMs}`,
    `endpoint=${endpoint}`,
  ].join(" ");
}

async function main() {
  const origin = normalizeOrigin(
    getArg("origin") || process.env.JATO_PREWARM_ORIGIN || DEFAULT_ORIGIN,
  );
  const timeoutMs = parseInteger(
    getArg("timeout-ms") || process.env.JATO_PREWARM_TIMEOUT_MS,
    DEFAULT_TIMEOUT_MS,
  );
  const repetitions = parseInteger(
    getArg("repetitions") || process.env.JATO_PREWARM_REPETITIONS,
    DEFAULT_REPETITIONS,
  );
  const configuredCountries = parseList(
    getArg("countries") || process.env.JATO_PREWARM_COUNTRIES,
    [],
  );
  const configuredPowertrains = parseList(
    getArg("powertrains") || process.env.JATO_PREWARM_POWERTRAINS,
    [],
  );
  const username = getArg("username") || process.env.JATO_PREWARM_USERNAME || "";
  const password = getArg("password") || process.env.JATO_PREWARM_PASSWORD || "";
  const loginAuth = await login(origin, username, password, timeoutMs);
  const explicitRole = getArg("role") || process.env.JATO_PREWARM_ROLE || "";
  const token = getArg("token") || process.env.JATO_PREWARM_TOKEN || loginAuth?.token || "";
  const roles = resolveWarmupRoles({
    configuredRoles: parseList(getArg("roles") || process.env.JATO_PREWARM_ROLES, []),
    explicitRole,
    loginRole: loginAuth?.role || "",
    token,
  });
  const auth = {
    dataVersion: getArg("data-version") || process.env.JATO_PREWARM_DATA_VERSION || "",
    token,
    user: getArg("user") || process.env.JATO_PREWARM_USER || loginAuth?.user || DEFAULT_USER,
  };
  const auths = roles.map((role) => ({ ...auth, role }));

  const seedRequests = [
    {
      captureJson: true,
      label: "metadata-filter-snapshot",
      method: "GET",
      path: "/metadata/filter-snapshot",
    },
    {
      label: "metadata-columns",
      method: "GET",
      path: "/metadata/columns",
    },
    {
      label: "assistant-country-metadata",
      method: "GET",
      path: "/assistant/country/metadata",
    },
    {
      label: "analysis-data-freshness",
      method: "GET",
      path: "/analysis/data-freshness",
    },
  ];

  const results = [];
  let snapshot = null;
  const failOnError = hasFlag("fail-on-error") || process.env.JATO_PREWARM_FAIL_ON_ERROR === "1";

  for (let round = 1; round <= repetitions; round += 1) {
    for (const scopedAuth of auths) {
      for (const requestDef of seedRequests) {
        try {
          const result = await callPrewarm(origin, requestDef, scopedAuth, timeoutMs);
          const scopedResult = { ...result, role: scopedAuth.role, round };
          results.push(scopedResult);
          console.log(formatResult(scopedResult, round));
          if (requestDef.label === "metadata-filter-snapshot" && result.payload) {
            snapshot = result.payload;
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.warn(`round=${round} role=${scopedAuth.role} label=${requestDef.label} error=${message}`);
          if (failOnError) throw error;
        }
      }

      const dependentRequests = buildWarmupRequests(
        snapshot,
        configuredCountries,
        configuredPowertrains,
      );
      for (const requestDef of dependentRequests) {
        try {
          const result = await callPrewarm(origin, requestDef, scopedAuth, timeoutMs);
          const scopedResult = { ...result, role: scopedAuth.role, round };
          results.push(scopedResult);
          console.log(formatResult(scopedResult, round));
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.warn(`round=${round} role=${scopedAuth.role} label=${requestDef.label} error=${message}`);
          if (failOnError) throw error;
        }
      }
    }

    if (round < repetitions) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  const hitCount = results.filter((item) => item.cache === "HIT").length;
  const missCount = results.filter((item) => item.cache === "MISS").length;
  const bypassCount = results.filter((item) => item.cache === "BYPASS").length;
  console.log(`summary origin=${origin} user=${auth.user} roles=${roles.join(",")} hit=${hitCount} miss=${missCount} bypass=${bypassCount} total=${results.length}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}

module.exports = {
  buildDefaultCascadePayloads,
  buildDefaultFilterPayload,
  buildWarmupRequests,
  initialCascadeStartIndex,
  resolveWarmupRoles,
  resolveColumns,
};
