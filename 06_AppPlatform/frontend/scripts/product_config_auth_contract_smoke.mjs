import { createHmac } from "node:crypto";

function parseArgs(argv) {
  const options = {
    apiBase: process.env.PRODUCT_CONFIG_SMOKE_API_BASE || "http://127.0.0.1:8004/v1",
    viewerToken: process.env.PRODUCT_CONFIG_VIEWER_TOKEN || "",
    editorToken: process.env.PRODUCT_CONFIG_EDITOR_TOKEN || process.env.PRODUCT_CONFIG_SMOKE_AUTH_TOKEN || "",
    adminToken: process.env.PRODUCT_CONFIG_ADMIN_TOKEN || "",
    viewerUser: process.env.PRODUCT_CONFIG_VIEWER_USER || "config-viewer-smoke",
    editorUser: process.env.PRODUCT_CONFIG_EDITOR_USER || "config-editor-smoke",
    adminUser: process.env.PRODUCT_CONFIG_ADMIN_USER || "config-admin-smoke",
    compareTrimIds: (process.env.PRODUCT_CONFIG_AUTH_SMOKE_TRIM_IDS || "").split(",").map((value) => value.trim()).filter(Boolean),
    allowPartial: process.env.PRODUCT_CONFIG_AUTH_SMOKE_ALLOW_PARTIAL === "1",
    mintLocalTokens: process.env.PRODUCT_CONFIG_AUTH_SMOKE_MINT_LOCAL_TOKENS === "1",
    jwtSecret: process.env.APP_JWT_SECRET || "change-me-jwt-secret",
    jwtTtlSeconds: 24 * 3600,
    strict: false,
    timeoutMs: 15000,
    help: false,
  };
  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") options.help = true;
    else if (arg === "--allow-partial") options.allowPartial = true;
    else if (arg === "--mint-local-tokens") options.mintLocalTokens = true;
    else if (arg === "--strict") options.strict = true;
    else if (arg.startsWith("--api-base=")) options.apiBase = arg.slice("--api-base=".length);
    else if (arg.startsWith("--viewer-token=")) options.viewerToken = arg.slice("--viewer-token=".length);
    else if (arg.startsWith("--editor-token=")) options.editorToken = arg.slice("--editor-token=".length);
    else if (arg.startsWith("--admin-token=")) options.adminToken = arg.slice("--admin-token=".length);
    else if (arg.startsWith("--viewer-user=")) options.viewerUser = arg.slice("--viewer-user=".length);
    else if (arg.startsWith("--editor-user=")) options.editorUser = arg.slice("--editor-user=".length);
    else if (arg.startsWith("--admin-user=")) options.adminUser = arg.slice("--admin-user=".length);
    else if (arg.startsWith("--compare-trim-ids=")) options.compareTrimIds = arg.slice("--compare-trim-ids=".length).split(",").map((value) => value.trim()).filter(Boolean);
    else if (arg.startsWith("--jwt-secret=")) options.jwtSecret = arg.slice("--jwt-secret=".length);
    else if (arg.startsWith("--jwt-ttl-seconds=")) options.jwtTtlSeconds = Math.max(60, Number(arg.slice("--jwt-ttl-seconds=".length)) || options.jwtTtlSeconds);
    else if (arg.startsWith("--timeout-ms=")) options.timeoutMs = Math.max(5000, Number(arg.slice("--timeout-ms=".length)) || options.timeoutMs);
  }
  options.apiBase = options.apiBase.replace(/\/+$/, "");
  applyLocalTokens(options);
  return options;
}

function usage() {
  return [
    "Product Config auth contract smoke.",
    "",
    "Checks the role boundary used by /product/compare/config against a live API:",
    "- viewer can read source/config/compare surfaces",
    "- viewer cannot write source/config-column/config-value resources",
    "- editor/admin can reach write handlers, with 404 accepted for fake ids",
    "",
    "Production/staging strict example:",
    "",
    "  node scripts/product_config_auth_contract_smoke.mjs --strict \\",
    "    --api-base=https://<backend>/v1 \\",
    "    --viewer-token=<viewer> \\",
    "    --editor-token=<editor> \\",
    "    --compare-trim-ids=id1,id2",
    "",
    "Local partial example with the default static editor token:",
    "",
    "  node scripts/product_config_auth_contract_smoke.mjs --allow-partial \\",
    "    --api-base=http://127.0.0.1:8004/v1 \\",
    "    --editor-token=change-me",
    "",
    "Options:",
    "  --viewer-token=...       Token expected to resolve to viewer/order_filler.",
    "  --editor-token=...       Token expected to resolve to editor/admin/developer.",
    "  --admin-token=...        Optional stronger write token.",
    "  --mint-local-tokens      Mint local HS256 JWTs for viewer/editor/admin using APP_JWT_SECRET.",
    "  --jwt-secret=...         Override the local JWT secret; default matches backend dev default.",
    "  --compare-trim-ids=a,b   Optional read smoke for a real compare payload.",
    "  --allow-partial          Allow missing viewer/editor tokens and mark those checks skipped.",
    "  --strict                 Exit non-zero when any check is skipped/degraded/failed.",
  ].join("\n");
}

function base64urlJson(value) {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}

function mintJwt(username, role, jwtSecret, ttlSeconds) {
  const now = Math.floor(Date.now() / 1000);
  const header = base64urlJson({ alg: "HS256", typ: "JWT" });
  const body = base64urlJson({
    username,
    role,
    exp: now + ttlSeconds,
    iat: now,
  });
  const signature = createHmac("sha256", Buffer.from(jwtSecret))
    .update(`${header}.${body}`)
    .digest("base64url");
  return `${header}.${body}.${signature}`;
}

function applyLocalTokens(options) {
  if (!options.mintLocalTokens) return;
  if (!options.viewerToken) {
    options.viewerToken = mintJwt(options.viewerUser, "viewer", options.jwtSecret, options.jwtTtlSeconds);
  }
  if (!options.editorToken) {
    options.editorToken = mintJwt(options.editorUser, "editor", options.jwtSecret, options.jwtTtlSeconds);
  }
  if (!options.adminToken) {
    options.adminToken = mintJwt(options.adminUser, "admin", options.jwtSecret, options.jwtTtlSeconds);
  }
}

function headers(token, userName) {
  return {
    "Accept": "application/json",
    "Content-Type": "application/json",
    ...(token ? { "X-Auth-Token": token } : {}),
    "X-User-Name": userName,
  };
}

async function requestJson(options, role, token, userName, method, apiPath, body) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs);
  try {
    const response = await fetch(`${options.apiBase}${apiPath}`, {
      method,
      headers: headers(token, userName),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    return {
      role,
      method,
      apiPath,
      status: response.status,
      ok: response.ok,
      payload,
    };
  } catch (error) {
    return {
      role,
      method,
      apiPath,
      status: 0,
      ok: false,
      payload: {
        error: error instanceof Error ? error.message : String(error),
        name: error instanceof Error ? error.name : "Error",
        cause: error instanceof Error && error.cause ? String(error.cause) : null,
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

function pass(key, message, details = {}) {
  return { key, status: "passed", message, details };
}

function fail(key, message, details = {}) {
  return { key, status: "failed", message, details };
}

function skip(key, message, details = {}) {
  return { key, status: "skipped", message, details };
}

function statusIsForbidden(status) {
  return status === 401 || status === 403;
}

function statusReachedHandler(status) {
  return status > 0 && !statusIsForbidden(status);
}

function resolvedRole(payload) {
  return String(payload?.role || payload?.user?.role || "").toLowerCase();
}

async function checkMe(options, roleName, token, userName, acceptedRoles) {
  const response = await requestJson(options, roleName, token, userName, "GET", "/auth/me");
  const role = resolvedRole(response.payload);
  if (response.ok && acceptedRoles.includes(role)) {
    return pass(`${roleName}_auth_me`, `${roleName} token resolved as ${role}`, { response });
  }
  return fail(
    `${roleName}_auth_me`,
    `${roleName} token did not resolve to expected role (${acceptedRoles.join(", ")})`,
    { response, resolvedRole: role },
  );
}

async function checkRead(options, roleName, token, userName) {
  const source = await requestJson(options, roleName, token, userName, "GET", "/engineering-config/source/snapshots?limit=1");
  const trim = await requestJson(options, roleName, token, userName, "GET", "/engineering-config/trims?limit=1");
  const checks = [];
  checks.push(source.ok
    ? pass(`${roleName}_read_source_library`, `${roleName} can read source library`, { status: source.status })
    : fail(`${roleName}_read_source_library`, `${roleName} could not read source library`, { response: source }));
  checks.push(trim.ok
    ? pass(`${roleName}_read_config_columns`, `${roleName} can read config-column library`, { status: trim.status })
    : fail(`${roleName}_read_config_columns`, `${roleName} could not read config-column library`, { response: trim }));
  if (options.compareTrimIds.length >= 2) {
    const params = new URLSearchParams({ trim_ids: options.compareTrimIds.slice(0, 4).join(",") });
    const compare = await requestJson(options, roleName, token, userName, "GET", `/engineering-config/compare?${params.toString()}`);
    checks.push(compare.ok
      ? pass(`${roleName}_read_compare`, `${roleName} can read compare payload`, {
          status: compare.status,
          trimCount: Array.isArray(compare.payload?.trims) ? compare.payload.trims.length : null,
          rowCount: Array.isArray(compare.payload?.rows) ? compare.payload.rows.length : null,
        })
      : fail(`${roleName}_read_compare`, `${roleName} could not read compare payload`, { response: compare }));
  }
  return checks;
}

async function checkViewerWriteDenied(options) {
  if (!options.viewerToken) {
    return [skip("viewer_write_denied", "viewer token missing; cannot prove viewer write denial")];
  }
  const fakeSourceId = "00000000-0000-0000-0000-000000000000";
  const fakeTrimId = "00000000-0000-0000-0000-000000000000";
  const fakeValueId = "00000000-0000-0000-0000-000000000000";
  const sourceDelete = await requestJson(
    options,
    "viewer",
    options.viewerToken,
    options.viewerUser,
    "DELETE",
    `/engineering-config/source/snapshots/${fakeSourceId}`,
  );
  const trimPatch = await requestJson(
    options,
    "viewer",
    options.viewerToken,
    options.viewerUser,
    "PATCH",
    `/engineering-config/trims/${fakeTrimId}`,
    { status: "trashed" },
  );
  const valuePatch = await requestJson(
    options,
    "viewer",
    options.viewerToken,
    options.viewerUser,
    "PATCH",
    `/engineering-config/values/${fakeValueId}`,
    { raw_value: "●", expected_version: 1, updated_by: options.viewerUser, comment: "auth contract smoke fake-id write denial" },
  );
  return [
    statusIsForbidden(sourceDelete.status)
      ? pass("viewer_cannot_delete_source", "viewer is blocked from source delete", { status: sourceDelete.status })
      : fail("viewer_cannot_delete_source", "viewer was not blocked from source delete", { response: sourceDelete }),
    statusIsForbidden(trimPatch.status)
      ? pass("viewer_cannot_patch_trim", "viewer is blocked from config-column patch", { status: trimPatch.status })
      : fail("viewer_cannot_patch_trim", "viewer was not blocked from config-column patch", { response: trimPatch }),
    statusIsForbidden(valuePatch.status)
      ? pass("viewer_cannot_patch_value", "viewer is blocked from config-value patch", { status: valuePatch.status })
      : fail("viewer_cannot_patch_value", "viewer was not blocked from config-value patch", { response: valuePatch }),
  ];
}

async function checkWriterReachability(options, roleName, token, userName) {
  if (!token) {
    return [skip(`${roleName}_write_reachability`, `${roleName} token missing; cannot prove write handler reachability`)];
  }
  const fakeSourceId = "00000000-0000-0000-0000-000000000000";
  const fakeTrimId = "00000000-0000-0000-0000-000000000000";
  const fakeValueId = "00000000-0000-0000-0000-000000000000";
  const sourceDelete = await requestJson(
    options,
    roleName,
    token,
    userName,
    "DELETE",
    `/engineering-config/source/snapshots/${fakeSourceId}`,
  );
  const trimPatch = await requestJson(
    options,
    roleName,
    token,
    userName,
    "PATCH",
    `/engineering-config/trims/${fakeTrimId}`,
    { status: "trashed" },
  );
  const valuePatch = await requestJson(
    options,
    roleName,
    token,
    userName,
    "PATCH",
    `/engineering-config/values/${fakeValueId}`,
    { raw_value: "●", expected_version: 1, updated_by: userName, comment: "auth contract smoke fake-id write reachability" },
  );
  return [
    statusReachedHandler(sourceDelete.status)
      ? pass(`${roleName}_source_delete_handler_reached`, `${roleName} reached source delete handler`, { status: sourceDelete.status })
      : fail(`${roleName}_source_delete_handler_reached`, `${roleName} was blocked before source delete handler`, { response: sourceDelete }),
    statusReachedHandler(trimPatch.status)
      ? pass(`${roleName}_trim_patch_handler_reached`, `${roleName} reached config-column patch handler`, { status: trimPatch.status })
      : fail(`${roleName}_trim_patch_handler_reached`, `${roleName} was blocked before config-column patch handler`, { response: trimPatch }),
    statusReachedHandler(valuePatch.status)
      ? pass(`${roleName}_value_patch_handler_reached`, `${roleName} reached config-value patch handler`, { status: valuePatch.status })
      : fail(`${roleName}_value_patch_handler_reached`, `${roleName} was blocked before config-value patch handler`, { response: valuePatch }),
  ];
}

async function runSmoke(options) {
  const checks = [];
  if (!options.viewerToken && !options.editorToken && !options.adminToken && !options.allowPartial) {
    checks.push(fail(
      "token_configuration",
      "No auth tokens provided. Pass viewer/editor tokens, or --allow-partial for local static-token checks.",
    ));
  }

  if (options.viewerToken) {
    checks.push(await checkMe(options, "viewer", options.viewerToken, options.viewerUser, ["viewer", "order_filler"]));
    checks.push(...await checkRead(options, "viewer", options.viewerToken, options.viewerUser));
  } else {
    checks.push(skip("viewer_token", "viewer token missing"));
  }

  if (options.editorToken) {
    checks.push(await checkMe(options, "editor", options.editorToken, options.editorUser, ["editor", "admin", "developer"]));
    checks.push(...await checkRead(options, "editor", options.editorToken, options.editorUser));
  } else {
    checks.push(skip("editor_token", "editor token missing"));
  }

  if (options.adminToken) {
    checks.push(await checkMe(options, "admin", options.adminToken, options.adminUser, ["admin", "developer"]));
    checks.push(...await checkRead(options, "admin", options.adminToken, options.adminUser));
  }

  checks.push(...await checkViewerWriteDenied(options));
  checks.push(...await checkWriterReachability(options, "editor", options.editorToken, options.editorUser));
  if (options.adminToken) {
    checks.push(...await checkWriterReachability(options, "admin", options.adminToken, options.adminUser));
  }

  const summary = {
    passed: checks.filter((check) => check.status === "passed").length,
    skipped: checks.filter((check) => check.status === "skipped").length,
    failed: checks.filter((check) => check.status === "failed").length,
  };
  const status = summary.failed > 0 ? "failed" : summary.skipped > 0 ? "degraded" : "passed";
  const report = {
    createdAt: new Date().toISOString(),
    apiBase: options.apiBase,
    status,
    strict: options.strict,
    allowPartial: options.allowPartial,
    compareTrimIds: options.compareTrimIds,
    summary,
    checks,
  };
  console.log(JSON.stringify(report, null, 2));
  if (summary.failed > 0 || (options.strict && status !== "passed")) {
    process.exitCode = 1;
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
