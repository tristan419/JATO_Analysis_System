const TOKEN_URL = "https://oauth2.googleapis.com/token";
const USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo";

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function reject(status, message) {
  return jsonResponse({ error: message }, status);
}

function hasValidRelayToken(request, env) {
  const expected = String(env.RELAY_TOKEN || "").trim();
  if (!expected) return false;
  return request.headers.get("x-jato-relay-token") === expected;
}

function relayResponse(response) {
  const headers = new Headers(response.headers);
  headers.set("cache-control", "no-store");
  headers.delete("set-cookie");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function relayToken(request) {
  if (request.method !== "POST") {
    return reject(405, "method_not_allowed");
  }
  const response = await fetch(TOKEN_URL, {
    method: "POST",
    headers: {
      "content-type": request.headers.get("content-type") || "application/x-www-form-urlencoded",
      "accept": "application/json",
    },
    body: await request.text(),
  });
  return relayResponse(response);
}

async function relayUserinfo(request) {
  if (request.method !== "GET") {
    return reject(405, "method_not_allowed");
  }
  const authorization = request.headers.get("authorization") || "";
  if (!authorization.toLowerCase().startsWith("bearer ")) {
    return reject(400, "missing_bearer_token");
  }
  const response = await fetch(USERINFO_URL, {
    method: "GET",
    headers: {
      "authorization": authorization,
      "accept": "application/json",
    },
  });
  return relayResponse(response);
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const path = Array.isArray(params.path) ? params.path.join("/") : String(params.path || "");
  if (path === "healthz") {
    return jsonResponse({ status: "ok" });
  }
  if (!hasValidRelayToken(request, env)) {
    return reject(401, "unauthorized");
  }
  if (path === "token") {
    return relayToken(request);
  }
  if (path === "userinfo") {
    return relayUserinfo(request);
  }
  return reject(404, "not_found");
}
