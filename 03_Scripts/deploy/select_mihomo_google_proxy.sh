#!/usr/bin/env bash
set -euo pipefail

MIHOMO_CONTROLLER_URL="${MIHOMO_CONTROLLER_URL:-http://127.0.0.1:9090}"
MIHOMO_PROXY_URL="${MIHOMO_PROXY_URL:-http://127.0.0.1:7897}"
MIHOMO_TEST_URL="${MIHOMO_TEST_URL:-https://oauth2.googleapis.com/token}"
MIHOMO_DELAY_TEST_URL="${MIHOMO_DELAY_TEST_URL:-https://www.gstatic.com/generate_204}"
MIHOMO_DELAY_TIMEOUT_MS="${MIHOMO_DELAY_TIMEOUT_MS:-5000}"
MIHOMO_MAX_CANDIDATES="${MIHOMO_MAX_CANDIDATES:-80}"

python3 - "$MIHOMO_CONTROLLER_URL" "$MIHOMO_PROXY_URL" "$MIHOMO_TEST_URL" "$MIHOMO_DELAY_TEST_URL" "$MIHOMO_DELAY_TIMEOUT_MS" "$MIHOMO_MAX_CANDIDATES" <<'PY'
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

controller_url, proxy_url, test_url, delay_test_url, timeout_ms, max_candidates_text = sys.argv[1:]
timeout_ms_int = int(timeout_ms)
max_candidates = int(max_candidates_text)


def request_json(method: str, path: str, payload: dict | None = None, timeout: float = 8.0) -> dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        controller_url.rstrip("/") + path,
        data=data,
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read().decode("utf-8").strip()
        if not body:
            return {}
        return json.loads(body)


def curl_check() -> bool:
    result = subprocess.run(
        [
            "curl",
            "-I",
            "--connect-timeout",
            "5",
            "--max-time",
            "12",
            "--proxy",
            proxy_url,
            test_url,
        ],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        return False
    output = result.stdout + result.stderr
    return "HTTP/2 404" in output or "HTTP/1.1 404" in output or "HTTP/2 400" in output


def delay_ok(proxy_name: str) -> tuple[bool, int | None]:
    quoted = urllib.parse.quote(proxy_name, safe="")
    quoted_url = urllib.parse.quote(delay_test_url, safe="")
    path = f"/proxies/{quoted}/delay?timeout={timeout_ms_int}&url={quoted_url}"
    try:
        payload = request_json("GET", path, timeout=max(6.0, timeout_ms_int / 1000 + 2))
    except Exception:
        return False, None
    delay = payload.get("delay")
    return isinstance(delay, int) and delay >= 0, delay if isinstance(delay, int) else None


try:
    proxies_payload = request_json("GET", "/proxies")
except Exception as exc:
    print(f"[mihomo-select] WARN: controller unavailable: {exc}", file=sys.stderr)
    raise SystemExit(0)

proxies = proxies_payload.get("proxies") or {}
group_names = [
    name
    for name, payload in proxies.items()
    if isinstance(payload, dict) and isinstance(payload.get("all"), list) and payload.get("all")
]
preferred_groups = [
    name
    for name in group_names
    if name.lower() in {"auto", "global", "proxy", "select"}
    or any(token in name for token in ("节点", "代理", "选择", "自动"))
]
target_groups = preferred_groups or group_names[:3]
if not target_groups:
    print("[mihomo-select] WARN: no selectable proxy groups found", file=sys.stderr)
    raise SystemExit(0)

tested: set[str] = set()
candidate_names: list[str] = []
ranked: list[tuple[int, str]] = []
for group_name in target_groups:
    group = proxies.get(group_name) or {}
    for proxy_name in group.get("all") or []:
        if proxy_name in tested or proxy_name in {"DIRECT", "REJECT"}:
            continue
        if proxy_name in proxies and isinstance(proxies[proxy_name], dict) and proxies[proxy_name].get("all"):
            continue
        tested.add(proxy_name)
        candidate_names.append(proxy_name)
        ok, delay = delay_ok(proxy_name)
        if ok and delay is not None:
            ranked.append((delay, proxy_name))
        if len(tested) >= max_candidates:
            break
    if len(tested) >= max_candidates:
        break

ranked.sort()
if not ranked:
    print("[mihomo-select] WARN: no candidate passed controller delay test; falling back to direct curl checks", file=sys.stderr)
    ranked = [(999_999, proxy_name) for proxy_name in candidate_names]

for _, proxy_name in ranked[:max_candidates]:
    selected_groups: list[str] = []
    for group_name in target_groups:
        group = proxies.get(group_name) or {}
        if proxy_name not in (group.get("all") or []):
            continue
        try:
            request_json("PUT", f"/proxies/{urllib.parse.quote(group_name, safe='')}", {"name": proxy_name})
            selected_groups.append(group_name)
        except Exception as exc:
            print(f"[mihomo-select] WARN: failed to select {proxy_name} for {group_name}: {exc}", file=sys.stderr)
    if not selected_groups:
        continue
    time.sleep(1)
    if curl_check():
        print(
            f"[mihomo-select] Selected proxy for Google OAuth: groups={','.join(selected_groups)}"
        )
        raise SystemExit(0)
    print(f"[mihomo-select] WARN: candidate failed curl check after select: {proxy_name}", file=sys.stderr)

print("[mihomo-select] WARN: no selected candidate passed Google OAuth curl check", file=sys.stderr)
PY
