#!/usr/bin/env bash
set -euo pipefail
# ──────────────────────────────────────────────────────────────
# Update mihomo (Clash Meta) config on Tencent Cloud server
# from a Clash or Shadowrocket subscription URL.
#
# Usage:
#   bash 03_Scripts/deploy/update_mihomo_subscription.sh [SUBSCRIPTION_URL]
#
# If SUBSCRIPTION_URL is omitted, uses MIHOMO_SUB_URL env var, a protected
# local subscription file, or a 0dcloud profile URL discovered on the target
# host. Set MIHOMO_DB_PATH to override the 0dcloud sqlite path when it is not
# under the default user home.
# ──────────────────────────────────────────────────────────────

SSH_HOST="${SSH_HOST:-tencent-cloud}"
MIHOMO_CONFIG_DIR="/etc/mihomo"
MIHOMO_LOCAL="${MIHOMO_LOCAL:-false}"
MIHOMO_DRY_RUN="${MIHOMO_DRY_RUN:-false}"
MIHOMO_SUB_URL_FILE="${MIHOMO_SUB_URL_FILE:-$MIHOMO_CONFIG_DIR/subscription_url}"
MIHOMO_SUB_PROXY_URL="${MIHOMO_SUB_PROXY_URL:-http://127.0.0.1:7897}"
MIHOMO_SUB_USER_AGENT="${MIHOMO_SUB_USER_AGENT:-mihomo/1.18 JATO-deploy}"
MIHOMO_SOURCE_CONFIG="${MIHOMO_SOURCE_CONFIG:-}"
MIHOMO_OUTPUT_CONFIG="${MIHOMO_OUTPUT_CONFIG:-}"

read_subscription_url_file() {
  if [[ "$MIHOMO_LOCAL" == "true" ]]; then
    if [[ -r "$MIHOMO_SUB_URL_FILE" ]]; then
      sed -n '1p' "$MIHOMO_SUB_URL_FILE"
    elif command -v sudo >/dev/null 2>&1; then
      sudo -n sed -n '1p' "$MIHOMO_SUB_URL_FILE" 2>/dev/null || true
    fi
    return
  fi

  ssh "$SSH_HOST" "file='${MIHOMO_SUB_URL_FILE}'; if [ -r \"\$file\" ]; then sed -n '1p' \"\$file\"; else sudo -n sed -n '1p' \"\$file\" 2>/dev/null || true; fi"
}

discover_remote_subscription_url() {
  if [[ "$MIHOMO_LOCAL" == "true" ]]; then
    python3 - <<"PY"
import os
import re
import sqlite3
from pathlib import Path

URL_RE = re.compile(r"https?://[^\s\"'<>\\{}\x00-\x1f]+")

def q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'

def candidate_paths() -> list[Path]:
    paths: list[Path] = []
    if os.getenv("MIHOMO_DB_PATH"):
        paths.append(Path(os.environ["MIHOMO_DB_PATH"]))
    if os.getenv("HOME"):
        paths.append(Path(os.environ["HOME"]) / ".local/share/0dcloud/database.sqlite")
    paths.append(Path("/home/ubuntu/.local/share/0dcloud/database.sqlite"))
    paths.extend(Path("/home").glob("*/.local/share/0dcloud/database.sqlite"))
    paths.append(Path("/root/.local/share/0dcloud/database.sqlite"))
    seen: set[str] = set()
    result: list[Path] = []
    for path in paths:
        key = str(path)
        if key not in seen:
            seen.add(key)
            result.append(path)
    return result

def extract_url(value: object) -> str:
    text = str(value or "").strip()
    if text.startswith(("http://", "https://")):
        return text
    match = URL_RE.search(text)
    return match.group(0).strip() if match else ""

def looks_like_subscription_url(url: str) -> bool:
    lower = url.lower()
    return any(
        token in lower
        for token in ("sub", "subscribe", "clash", "profile", "api", "token", "mihomo")
    )

def safe_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False

def discover_from_raw_file(db_path: Path) -> str:
    try:
        text = db_path.read_bytes().decode("utf-8", "ignore")
    except OSError:
        return ""
    urls: list[str] = []
    seen: set[str] = set()
    for match in URL_RE.finditer(text):
        url = match.group(0).strip()
        if looks_like_subscription_url(url) and url not in seen:
            seen.add(url)
            urls.append(url)
    return "\n".join(urls)

def discover_from_db(db_path: Path) -> str:
    if not safe_exists(db_path):
        return ""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    urls: list[str] = []
    seen: set[str] = set()

    def add_url(url: str) -> None:
        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    tables = [
        row[0]
        for row in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    if "profiles" in tables:
        cols = [row[1] for row in cur.execute("PRAGMA table_info(profiles)").fetchall()]
        if "url" in cols:
            order_cols = [name for name in ("last_update_date", "id") if name in cols]
            order_sql = (
                " ORDER BY " + ", ".join(f"{q(name)} DESC" for name in order_cols)
                if order_cols else ""
            )
            rows = cur.execute(
                f"SELECT {q('url')} FROM {q('profiles')} "
                f"WHERE {q('url')} IS NOT NULL AND TRIM({q('url')}) != ''"
                f"{order_sql} LIMIT 20"
            ).fetchall()
            for (value,) in rows:
                add_url(extract_url(value))
    for table in tables:
        cols = [row[1] for row in cur.execute(f"PRAGMA table_info({q(table)})").fetchall()]
        url_cols = [
            col for col in cols
            if any(token in col.lower() for token in ("url", "link", "sub", "remote"))
        ]
        for col in url_cols:
            try:
                rows = cur.execute(
                    f"SELECT {q(col)} FROM {q(table)} "
                    f"WHERE {q(col)} IS NOT NULL AND TRIM({q(col)}) != '' LIMIT 20"
                ).fetchall()
            except sqlite3.Error:
                continue
            for (value,) in rows:
                add_url(extract_url(value))
    for raw_url in discover_from_raw_file(db_path).splitlines():
        add_url(raw_url.strip())
    return "\n".join(urls)

for path in candidate_paths():
    url = discover_from_db(path)
    if url:
        print(url)
        break
PY
    return
  fi

  ssh "$SSH_HOST" "MIHOMO_DB_PATH='${MIHOMO_DB_PATH:-}' python3 - <<'PY'
import os
import re
import sqlite3
from pathlib import Path

URL_RE = re.compile(r\"https?://[^\\s\\\"'<>\\\\{}\\x00-\\x1f]+\")

def q(identifier: str) -> str:
    return '\"' + identifier.replace('\"', '\"\"') + '\"'

def candidate_paths() -> list[Path]:
    paths: list[Path] = []
    if os.getenv(\"MIHOMO_DB_PATH\"):
        paths.append(Path(os.environ[\"MIHOMO_DB_PATH\"]))
    if os.getenv(\"HOME\"):
        paths.append(Path(os.environ[\"HOME\"]) / \".local/share/0dcloud/database.sqlite\")
    paths.append(Path(\"/home/ubuntu/.local/share/0dcloud/database.sqlite\"))
    paths.extend(Path(\"/home\").glob(\"*/.local/share/0dcloud/database.sqlite\"))
    paths.append(Path(\"/root/.local/share/0dcloud/database.sqlite\"))
    seen: set[str] = set()
    result: list[Path] = []
    for path in paths:
        key = str(path)
        if key not in seen:
            seen.add(key)
            result.append(path)
    return result

def extract_url(value: object) -> str:
    text = str(value or \"\").strip()
    if text.startswith((\"http://\", \"https://\")):
        return text
    match = URL_RE.search(text)
    return match.group(0).strip() if match else \"\"

def looks_like_subscription_url(url: str) -> bool:
    lower = url.lower()
    return any(
        token in lower
        for token in (\"sub\", \"subscribe\", \"clash\", \"profile\", \"api\", \"token\", \"mihomo\")
    )

def safe_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False

def discover_from_raw_file(db_path: Path) -> str:
    try:
        text = db_path.read_bytes().decode(\"utf-8\", \"ignore\")
    except OSError:
        return \"\"
    urls: list[str] = []
    seen: set[str] = set()
    for match in URL_RE.finditer(text):
        url = match.group(0).strip()
        if looks_like_subscription_url(url) and url not in seen:
            seen.add(url)
            urls.append(url)
    return \"\\n\".join(urls)

def discover_from_db(db_path: Path) -> str:
    if not safe_exists(db_path):
        return \"\"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    urls: list[str] = []
    seen: set[str] = set()

    def add_url(url: str) -> None:
        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    tables = [
        row[0]
        for row in cur.execute(
            \"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\"
        ).fetchall()
    ]
    if \"profiles\" in tables:
        cols = [row[1] for row in cur.execute(\"PRAGMA table_info(profiles)\").fetchall()]
        if \"url\" in cols:
            order_cols = [name for name in (\"last_update_date\", \"id\") if name in cols]
            order_sql = (
                \" ORDER BY \" + \", \".join(f\"{q(name)} DESC\" for name in order_cols)
                if order_cols else \"\"
            )
            rows = cur.execute(
                f\"SELECT {q('url')} FROM {q('profiles')} \"
                f\"WHERE {q('url')} IS NOT NULL AND TRIM({q('url')}) != ''\"
                f\"{order_sql} LIMIT 20\"
            ).fetchall()
            for (value,) in rows:
                add_url(extract_url(value))
    for table in tables:
        cols = [row[1] for row in cur.execute(f\"PRAGMA table_info({q(table)})\").fetchall()]
        url_cols = [
            col for col in cols
            if any(token in col.lower() for token in (\"url\", \"link\", \"sub\", \"remote\"))
        ]
        for col in url_cols:
            try:
                rows = cur.execute(
                    f\"SELECT {q(col)} FROM {q(table)} \"
                    f\"WHERE {q(col)} IS NOT NULL AND TRIM({q(col)}) != '' LIMIT 20\"
                ).fetchall()
            except sqlite3.Error:
                continue
            for (value,) in rows:
                add_url(extract_url(value))
    for raw_url in discover_from_raw_file(db_path).splitlines():
        add_url(raw_url.strip())
    return \"\\n\".join(urls)

for path in candidate_paths():
    url = discover_from_db(path)
    if url:
        print(url)
        break
PY"
}

SUB_URL="${1:-${MIHOMO_SUB_URL:-}}"
SUB_URLS=()
if [[ -z "$MIHOMO_SOURCE_CONFIG" ]]; then
  if [[ -n "$SUB_URL" ]]; then
    SUB_URLS+=("$SUB_URL")
  else
    FILE_SUB_URL="$(read_subscription_url_file | head -n 1 | tr -d '\r' || true)"
    if [[ -n "$FILE_SUB_URL" ]]; then
      SUB_URLS+=("$FILE_SUB_URL")
    fi
    while IFS= read -r candidate_url; do
      if [[ -n "$candidate_url" ]]; then
        SUB_URLS+=("$candidate_url")
      fi
    done < <(discover_remote_subscription_url)
  fi
fi

if [[ -z "$MIHOMO_SOURCE_CONFIG" && "${#SUB_URLS[@]}" -eq 0 ]]; then
  echo "[mihomo-sub] ERROR: no subscription URL configured. Set MIHOMO_SUB_URL, pass SUBSCRIPTION_URL, write $MIHOMO_SUB_URL_FILE, or set MIHOMO_DB_PATH to a 0dcloud database with a saved URL." >&2
  exit 1
fi

log_subscription_url() {
  python3 - "$1" <<"PY"
import hashlib
import sys
from urllib.parse import urlparse

url = sys.argv[1]
parsed = urlparse(url)
print(
    "[mihomo-sub] Fetching subscription: "
    f"host={parsed.netloc or 'unknown'} len={len(url)} "
    f"sha256={hashlib.sha256(url.encode()).hexdigest()[:12]}"
)
PY
}

fetch_subscription_candidate() {
  local candidate_url="$1"
  local output_file="$2"
  local mode="$3"
  local curl_args=(
    -sL
    -w "%{http_code}"
    --connect-timeout 15
    --max-time 60
    -A "$MIHOMO_SUB_USER_AGENT"
    -o "$output_file"
  )
  local curl_rc=0
  local http_code

  if [[ "$mode" == "local-proxy" ]]; then
    curl_args+=(--proxy "$MIHOMO_SUB_PROXY_URL")
  fi

  http_code="$(curl "${curl_args[@]}" "$candidate_url")" || curl_rc=$?
  printf '%s %s\n' "$curl_rc" "$http_code"
}

normalize_subscription_file() {
  local file="$1"
  local normalized
  normalized="$(mktemp)"
  if python3 - "$file" "$normalized" <<"PY"
import base64
import hashlib
import ipaddress
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import yaml

source = Path(sys.argv[1])
target = Path(sys.argv[2])


def _decode_b64(value: str) -> str:
    compact = "".join(value.strip().split())
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        for pad_len in range(4):
            try:
                return decoder(compact + ("=" * pad_len)).decode("utf-8")
            except Exception:
                continue
    raise ValueError("invalid base64")


def _candidate_texts(raw: str) -> list[str]:
    values = [raw]
    try:
        decoded = _decode_b64(raw)
    except Exception:
        decoded = ""
    if decoded and decoded not in values:
        values.append(decoded)
    return values


def _split_host_port(value: str) -> tuple[str, int]:
    if value.startswith("["):
        host, _, rest = value[1:].partition("]")
        if not rest.startswith(":"):
            raise ValueError("missing port")
        return host, int(rest[1:])
    host, _, port = value.rpartition(":")
    if not host or not port:
        raise ValueError("missing host or port")
    return host, int(port)


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower()
    if normalized in {"localhost", "ip6-localhost"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _parse_ss(link: str, index: int) -> dict:
    parsed = urlsplit(link)
    body = link[len("ss://"):].split("#", 1)[0]
    body = body.split("?", 1)[0]
    name = unquote(parsed.fragment or f"ss-{index:02d}").strip() or f"ss-{index:02d}"
    if "@" in body:
        userinfo, hostport = body.rsplit("@", 1)
        method_password = _decode_b64(userinfo) if ":" not in userinfo else unquote(userinfo)
    else:
        decoded = _decode_b64(body)
        method_password, hostport = decoded.rsplit("@", 1)
    method, _, password = method_password.partition(":")
    if not method or not password:
        raise ValueError("missing method or password")
    host, port = _split_host_port(hostport)
    if _is_loopback_host(host):
        raise ValueError("loopback proxy server is not usable on this host")
    query = parse_qs(parsed.query or "")
    plugin = query.get("plugin", [""])[0]
    proxy = {
        "name": name,
        "type": "ss",
        "server": host,
        "port": port,
        "cipher": method,
        "password": password,
        "udp": True,
    }
    if plugin:
        # Plugins are deliberately not translated yet. Failing closed avoids
        # installing a config that looks valid but cannot connect.
        raise ValueError("ss plugin options are not supported")
    return proxy


def _unique_names(proxies: list[dict]) -> list[dict]:
    seen: dict[str, int] = {}
    result: list[dict] = []
    for proxy in proxies:
        base = str(proxy.get("name") or "proxy").strip() or "proxy"
        count = seen.get(base, 0)
        seen[base] = count + 1
        proxy = dict(proxy)
        proxy["name"] = base if count == 0 else f"{base}-{count + 1}"
        result.append(proxy)
    return result


raw = source.read_text(encoding="utf-8", errors="ignore").strip()
for text in _candidate_texts(raw):
    try:
        cfg = yaml.safe_load(text)
    except Exception:
        cfg = None
    if isinstance(cfg, dict) and isinstance(cfg.get("proxies"), list):
        target.write_text(text, encoding="utf-8")
        print("[mihomo-sub] Subscription format: clash-yaml")
        sys.exit(0)

links: list[str] = []
for text in _candidate_texts(raw):
    for line in text.splitlines():
        value = line.strip()
        if value.startswith("ss://"):
            links.append(value)

proxies: list[dict] = []
errors = 0
for index, link in enumerate(links, 1):
    try:
        proxies.append(_parse_ss(link, index))
    except Exception:
        errors += 1

if not proxies:
    print(
        f"[mihomo-sub] Subscription format: unsupported "
        f"bytes={len(raw.encode())} sha256={hashlib.sha256(raw.encode()).hexdigest()[:12]}",
        file=sys.stderr,
    )
    sys.exit(2)

proxies = _unique_names(proxies)
names = [proxy["name"] for proxy in proxies]
cfg = {
    "mixed-port": 7897,
    "allow-lan": False,
    "bind-address": "127.0.0.1",
    "mode": "rule",
    "log-level": "warning",
    "external-controller": "127.0.0.1:9090",
    "dns": {
        "enable": True,
        "listen": "127.0.0.1:1053",
    },
    "proxies": proxies,
    "proxy-groups": [
        {
            "name": "auto",
            "type": "url-test",
            "url": "https://www.gstatic.com/generate_204",
            "interval": 300,
            "tolerance": 80,
            "proxies": names,
        }
    ],
    "rules": ["MATCH,auto"],
}
target.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
print(
    f"[mihomo-sub] Subscription format: shadowrocket-ss converted={len(proxies)} skipped={errors}"
)
PY
  then
    mv "$normalized" "$file"
    return 0
  fi
  rm -f "$normalized"
  return 1
}

TMP_CONF=""
FETCH_OK=false
if [[ -n "$MIHOMO_SOURCE_CONFIG" ]]; then
  if [[ ! -r "$MIHOMO_SOURCE_CONFIG" ]]; then
    echo "[mihomo-sub] ERROR: MIHOMO_SOURCE_CONFIG is not readable" >&2
    exit 1
  fi
  TMP_CONF="$(mktemp)"
  cp "$MIHOMO_SOURCE_CONFIG" "$TMP_CONF"
  echo "[mihomo-sub] Using prebuilt mihomo config"
  FETCH_OK=true
else
  for candidate_url in "${SUB_URLS[@]}"; do
    log_subscription_url "$candidate_url"
    FETCH_MODES=("direct")
    if [[ "$MIHOMO_LOCAL" == "true" && -n "$MIHOMO_SUB_PROXY_URL" ]]; then
      FETCH_MODES+=("local-proxy")
    fi

    for fetch_mode in "${FETCH_MODES[@]}"; do
      TMP_CONF="$(mktemp)"
      read -r CURL_RC HTTP_CODE < <(fetch_subscription_candidate "$candidate_url" "$TMP_CONF" "$fetch_mode")

      if [[ "$CURL_RC" -ne 0 || "$HTTP_CODE" != "200" ]]; then
        echo "[mihomo-sub] WARN: mode=$fetch_mode curl_rc=$CURL_RC HTTP $HTTP_CODE — subscription candidate unavailable" >&2
        rm -f "$TMP_CONF"
        TMP_CONF=""
        continue
      fi

      if ! normalize_subscription_file "$TMP_CONF"; then
        echo "[mihomo-sub] WARN: mode=$fetch_mode subscription candidate could not be converted to a mihomo config" >&2
        rm -f "$TMP_CONF"
        TMP_CONF=""
        continue
      fi

      # Validate it looks like a YAML Clash config. Some managed subscriptions start
      # with comment headers such as #!MANAGED-CONFIG, so scan beyond the first lines.
      if ! grep -qE '^(port:|mixed-port:|proxies:|proxy-groups:)' "$TMP_CONF"; then
        echo "[mihomo-sub] WARN: mode=$fetch_mode converted subscription does not contain mihomo YAML keys" >&2
        rm -f "$TMP_CONF"
        TMP_CONF=""
        continue
      fi

      if [[ "$fetch_mode" == "local-proxy" ]]; then
        echo "[mihomo-sub] Fetched subscription through local mihomo proxy"
      fi
      FETCH_OK=true
      break
    done

    if [[ "$FETCH_OK" == "true" ]]; then
      break
    fi
  done
fi

if [[ "$FETCH_OK" != "true" || -z "$TMP_CONF" ]]; then
  echo "[mihomo-sub] ERROR: no usable subscription candidate could be downloaded" >&2
  exit 1
fi

echo "[mihomo-sub] Downloaded $(wc -l < "$TMP_CONF") lines of config"

# Patch: bind to localhost only, set mixed-port 7897
python3 -c "
import yaml, sys

with open('$TMP_CONF') as f:
    cfg = yaml.safe_load(f)

# Server-safe overrides
cfg['mixed-port'] = 7897
cfg.pop('port', None)
cfg.pop('socks-port', None)
cfg.pop('redir-port', None)
cfg['allow-lan'] = False
cfg['bind-address'] = '127.0.0.1'
cfg['log-level'] = 'warning'
cfg['external-controller'] = '127.0.0.1:9090'

# DNS: use DoH
cfg.setdefault('dns', {})
cfg['dns']['enable'] = True
cfg['dns']['listen'] = '127.0.0.1:1053'

proxies = cfg.get('proxies') or []
proxy_names = [
    proxy.get('name')
    for proxy in proxies
    if isinstance(proxy, dict) and proxy.get('name')
]
if proxy_names:
    cfg['proxy-groups'] = [
        {
            'name': 'auto',
            'type': 'url-test',
            'url': 'https://www.gstatic.com/generate_204',
            'interval': 300,
            'tolerance': 80,
            'proxies': proxy_names,
        }
    ]
    cfg['rules'] = ['MATCH,auto']
    cfg['mode'] = 'rule'

with open('$TMP_CONF', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)

print(
    f\"[mihomo-sub] Patched config: {len(proxies)} proxies, \"
    f\"auto_group={'yes' if proxy_names else 'no'}\"
)
"

if [[ -n "$MIHOMO_OUTPUT_CONFIG" ]]; then
  mkdir -p "$(dirname "$MIHOMO_OUTPUT_CONFIG")"
  cp "$TMP_CONF" "$MIHOMO_OUTPUT_CONFIG"
  chmod 600 "$MIHOMO_OUTPUT_CONFIG"
  echo "[mihomo-sub] Wrote prebuilt mihomo config"
  rm -f "$TMP_CONF" 2>/dev/null || true
  exit 0
fi

if [[ "$MIHOMO_DRY_RUN" == "true" ]]; then
  echo "[mihomo-sub] Dry run complete; config was not installed"
  rm -f "$TMP_CONF" 2>/dev/null || true
  exit 0
fi

restart_mihomo_local() {
  timeout 45s sudo systemctl restart mihomo.service \
    || timeout 45s sudo systemctl restart mihomo \
    || true
}

test_google_connectivity() {
  if [[ "$MIHOMO_LOCAL" == "true" ]]; then
    curl -sL --connect-timeout 10 --max-time 30 -x http://127.0.0.1:7897 \
      -o /dev/null -w 'HTTP:%{http_code}\n' https://news.google.com/
  else
    ssh "$SSH_HOST" "curl -sL --connect-timeout 10 --max-time 30 -x http://127.0.0.1:7897 -o /dev/null -w 'HTTP:%{http_code}\n' https://news.google.com/"
  fi
}

if [[ "$MIHOMO_LOCAL" == "true" ]]; then
  echo "[mihomo-sub] Installing local config..."
  sudo cp "$MIHOMO_CONFIG_DIR/config.yaml" "$MIHOMO_CONFIG_DIR/config.yaml.bak" 2>/dev/null || true
  sudo mv "$TMP_CONF" "$MIHOMO_CONFIG_DIR/config.yaml"
  sudo chmod 600 "$MIHOMO_CONFIG_DIR/config.yaml"
  restart_mihomo_local
  sleep 2
  echo '[mihomo-sub] Service restarted'
  sudo systemctl status mihomo.service --no-pager | head -8 || true
else
  echo "[mihomo-sub] Uploading to $SSH_HOST ..."
  scp "$TMP_CONF" "${SSH_HOST}:/tmp/mihomo-sub.yaml"
  ssh "$SSH_HOST" "sudo cp $MIHOMO_CONFIG_DIR/config.yaml $MIHOMO_CONFIG_DIR/config.yaml.bak 2>/dev/null || true; \
    sudo mv /tmp/mihomo-sub.yaml $MIHOMO_CONFIG_DIR/config.yaml && \
    sudo chmod 600 $MIHOMO_CONFIG_DIR/config.yaml && \
    timeout 45s sudo systemctl restart mihomo.service || timeout 45s sudo systemctl restart mihomo || true && \
    sleep 2 && \
    echo '[mihomo-sub] Service restarted' && \
    sudo systemctl status mihomo.service --no-pager | head -8"
fi

# Quick connectivity test
echo "[mihomo-sub] Testing Google connectivity..."
if ! GOOGLE_STATUS="$(test_google_connectivity)"; then
  echo "$GOOGLE_STATUS"
  echo "[mihomo-sub] ERROR: Google connectivity check failed after refresh" >&2
  if [[ "$MIHOMO_LOCAL" == "true" ]]; then
    echo "[mihomo-sub] Rolling back local mihomo config..."
    sudo cp "$MIHOMO_CONFIG_DIR/config.yaml.bak" "$MIHOMO_CONFIG_DIR/config.yaml" 2>/dev/null || true
    restart_mihomo_local
  else
    ssh "$SSH_HOST" "sudo cp $MIHOMO_CONFIG_DIR/config.yaml.bak $MIHOMO_CONFIG_DIR/config.yaml 2>/dev/null || true; timeout 45s sudo systemctl restart mihomo.service || timeout 45s sudo systemctl restart mihomo || true"
  fi
  exit 1
fi
echo "$GOOGLE_STATUS"

rm -f "$TMP_CONF" 2>/dev/null || true
echo "[mihomo-sub] Done."
