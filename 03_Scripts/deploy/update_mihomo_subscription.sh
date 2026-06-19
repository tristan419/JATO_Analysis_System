#!/usr/bin/env bash
set -euo pipefail
# ──────────────────────────────────────────────────────────────
# Update mihomo (Clash Meta) config on Tencent Cloud server
# from a Clash subscription URL.
#
# Usage:
#   bash 03_Scripts/deploy/update_mihomo_subscription.sh [SUBSCRIPTION_URL]
#
# If SUBSCRIPTION_URL is omitted, uses MIHOMO_SUB_URL env var or a 0dcloud
# profile URL discovered on the target host. Set MIHOMO_DB_PATH to override the
# 0dcloud sqlite path when it is not under the default user home.
# ──────────────────────────────────────────────────────────────

SSH_HOST="${SSH_HOST:-tencent-cloud}"
MIHOMO_CONFIG_DIR="/etc/mihomo"
MIHOMO_LOCAL="${MIHOMO_LOCAL:-false}"
MIHOMO_DRY_RUN="${MIHOMO_DRY_RUN:-false}"

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
if [[ -n "$SUB_URL" ]]; then
  SUB_URLS+=("$SUB_URL")
else
  while IFS= read -r candidate_url; do
    if [[ -n "$candidate_url" ]]; then
      SUB_URLS+=("$candidate_url")
    fi
  done < <(discover_remote_subscription_url)
fi

if [[ "${#SUB_URLS[@]}" -eq 0 ]]; then
  echo "[mihomo-sub] ERROR: no subscription URL configured. Set MIHOMO_SUB_URL, pass SUBSCRIPTION_URL, or set MIHOMO_DB_PATH to a 0dcloud database with a saved URL." >&2
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

TMP_CONF=""
FETCH_OK=false
for candidate_url in "${SUB_URLS[@]}"; do
  log_subscription_url "$candidate_url"
  TMP_CONF="$(mktemp)"
  CURL_RC=0
  HTTP_CODE=$(curl -sL -w "%{http_code}" --connect-timeout 15 --max-time 60 -o "$TMP_CONF" "$candidate_url") || CURL_RC=$?

  if [[ "$CURL_RC" -ne 0 || "$HTTP_CODE" != "200" ]]; then
    echo "[mihomo-sub] WARN: curl_rc=$CURL_RC HTTP $HTTP_CODE — subscription candidate unavailable" >&2
    rm -f "$TMP_CONF"
    TMP_CONF=""
    continue
  fi

  # Validate it looks like a YAML Clash config. Some managed subscriptions start
  # with comment headers such as #!MANAGED-CONFIG, so scan beyond the first lines.
  if ! grep -qE '^(port:|mixed-port:|proxies:|proxy-groups:)' "$TMP_CONF"; then
    echo "[mihomo-sub] WARN: subscription candidate does not look like a Clash YAML config" >&2
    head -20 "$TMP_CONF" >&2
    rm -f "$TMP_CONF"
    TMP_CONF=""
    continue
  fi

  FETCH_OK=true
  break
done

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

if [[ "$MIHOMO_DRY_RUN" == "true" ]]; then
  echo "[mihomo-sub] Dry run complete; config was not installed"
  rm -f "$TMP_CONF" 2>/dev/null || true
  exit 0
fi

if [[ "$MIHOMO_LOCAL" == "true" ]]; then
  echo "[mihomo-sub] Installing local config..."
  sudo cp "$MIHOMO_CONFIG_DIR/config.yaml" "$MIHOMO_CONFIG_DIR/config.yaml.bak" 2>/dev/null || true
  sudo mv "$TMP_CONF" "$MIHOMO_CONFIG_DIR/config.yaml"
  sudo chmod 600 "$MIHOMO_CONFIG_DIR/config.yaml"
  timeout 45s sudo systemctl restart mihomo.service \
    || timeout 45s sudo systemctl restart mihomo \
    || true
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
if [[ "$MIHOMO_LOCAL" == "true" ]]; then
  curl -sL --connect-timeout 10 --max-time 30 -x http://127.0.0.1:7897 \
    -o /dev/null -w 'HTTP:%{http_code}\n' https://news.google.com/
else
  ssh "$SSH_HOST" "curl -sL --connect-timeout 10 --max-time 30 -x http://127.0.0.1:7897 -o /dev/null -w 'HTTP:%{http_code}\n' https://news.google.com/"
fi

rm -f "$TMP_CONF" 2>/dev/null || true
echo "[mihomo-sub] Done."
