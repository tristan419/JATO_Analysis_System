#!/usr/bin/env bash
set -euo pipefail
# ──────────────────────────────────────────────────────────────
# Update mihomo (Clash Meta) config on Tencent Cloud server
# from a Clash subscription URL.
#
# Usage:
#   bash 03_Scripts/deploy/update_mihomo_subscription.sh [SUBSCRIPTION_URL]
#
# If SUBSCRIPTION_URL is omitted, uses MIHOMO_SUB_URL env var, the latest
# remote 0dcloud profile URL, or the default subscription below.
# ──────────────────────────────────────────────────────────────

SSH_HOST="${SSH_HOST:-tencent-cloud}"
MIHOMO_CONFIG_DIR="/etc/mihomo"
DEFAULT_MIHOMO_SUB_URL="${DEFAULT_MIHOMO_SUB_URL:-https://naikosub.com/link/LPFT7gIKqNavBKoe?clash=1}"
MIHOMO_LOCAL="${MIHOMO_LOCAL:-false}"

discover_remote_subscription_url() {
  if [[ "$MIHOMO_LOCAL" == "true" ]]; then
    python3 - <<"PY"
import sqlite3
from pathlib import Path

db_path = Path("/home/ubuntu/.local/share/0dcloud/database.sqlite")
if not db_path.exists():
    raise SystemExit(0)

conn = sqlite3.connect(str(db_path))
cur = conn.cursor()
query = """
SELECT url, last_update_date, id
FROM profiles
WHERE url IS NOT NULL AND TRIM(url) != ?
ORDER BY last_update_date DESC, id DESC
LIMIT 1
"""
rows = list(cur.execute(query, ("",)))
if rows:
    print(rows[0][0].strip())
PY
    return
  fi

  ssh "$SSH_HOST" 'python3 - <<"PY"
import sqlite3
from pathlib import Path

db_path = Path("/home/ubuntu/.local/share/0dcloud/database.sqlite")
if not db_path.exists():
    raise SystemExit(0)

conn = sqlite3.connect(str(db_path))
cur = conn.cursor()
query = """
SELECT url, last_update_date, id
FROM profiles
WHERE url IS NOT NULL AND TRIM(url) != ?
ORDER BY last_update_date DESC, id DESC
LIMIT 1
"""
rows = list(cur.execute(query, ("",)))
if rows:
    print(rows[0][0].strip())
PY'
}

SUB_URL="${1:-${MIHOMO_SUB_URL:-}}"
if [[ -z "$SUB_URL" ]]; then
  SUB_URL="$(discover_remote_subscription_url)"
fi
if [[ -z "$SUB_URL" ]]; then
  SUB_URL="$DEFAULT_MIHOMO_SUB_URL"
fi

if [[ -z "$SUB_URL" ]]; then
  echo "[mihomo-sub] ERROR: no subscription URL provided and no remote 0dcloud profile URL could be discovered" >&2
  exit 1
fi

echo "[mihomo-sub] Fetching subscription: $SUB_URL"
TMP_CONF="$(mktemp)"
HTTP_CODE=$(curl -sL -w "%{http_code}" --connect-timeout 15 --max-time 60 -o "$TMP_CONF" "$SUB_URL")

if [[ "$HTTP_CODE" != "200" ]]; then
  echo "[mihomo-sub] ERROR: HTTP $HTTP_CODE — subscription unavailable" >&2
  rm -f "$TMP_CONF"
  exit 1
fi

# Validate it looks like a YAML Clash config
if ! head -5 "$TMP_CONF" | grep -qE '^(port:|mixed-port:|proxies:)'; then
  echo "[mihomo-sub] ERROR: Response does not look like a Clash YAML config" >&2
  head -5 "$TMP_CONF" >&2
  rm -f "$TMP_CONF"
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
  curl -sL --connect-timeout 10 -x http://127.0.0.1:7897 \
    --max-time 30 \
    -o /dev/null -w 'HTTP:%{http_code}\n' https://news.google.com/
else
  ssh "$SSH_HOST" "curl -sL --connect-timeout 10 --max-time 30 -x http://127.0.0.1:7897 -o /dev/null -w 'HTTP:%{http_code}\n' https://news.google.com/"
fi

rm -f "$TMP_CONF" 2>/dev/null || true
echo "[mihomo-sub] Done."
