#!/usr/bin/env bash
set -u

MSRP_ENV_FILE="${MSRP_ENV_FILE:-/etc/jato-fullstack/msrp.env}"

python3 - "$MSRP_ENV_FILE" <<'PY' 2>&1 || true
from pathlib import Path
import sys

path = Path(sys.argv[1])
allow = {
    "JATO_MSRP_MODE",
    "JATO_MSRP_COUNTRIES",
    "JATO_MSRP_CONCURRENCY",
    "JATO_MSRP_PAUSE_SECONDS",
    "JATO_MSRP_STOP_ON_FAILURE",
    "JATO_MSRP_COUNTRY_TIMEOUT_SECONDS",
    "JATO_MSRP_MIN_DRYRUN_PASS_PCT",
    "JATO_STRICT_EXIT",
}
proxy_keys = {"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"}

if not path.is_file():
    print(f"missing {path}")
else:
    proxy_configured = False
    for line in path.read_text(errors="replace").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        if key in allow:
            print(f"{key}={value.strip()}")
        if key in proxy_keys:
            proxy_configured = True
    print(f"proxy_configured={str(proxy_configured).lower()}")
PY
