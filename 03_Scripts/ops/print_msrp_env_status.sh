#!/usr/bin/env bash
set -u

MSRP_ENV_FILE="${MSRP_ENV_FILE:-/etc/jato-fullstack/msrp.env}"

python3 - "$MSRP_ENV_FILE" <<'PY' 2>&1 || true
from pathlib import Path
import subprocess
import sys

path = Path(sys.argv[1])
allow = {
    "JATO_MSRP_MODE",
    "JATO_MSRP_COUNTRIES",
    "JATO_MSRP_CONCURRENCY",
    "JATO_MSRP_MAX_DRYRUN_CONCURRENCY",
    "JATO_MSRP_ALLOW_HIGH_CONCURRENCY",
    "JATO_MSRP_PAUSE_SECONDS",
    "JATO_MSRP_STOP_ON_FAILURE",
    "JATO_MSRP_COUNTRY_TIMEOUT_SECONDS",
    "JATO_MSRP_MIN_DRYRUN_PASS_PCT",
    "JATO_STRICT_EXIT",
}
proxy_keys = {"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"}

def _read_env_file(env_path: Path) -> str | None:
    try:
        return env_path.read_text(errors="replace")
    except PermissionError:
        proc = subprocess.run(
            ["sudo", "-n", "cat", str(env_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return proc.stdout
        print(f"permission_denied {env_path}")
        return None
    except OSError as exc:
        print(f"read_failed {env_path}: {exc}")
        return None


if not path.is_file():
    print(f"missing {path}")
else:
    text = _read_env_file(path)
    if text is not None:
        proxy_configured = False
        for line in text.splitlines():
            item = line.strip()
            if not item or item.startswith("#") or "=" not in item:
                continue
            key, value = item.split("=", 1)
            key = key.strip()
            if key.startswith("export "):
                key = key.removeprefix("export ").strip()
            if key in allow:
                print(f"{key}={value.strip()}")
            if key in proxy_keys:
                proxy_configured = True
        print(f"proxy_configured={str(proxy_configured).lower()}")
PY
