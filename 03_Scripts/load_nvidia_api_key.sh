#!/usr/bin/env bash

set -euo pipefail

service="${JATO_NVIDIA_KEYCHAIN_SERVICE:-jato.nvidia.nvapi}"
account="${JATO_NVIDIA_KEYCHAIN_ACCOUNT:-$USER}"
store_script="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/store_nvidia_api_key.sh"

if [[ -z "${NVIDIA_API_KEY:-}" ]]; then
  key="$(security find-generic-password -a "$account" -s "$service" -w 2>/dev/null || true)"
  if [[ -z "$key" ]]; then
    echo "NVIDIA API key not found in macOS Keychain service '$service' for account '$account'." >&2
    echo "Create it first with: $store_script" >&2
    exit 1
  fi
  export NVIDIA_API_KEY="$key"
fi

export NVAPI_KEY="${NVAPI_KEY:-$NVIDIA_API_KEY}"
echo "NVIDIA API key loaded from macOS Keychain service '$service'."