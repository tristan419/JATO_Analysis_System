#!/usr/bin/env bash

set -euo pipefail

service="${JATO_NVIDIA_KEYCHAIN_SERVICE:-jato.nvidia.nvapi}"
account="${JATO_NVIDIA_KEYCHAIN_ACCOUNT:-$USER}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--service SERVICE] [--account ACCOUNT]

Stores NVIDIA_API_KEY in the macOS Keychain using a secure interactive prompt.

Environment overrides:
  JATO_NVIDIA_KEYCHAIN_SERVICE   Keychain service name
  JATO_NVIDIA_KEYCHAIN_ACCOUNT   Keychain account name

Examples:
  $(basename "$0")
  $(basename "$0") --service jato.nvidia.nvapi --account "$USER"
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --service" >&2
        usage >&2
        exit 1
      fi
      service="$2"
      shift 2
      ;;
    --account)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --account" >&2
        usage >&2
        exit 1
      fi
      account="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

echo "macOS Keychain will prompt for the NVIDIA API key for service '$service' and account '$account'."

security add-generic-password \
  -a "$account" \
  -s "$service" \
  -D "application password" \
  -j "JATO Analysis System NVIDIA provider key" \
  -U \
  -w

security find-generic-password -a "$account" -s "$service" >/dev/null

echo "NVIDIA API key stored in macOS Keychain service '$service' for account '$account'."
echo "Load it into the shell with: source 03_Scripts/load_nvidia_api_key.sh"