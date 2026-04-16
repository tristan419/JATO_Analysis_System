#!/usr/bin/env bash
set -euo pipefail

# Install 0dcloud on Ubuntu 22.04+ (headless/server-safe script)
# Usage:
#   bash 03_Scripts/deploy/install_0dcloud_server.sh /path/to/0dcloud_xxx.deb

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /path/to/0dcloud_xxx.deb" >&2
  exit 1
fi

DEB_PATH="$1"

if [[ ! -f "$DEB_PATH" ]]; then
  echo "[0dcloud] .deb file not found: $DEB_PATH" >&2
  exit 1
fi

if [[ ! -r "$DEB_PATH" ]]; then
  echo "[0dcloud] .deb file is not readable: $DEB_PATH" >&2
  exit 1
fi

if ! command -v dpkg >/dev/null 2>&1; then
  echo "[0dcloud] dpkg not found. This script is for Debian/Ubuntu only." >&2
  exit 1
fi

OS_ID="$(. /etc/os-release && echo "$ID")"
OS_VER="$(. /etc/os-release && echo "$VERSION_ID")"
if [[ "$OS_ID" != "ubuntu" ]]; then
  echo "[0dcloud] Warning: detected OS=$OS_ID, expected ubuntu." >&2
fi

echo "[0dcloud] Installing package: $DEB_PATH"
sudo dpkg -i "$DEB_PATH" || true

# Repair dependencies if needed
echo "[0dcloud] Fixing dependencies if required"
sudo apt-get update -y
sudo apt-get install -f -y

if command -v 0dcloud >/dev/null 2>&1; then
  echo "[0dcloud] Installed successfully: $(command -v 0dcloud)"
  0dcloud --help >/dev/null 2>&1 || true
else
  echo "[0dcloud] Install completed but executable '0dcloud' not found in PATH." >&2
  echo "[0dcloud] Try: dpkg -L 0dcloud | head -50" >&2
  exit 1
fi

echo "[0dcloud] Done."
