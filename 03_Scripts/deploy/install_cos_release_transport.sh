#!/usr/bin/env bash
set -euo pipefail

COSCLI_VERSION="v1.0.8"
DESTINATION="${COSCLI_BIN:-/usr/local/bin/coscli}"
TENCENT_DOWNLOAD_BASE="https://cosbrowser.cloud.tencent.com/software/coscli"
GITHUB_DOWNLOAD_BASE="https://github.com/tencentyun/coscli/releases/download/${COSCLI_VERSION}"

case "$(uname -m)" in
  x86_64|amd64)
    platform="linux-amd64"
    expected_sha256="7165f2ae16c5f7ac495864c963ca574a76e04ec72680d7bc8a8eee3234d8cf91"
    ;;
  aarch64|arm64)
    platform="linux-arm64"
    expected_sha256="0404b4da5b1d0c230c7d7522cb3bbec2909e314ab998889a0aeb8dc6094a2d21"
    ;;
  *)
    echo "[ERROR] Unsupported server architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

download="$(mktemp /tmp/coscli-install.XXXXXX)"
cleanup() {
  rm -f "$download"
}
trap cleanup EXIT

verified_download="false"
for url in \
  "$TENCENT_DOWNLOAD_BASE/coscli-$platform" \
  "$GITHUB_DOWNLOAD_BASE/coscli-${COSCLI_VERSION}-$platform"
do
  if curl --fail --show-error --silent --location \
    --proto '=https' --tlsv1.2 \
    "$url" \
    --output "$download"; then
    actual_sha256="$(sha256sum "$download" | awk '{print $1}')"
    if [ "$actual_sha256" = "$expected_sha256" ]; then
      verified_download="true"
      break
    fi
    echo "[WARN] COSCLI mirror checksum mismatch; trying the next pinned source" >&2
  else
    echo "[WARN] COSCLI mirror unavailable; trying the next pinned source" >&2
  fi
done
if [ "$verified_download" != "true" ]; then
  echo "[ERROR] No COSCLI source matched the pinned SHA-256" >&2
  exit 1
fi

sudo install -o root -g root -m 0755 "$download" "$DESTINATION"
if ! "$DESTINATION" --version 2>&1 | grep -Fq "$COSCLI_VERSION"; then
  echo "[ERROR] Installed COSCLI version check failed" >&2
  exit 1
fi
echo "[INFO] Installed pinned COSCLI $COSCLI_VERSION at $DESTINATION"
