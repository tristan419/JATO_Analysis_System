#!/usr/bin/env bash
set -euo pipefail

# One-click publish script for GHCR.
# Usage:
#   bash 03_Scripts/deploy/docker/publish_ghcr.sh 1.0.0
#   GHCR_OWNER=my-org GHCR_TOKEN=xxxx bash 03_Scripts/deploy/docker/publish_ghcr.sh v1.0.0 jato-dashboard
#
# Env vars:
#   GHCR_OWNER   Optional. GitHub org/user for image namespace.
#   GHCR_USER    Optional. Username for docker login (default: GHCR_OWNER).
#   GHCR_TOKEN   Optional. Personal Access Token with package write permission.

usage() {
  echo "Usage: bash 03_Scripts/deploy/docker/publish_ghcr.sh <version> [image_name]"
  echo "Example: bash 03_Scripts/deploy/docker/publish_ghcr.sh 1.0.0 jato-dashboard"
}

if [[ "${1:-}" == "" ]]; then
  usage
  exit 1
fi

VERSION_RAW="$1"
IMAGE_NAME="${2:-jato-dashboard}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

if ! command -v docker >/dev/null 2>&1; then
  echo "[ERROR] docker command not found. Install Docker Desktop / Docker Engine first."
  exit 1
fi

infer_owner_from_git() {
  local remote_url
  remote_url="$(git -C "$ROOT_DIR" config --get remote.origin.url || true)"

  if [[ -z "$remote_url" ]]; then
    echo ""
    return
  fi

  if [[ "$remote_url" =~ github.com[:/]([^/]+)/([^/.]+)(.git)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return
  fi

  echo ""
}

GHCR_OWNER="${GHCR_OWNER:-$(infer_owner_from_git)}"
if [[ -z "$GHCR_OWNER" ]]; then
  echo "[ERROR] Unable to infer GHCR owner. Set GHCR_OWNER env manually."
  echo "        Example: GHCR_OWNER=litristan bash 03_Scripts/deploy/docker/publish_ghcr.sh 1.0.0"
  exit 1
fi

GHCR_USER="${GHCR_USER:-$GHCR_OWNER}"
if [[ "$VERSION_RAW" =~ ^v ]]; then
  TAG_VERSION="$VERSION_RAW"
else
  TAG_VERSION="v${VERSION_RAW}"
fi

IMAGE_URI="ghcr.io/${GHCR_OWNER}/${IMAGE_NAME}"

echo "[INFO] Project root: ${ROOT_DIR}"
echo "[INFO] Publish image: ${IMAGE_URI}:${TAG_VERSION}"

if [[ -n "${GHCR_TOKEN:-}" ]]; then
  echo "[INFO] Logging in to GHCR with GHCR_TOKEN"
  printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
elif command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
  echo "[INFO] Logging in to GHCR using gh auth token"
  gh auth token | docker login ghcr.io -u "$GHCR_USER" --password-stdin
else
  echo "[WARN] GHCR_TOKEN not set and gh auth unavailable."
  echo "[WARN] Will continue and rely on existing 'docker login ghcr.io' session."
fi

cd "$ROOT_DIR"

echo "[INFO] Building image tags"
docker build \
  --network=host \
  -t "${IMAGE_URI}:${TAG_VERSION}" \
  -t "${IMAGE_URI}:latest" \
  .

echo "[INFO] Pushing ${IMAGE_URI}:${TAG_VERSION}"
docker push "${IMAGE_URI}:${TAG_VERSION}"

echo "[INFO] Pushing ${IMAGE_URI}:latest"
docker push "${IMAGE_URI}:latest"

echo ""
echo "[DONE] GHCR publish completed"
echo "Next step for users:"
echo "1) Set JATO_IMAGE=${IMAGE_URI}:${TAG_VERSION} in .env"
echo "2) Run: docker compose pull && docker compose up -d"
