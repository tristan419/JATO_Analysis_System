#!/usr/bin/env bash
set -Eeuo pipefail

DEFAULT_HTTP_PROXY_URL="${GIT_PROXY_HTTP_URL:-http://127.0.0.1:7897}"
DEFAULT_HTTPS_PROXY_URL="${GIT_PROXY_HTTPS_URL:-${DEFAULT_HTTP_PROXY_URL}}"
DEFAULT_TEST_REMOTE_URL="${GIT_PROXY_TEST_REMOTE_URL:-https://github.com/tristan419/JATO_Analysis_System.git}"

usage() {
  cat <<EOF
Usage:
  bash 03_Scripts/git_proxy.sh on
  bash 03_Scripts/git_proxy.sh off
  bash 03_Scripts/git_proxy.sh status
  bash 03_Scripts/git_proxy.sh test

Optional overrides:
  --http URL     Override http.proxy value
  --https URL    Override https.proxy value
  --remote URL   Override test remote URL

Examples:
  bash 03_Scripts/git_proxy.sh on
  bash 03_Scripts/git_proxy.sh on --http http://127.0.0.1:7890 --https http://127.0.0.1:7890
  bash 03_Scripts/git_proxy.sh test
  bash 03_Scripts/git_proxy.sh off
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

ACTION="$1"
shift

HTTP_PROXY_URL="$DEFAULT_HTTP_PROXY_URL"
HTTPS_PROXY_URL="$DEFAULT_HTTPS_PROXY_URL"
TEST_REMOTE_URL="$DEFAULT_TEST_REMOTE_URL"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --http)
      HTTP_PROXY_URL="${2:-}"
      shift 2
      ;;
    --https)
      HTTPS_PROXY_URL="${2:-}"
      shift 2
      ;;
    --remote)
      TEST_REMOTE_URL="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

print_status() {
  local current_http current_https
  current_http="$(git config --global --get http.proxy || true)"
  current_https="$(git config --global --get https.proxy || true)"

  echo "Git global proxy status"
  echo "  http.proxy: ${current_http:-<unset>}"
  echo "  https.proxy: ${current_https:-<unset>}"
}

test_connectivity() {
  echo "Testing remote reachability via Git: $TEST_REMOTE_URL"
  if git ls-remote "$TEST_REMOTE_URL" HEAD >/dev/null 2>&1; then
    echo "  OK: Git can reach the remote with current global config."
  else
    echo "  FAIL: Git still cannot reach the remote." >&2
    return 1
  fi
}

case "$ACTION" in
  on)
    git config --global http.proxy "$HTTP_PROXY_URL"
    git config --global https.proxy "$HTTPS_PROXY_URL"
    echo "Enabled Git global proxy."
    print_status
    test_connectivity
    ;;
  off)
    git config --global --unset-all http.proxy >/dev/null 2>&1 || true
    git config --global --unset-all https.proxy >/dev/null 2>&1 || true
    echo "Disabled Git global proxy."
    print_status
    ;;
  status)
    print_status
    ;;
  test)
    print_status
    test_connectivity
    ;;
  *)
    echo "Unknown action: $ACTION" >&2
    usage
    exit 1
    ;;
esac