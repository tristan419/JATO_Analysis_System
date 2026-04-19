#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GITNEXUS_SOURCE_DIR="$ROOT_DIR/08_GitNexus"
GITNEXUS_CLI_DIR="$GITNEXUS_SOURCE_DIR/gitnexus"
GITNEXUS_CLI_DIST="$GITNEXUS_CLI_DIR/dist/cli/index.js"
COMMAND="${1:-status}"

if [[ -n "${1:-}" ]]; then
  shift
fi

if [[ ! -d "$GITNEXUS_SOURCE_DIR" ]]; then
  echo "GitNexus source directory not found: $GITNEXUS_SOURCE_DIR" >&2
  exit 1
fi

print_usage() {
  cat <<EOF
Usage: 03_Scripts/run_gitnexus.sh <command> [args...]

Source checkout:
  $GITNEXUS_SOURCE_DIR

Supported commands:
  analyze   Safely index the current JATO repository (skips AGENTS/CLAUDE updates)
  analyze-full  Full analyze with GitNexus default side effects
  status    Show GitNexus index status for the current repository
  clean     Remove the current repository index
  wiki      Generate repository wiki output
  serve     Start the local GitNexus HTTP server
  visualize Safely analyze this repo, then start the local HTTP server for Web UI
  mcp       Start the GitNexus MCP server
  list      List indexed repositories
  source    Print the local GitNexus source checkout path

Notes:
  - 'analyze' uses: --skip-agents-md --no-stats
  - This wrapper uses the published gitnexus CLI via npx.
  - GitNexus OSS is licensed under PolyForm Noncommercial; confirm your use fits.
  - 'setup' is intentionally not automated here because it edits global editor MCP config.
EOF
}

run_repo_scoped() {
  cd "$ROOT_DIR"
  if [[ -f "$GITNEXUS_CLI_DIST" ]]; then
    exec node "$GITNEXUS_CLI_DIST" "$@"
  fi
  exec npx -y gitnexus@latest "$@"
}

run_global_cli() {
  if [[ -f "$GITNEXUS_CLI_DIST" ]]; then
    cd "$GITNEXUS_CLI_DIR"
    exec node "$GITNEXUS_CLI_DIST" "$@"
  fi
  exec npx -y gitnexus@latest "$@"
}

serve_with_ipv4_default() {
  local -a args=()
  local has_host=false

  if [[ "$#" -gt 0 ]]; then
    args=("$@")
  fi

  if [[ "${#args[@]}" -gt 0 ]]; then
    for arg in "${args[@]}"; do
      if [[ "$arg" == "--host" ]] || [[ "$arg" == --host=* ]]; then
        has_host=true
        break
      fi
    done
  fi

  if [[ "$has_host" == false ]]; then
    args+=(--host 127.0.0.1)
  fi

  run_global_cli serve "${args[@]}"
}

case "$COMMAND" in
  analyze)
    run_repo_scoped analyze "$ROOT_DIR" --skip-agents-md --no-stats "$@"
    ;;
  analyze-full)
    run_repo_scoped analyze "$ROOT_DIR" "$@"
    ;;
  status|clean|wiki)
    run_repo_scoped "$COMMAND" "$@"
    ;;
  serve)
    serve_with_ipv4_default "$@"
    ;;
  mcp|list)
    run_global_cli "$COMMAND" "$@"
    ;;
  visualize)
    cd "$ROOT_DIR"
    if [[ -f "$GITNEXUS_CLI_DIST" ]]; then
      node "$GITNEXUS_CLI_DIST" analyze "$ROOT_DIR" --skip-agents-md --no-stats "$@"
    else
      npx -y gitnexus@latest analyze "$ROOT_DIR" --skip-agents-md --no-stats "$@"
    fi
    echo
    echo "GitNexus local backend is ready to start."
    echo "Open https://gitnexus.vercel.app after the server comes up."
    serve_with_ipv4_default
    ;;
  source)
    printf '%s\n' "$GITNEXUS_SOURCE_DIR"
    ;;
  setup)
    cat >&2 <<EOF
GitNexus setup writes global MCP editor configuration.
Run it manually if you want that behavior:
  npx -y gitnexus@latest setup
EOF
    exit 2
    ;;
  help|-h|--help)
    print_usage
    ;;
  *)
    echo "Unsupported GitNexus command: $COMMAND" >&2
    print_usage >&2
    exit 2
    ;;
esac
