#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
RUNNER_SCRIPT="$REPO_DIR/03_Scripts/news/run_country_news_sync.sh"
LABEL="${JATO_COUNTRY_NEWS_SYNC_LABEL:-com.jato.country-news-sync}"
PLIST_DIR="${JATO_COUNTRY_NEWS_PLIST_DIR:-$HOME/Library/LaunchAgents}"
PLIST_PATH="$PLIST_DIR/$LABEL.plist"
AGENT_LOG_DIR="${JATO_COUNTRY_NEWS_AGENT_LOG_DIR:-$HOME/Library/Logs/JATO_Analysis_System}"
HOUR="${1:-6}"
MINUTE="${2:-15}"
SKIP_LAUNCHCTL="${JATO_COUNTRY_NEWS_SKIP_LAUNCHCTL:-false}"
LAUNCH_MODE="${JATO_COUNTRY_NEWS_LAUNCHD_MODE:-auto}"

is_truthy() {
  local normalized=""
  normalized="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$normalized" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

validate_number_range() {
  local value="$1"
  local lower="$2"
  local upper="$3"
  local label="$4"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] $label must be an integer, got: $value" >&2
    exit 1
  fi

  if (( value < lower || value > upper )); then
    echo "[ERROR] $label must be between $lower and $upper, got: $value" >&2
    exit 1
  fi
}

resolve_launch_mode() {
  case "$LAUNCH_MODE" in
    auto)
      if [[ "$REPO_DIR" == "$HOME/Downloads" || "$REPO_DIR" == "$HOME/Downloads/"* ]]; then
        printf '%s\n' terminal-bridge
      else
        printf '%s\n' direct
      fi
      ;;
    direct|terminal-bridge)
      printf '%s\n' "$LAUNCH_MODE"
      ;;
    *)
      echo "[ERROR] Unsupported JATO_COUNTRY_NEWS_LAUNCHD_MODE=$LAUNCH_MODE" >&2
      exit 1
      ;;
  esac
}

validate_number_range "$HOUR" 0 23 Hour
validate_number_range "$MINUTE" 0 59 Minute
LAUNCH_MODE="$(resolve_launch_mode)"

if [[ ! -f "$RUNNER_SCRIPT" ]]; then
  echo "[ERROR] Runner script not found: $RUNNER_SCRIPT" >&2
  exit 1
fi

mkdir -p "$PLIST_DIR" "$AGENT_LOG_DIR"

WORKING_DIRECTORY="$REPO_DIR"
PROGRAM_ARGUMENTS_XML=$(cat <<EOF
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$RUNNER_SCRIPT</string>
  </array>
EOF
)

if [[ "$LAUNCH_MODE" == terminal-bridge ]]; then
  quoted_repo_dir="$(printf '%q' "$REPO_DIR")"
  quoted_runner_script="$(printf '%q' "$RUNNER_SCRIPT")"
  terminal_command='cd '"$quoted_repo_dir"' || exit 1; /bin/bash '"$quoted_runner_script"'; exit $?'
  WORKING_DIRECTORY="$HOME"
  PROGRAM_ARGUMENTS_XML=$(cat <<EOF
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/osascript</string>
    <string>-e</string>
    <string>tell application "Terminal" to do script "$terminal_command"</string>
  </array>
EOF
)
fi

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
$PROGRAM_ARGUMENTS_XML
  <key>WorkingDirectory</key>
  <string>$WORKING_DIRECTORY</string>
  <key>RunAtLoad</key>
  <false/>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>$HOUR</integer>
    <key>Minute</key>
    <integer>$MINUTE</integer>
  </dict>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>$HOME</string>
    <key>PATH</key>
    <string>/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>JATO_COUNTRY_NEWS_ALERT_NOTIFY</key>
    <string>true</string>
  </dict>
  <key>StandardOutPath</key>
  <string>$AGENT_LOG_DIR/country-news-sync-launchd.out.log</string>
  <key>StandardErrorPath</key>
  <string>$AGENT_LOG_DIR/country-news-sync-launchd.err.log</string>
</dict>
</plist>
EOF

plutil -lint "$PLIST_PATH"

if ! is_truthy "$SKIP_LAUNCHCTL"; then
  launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
  launchctl enable "gui/$(id -u)/$LABEL"
fi

echo "[INFO] launchd plist written: $PLIST_PATH"
echo "[INFO] Schedule: every day at $(printf '%02d:%02d' "$HOUR" "$MINUTE")"
echo "[INFO] Runner: $RUNNER_SCRIPT"
echo "[INFO] LaunchAgent log dir: $AGENT_LOG_DIR"
echo "[INFO] Launch mode: $LAUNCH_MODE"

if is_truthy "$SKIP_LAUNCHCTL"; then
  echo "[INFO] Skipped launchctl bootstrap because JATO_COUNTRY_NEWS_SKIP_LAUNCHCTL=$SKIP_LAUNCHCTL"
else
  echo "[INFO] launchd job loaded: gui/$(id -u)/$LABEL"
  echo "[INFO] Run now: launchctl kickstart -k gui/$(id -u)/$LABEL"
fi
