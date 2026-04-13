#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
RUNNER_SCRIPT="$REPO_DIR/03_Scripts/schedule_msrp_sync_to_cloud.sh"
LABEL="${JATO_MSRP_SYNC_LABEL:-com.jato.msrp-db-sync}"
PLIST_DIR="${JATO_MSRP_SYNC_PLIST_DIR:-$HOME/Library/LaunchAgents}"
PLIST_PATH="$PLIST_DIR/$LABEL.plist"
LOG_DIR="${JATO_MSRP_SYNC_LOG_DIR:-$REPO_DIR/03_Scripts/logs}"
AGENT_LOG_DIR="${JATO_MSRP_LAUNCHD_AGENT_LOG_DIR:-$HOME/Library/Logs/JATO_Analysis_System}"
HOUR="${1:-3}"
MINUTE="${2:-20}"
SKIP_LAUNCHCTL="${JATO_MSRP_SYNC_SKIP_LAUNCHCTL:-false}"
ALERT_EMAIL="${JATO_MSRP_ALERT_EMAIL:-}"
ALERT_NOTIFY="${JATO_MSRP_ALERT_NOTIFY:-true}"
ALERT_MAIL_BIN="${JATO_MSRP_ALERT_MAIL_BIN:-/usr/bin/mail}"
LAUNCH_MODE="${JATO_MSRP_LAUNCHD_MODE:-auto}"

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

validate_number_range "$HOUR" 0 23 Hour
validate_number_range "$MINUTE" 0 59 Minute

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
      echo "[ERROR] Unsupported JATO_MSRP_LAUNCHD_MODE=$LAUNCH_MODE. Use auto, direct, or terminal-bridge." >&2
      exit 1
      ;;
  esac
}

LAUNCH_MODE="$(resolve_launch_mode)"

if [[ ! -f "$RUNNER_SCRIPT" ]]; then
  echo "[ERROR] Runner script not found: $RUNNER_SCRIPT" >&2
  exit 1
fi

mkdir -p "$PLIST_DIR" "$LOG_DIR" "$AGENT_LOG_DIR"

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
    <key>JATO_MSRP_ALERT_EMAIL</key>
    <string>$ALERT_EMAIL</string>
    <key>JATO_MSRP_ALERT_NOTIFY</key>
    <string>$ALERT_NOTIFY</string>
    <key>JATO_MSRP_ALERT_MAIL_BIN</key>
    <string>$ALERT_MAIL_BIN</string>
    <key>PATH</key>
    <string>/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key>
  <string>$AGENT_LOG_DIR/msrp-db-sync-launchd.out.log</string>
  <key>StandardErrorPath</key>
  <string>$AGENT_LOG_DIR/msrp-db-sync-launchd.err.log</string>
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
echo "[INFO] Runner log dir: $LOG_DIR"
echo "[INFO] LaunchAgent log dir: $AGENT_LOG_DIR"
echo "[INFO] Alert email: ${ALERT_EMAIL:-<disabled>}"
echo "[INFO] Launch mode: $LAUNCH_MODE"

if [[ "$LAUNCH_MODE" == terminal-bridge ]]; then
  echo "[INFO] Terminal bridge is enabled because the repo lives under Downloads."
  echo "[INFO] At run time, macOS may open a Terminal tab/window to execute the sync."
fi

if is_truthy "$SKIP_LAUNCHCTL"; then
  echo "[INFO] Skipped launchctl bootstrap because JATO_MSRP_SYNC_SKIP_LAUNCHCTL=$SKIP_LAUNCHCTL"
else
  echo "[INFO] launchd job loaded: gui/$(id -u)/$LABEL"
  echo "[INFO] Run now: launchctl kickstart -k gui/$(id -u)/$LABEL"
fi