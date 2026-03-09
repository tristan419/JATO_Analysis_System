#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo bash bootstrap_ubuntu.sh /opt/JATO_Analysis_System
# If arg omitted, defaults to /opt/JATO_Analysis_System

APP_DIR="${1:-/opt/JATO_Analysis_System}"

if [[ ! -d "$APP_DIR" ]]; then
  echo "[ERROR] APP_DIR not found: $APP_DIR"
  exit 1
fi

cd "$APP_DIR"

echo "[INFO] apt update"
sudo apt-get update -y

echo "[INFO] install runtime packages"
sudo apt-get install -y python3 python3-venv nginx git

PYTHON_BIN="python3"
if command -v python3.12 >/dev/null 2>&1; then
  PYTHON_BIN="python3.12"
fi

if [[ ! -d ".venv" ]]; then
  echo "[INFO] create virtualenv"
  "$PYTHON_BIN" -m venv .venv
fi

echo "[INFO] install python deps"
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "[INFO] install systemd unit template"
sudo cp 03_Scripts/deploy/systemd/jato-dashboard@.service /etc/systemd/system/
sudo systemctl daemon-reload

# Replace default path in unit file if APP_DIR is custom.
if [[ "$APP_DIR" != "/opt/JATO_Analysis_System" ]]; then
  sudo sed -i "s#/opt/JATO_Analysis_System#$APP_DIR#g" /etc/systemd/system/jato-dashboard@.service
  sudo systemctl daemon-reload
fi

echo "[INFO] start streamlit instances"
sudo systemctl enable --now jato-dashboard@8501
sudo systemctl enable --now jato-dashboard@8502

echo "[INFO] install nginx config"
sudo cp 03_Scripts/deploy/nginx/jato_dashboard.conf.example /etc/nginx/conf.d/jato_dashboard.conf
sudo nginx -t
sudo systemctl enable --now nginx
sudo systemctl reload nginx

echo "[INFO] done"
sudo systemctl --no-pager --full status jato-dashboard@8501 | head -n 20
sudo systemctl --no-pager --full status jato-dashboard@8502 | head -n 20
curl -sS http://127.0.0.1/healthz || true
