#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo bash bootstrap_ubuntu.sh /opt/JATO_Analysis_System
#   sudo JATO_ENABLE_SECONDARY_INSTANCE=true bash bootstrap_ubuntu.sh /opt/JATO_Analysis_System
# If arg omitted, defaults to /opt/JATO_Analysis_System

APP_DIR="${1:-/opt/JATO_Analysis_System}"
ENABLE_SECONDARY_INSTANCE="${JATO_ENABLE_SECONDARY_INSTANCE:-false}"
SWAP_TARGET_MB="${JATO_SWAP_TARGET_MB:-4096}"
SWAP_MIN_MB="${JATO_SWAP_MIN_MB:-1024}"
SWAP_RESERVED_DISK_MB="${JATO_SWAP_RESERVED_DISK_MB:-1024}"


is_truthy() {
  local value
  value="$(echo "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "$value" == "1" || "$value" == "true" || "$value" == "yes" || "$value" == "on" ]]
}


cleanup_stale_swapfile_if_needed() {
  local swap_active
  local swapfile_size_bytes
  local swapfile_size_mb

  if [[ ! -f /swapfile ]]; then
    return
  fi

  swap_active="$(swapon --show=NAME --noheadings 2>/dev/null | awk '{print $1}' | grep -x '/swapfile' || true)"
  if [[ -n "$swap_active" ]]; then
    return
  fi

  swapfile_size_bytes="$(stat -c%s /swapfile 2>/dev/null || echo 0)"
  swapfile_size_mb=$((swapfile_size_bytes / 1024 / 1024))

  echo "[WARN] found inactive /swapfile (${swapfile_size_mb}MB); removing stale file before apt update"
  sudo rm -f /swapfile || true
  if grep -qE '^/swapfile\s' /etc/fstab; then
    sudo sed -i.bak '/^\/swapfile[[:space:]]/d' /etc/fstab || true
  fi
}


run_apt_update_with_retry() {
  if sudo apt-get update -y; then
    return
  fi

  echo "[WARN] apt update failed, cleaning apt cache and retrying once"
  sudo apt-get clean || true
  sudo rm -rf /var/lib/apt/lists/* || true
  cleanup_stale_swapfile_if_needed
  sudo apt-get update -y
}


ensure_swap_for_low_memory_host() {
  local mem_total_mb
  local swap_total_mb
  local root_free_mb
  local swap_size_mb
  local swap_active

  mem_total_mb="$(awk '/MemTotal:/ {print int($2/1024)}' /proc/meminfo)"
  swap_total_mb="$(awk '/SwapTotal:/ {print int($2/1024)}' /proc/meminfo)"
  root_free_mb="$(df -Pm / | awk 'NR==2 {print $4}')"

  if (( mem_total_mb >= 8000 )); then
    echo "[INFO] skip swap bootstrap (memory ${mem_total_mb}MB >= 8000MB)"
    return
  fi

  if (( swap_total_mb > 0 )); then
    echo "[INFO] swap already enabled (${swap_total_mb}MB)"
    return
  fi

  swap_size_mb="$SWAP_TARGET_MB"
  if (( root_free_mb <= SWAP_RESERVED_DISK_MB + SWAP_MIN_MB )); then
    echo "[WARN] low-memory host detected (${mem_total_mb}MB), but disk is tight (${root_free_mb}MB free); skip swap bootstrap"
    return
  fi

  if (( swap_size_mb > root_free_mb - SWAP_RESERVED_DISK_MB )); then
    swap_size_mb=$((root_free_mb - SWAP_RESERVED_DISK_MB))
  fi

  # Round down to 256MB step for predictable allocation.
  swap_size_mb=$((swap_size_mb / 256 * 256))
  if (( swap_size_mb < SWAP_MIN_MB )); then
    echo "[WARN] computed swap size ${swap_size_mb}MB is below minimum ${SWAP_MIN_MB}MB; skip swap bootstrap"
    return
  fi

  echo "[INFO] low-memory host detected (${mem_total_mb}MB), creating ${swap_size_mb}MB swap (free ${root_free_mb}MB)"

  swap_active="$(swapon --show=NAME --noheadings 2>/dev/null | awk '{print $1}' | grep -x '/swapfile' || true)"
  if [[ -n "$swap_active" ]]; then
    echo "[INFO] /swapfile already active"
    return
  fi

  if [[ -f /swapfile ]]; then
    sudo swapoff /swapfile >/dev/null 2>&1 || true
    sudo rm -f /swapfile
  fi

  if ! sudo fallocate -l "${swap_size_mb}M" /swapfile 2>/dev/null; then
    if ! sudo dd if=/dev/zero of=/swapfile bs=1M count="${swap_size_mb}" status=none; then
      echo "[WARN] failed to allocate swapfile; continue without swap"
      sudo rm -f /swapfile >/dev/null 2>&1 || true
      return
    fi
  fi

  if ! sudo chmod 600 /swapfile; then
    echo "[WARN] failed to chmod /swapfile; continue without swap"
    sudo rm -f /swapfile >/dev/null 2>&1 || true
    return
  fi

  if ! sudo mkswap /swapfile >/dev/null; then
    echo "[WARN] failed to initialize swapfile; continue without swap"
    sudo rm -f /swapfile >/dev/null 2>&1 || true
    return
  fi

  if ! sudo swapon /swapfile; then
    echo "[WARN] failed to enable swapfile; continue without swap"
    sudo rm -f /swapfile >/dev/null 2>&1 || true
    return
  fi

  if ! grep -qE '^/swapfile\s' /etc/fstab; then
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
  fi

  echo "[INFO] swap enabled: $(free -h | awk '/^Swap:/ {print $2}')"
}


install_single_backend_nginx_config() {
  sudo tee /etc/nginx/conf.d/jato_dashboard.conf >/dev/null <<'EOF'
upstream jato_dashboard_upstream {
    server 127.0.0.1:8501 max_fails=3 fail_timeout=30s;
    keepalive 32;
}

map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

server {
    listen 80 default_server;
    server_name _;

    client_max_body_size 32m;

    gzip on;
    gzip_comp_level 5;
    gzip_min_length 1024;
    gzip_types text/plain text/css application/json application/javascript application/xml+rss application/xml;

    location / {
        proxy_pass http://jato_dashboard_upstream;

        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;

        proxy_connect_timeout 5s;
        proxy_send_timeout 120s;
        proxy_read_timeout 120s;

        proxy_buffering off;
    }

    location = /healthz {
        proxy_pass http://jato_dashboard_upstream/_stcore/health;
        proxy_http_version 1.1;
        access_log off;
    }
}
EOF
}

if [[ ! -d "$APP_DIR" ]]; then
  echo "[ERROR] APP_DIR not found: $APP_DIR"
  exit 1
fi

cd "$APP_DIR"

cleanup_stale_swapfile_if_needed

echo "[INFO] apt update"
run_apt_update_with_retry

echo "[INFO] install runtime packages"
sudo apt-get install -y python3 python3-venv nginx git

ensure_swap_for_low_memory_host

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

echo "[INFO] start streamlit primary instance"
sudo systemctl enable --now jato-dashboard@8501

if is_truthy "$ENABLE_SECONDARY_INSTANCE"; then
  echo "[INFO] start streamlit secondary instance (8502)"
  sudo systemctl enable --now jato-dashboard@8502
else
  echo "[INFO] keep single-instance mode; disable 8502"
  sudo systemctl disable --now jato-dashboard@8502 || true
fi

echo "[INFO] install nginx config"

# Disable default nginx site to avoid 404 from default server block.
if [[ -e /etc/nginx/sites-enabled/default ]]; then
  sudo rm -f /etc/nginx/sites-enabled/default
fi
if [[ -e /etc/nginx/conf.d/default.conf ]]; then
  sudo rm -f /etc/nginx/conf.d/default.conf
fi

if is_truthy "$ENABLE_SECONDARY_INSTANCE"; then
  sudo cp 03_Scripts/deploy/nginx/jato_dashboard.conf.example /etc/nginx/conf.d/jato_dashboard.conf
else
  install_single_backend_nginx_config
fi

sudo nginx -t
sudo systemctl enable --now nginx
sudo systemctl reload nginx

echo "[INFO] done"
sudo systemctl --no-pager --full status jato-dashboard@8501 | head -n 20
if is_truthy "$ENABLE_SECONDARY_INSTANCE"; then
  sudo systemctl --no-pager --full status jato-dashboard@8502 | head -n 20
fi
curl -sS http://127.0.0.1/healthz || true
