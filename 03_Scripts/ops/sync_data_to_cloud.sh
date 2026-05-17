#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# sync_data_to_cloud.sh
#
# 一键数据刷新 + 同步到腾讯云（全自动，无需输入密码）
#
# 用法:
#   bash 03_Scripts/ops/sync_data_to_cloud.sh <新xlsx文件路径>
#
# 也可以不传参数，自动扫描 01_RAW_DATA/new/ 目录：
#   bash 03_Scripts/ops/sync_data_to_cloud.sh
#
# 文件夹约定:
#   01_RAW_DATA/new/         ← 把新的 xlsx 文件放这里
#   01_RAW_DATA/archive/     ← 处理完成后自动归档到这里
#
# 流程:
#   1. 读取新 xlsx → 规范化
#   2. 对比已有数据，自动识别并补全缺失国家
#   3. 合并 → 写入本地 canonical parquet + 分区
#   4. 归档 xlsx 到 archive/
#   5. 上传到腾讯云
#   6. 重启后端 → 健康检查
# ─────────────────────────────────────────────────────────────────────
set -Eeuo pipefail

# ── 腾讯云连接信息（密钥免密登录）──
SSH_ALIAS="${SSH_ALIAS:-tencent-cloud}"
CLOUD_HOST="${CLOUD_HOST:-150.158.141.14}"
CLOUD_USER="${CLOUD_USER:-root}"
CLOUD_REPO="${CLOUD_REPO:-/opt/JATO_Analysis_System-main}"
CLOUD_DATA_DIR="${CLOUD_REPO}/04_Processed_data"
BACKEND_SERVICE="${BACKEND_SERVICE:-jato-fullstack-backend@8000}"
ALLOW_STALE_LOCAL_DATA_SYNC="${ALLOW_STALE_LOCAL_DATA_SYNC:-false}"

# ── 本地路径 ──
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
RAW_DIR="$PROJECT_ROOT/01_RAW_DATA"
NEW_DIR="$RAW_DIR/new"
ARCHIVE_DIR="$RAW_DIR/archive"
LOCAL_DATA_DIR="$PROJECT_ROOT/04_Processed_data"
LOCAL_PARQUET="$LOCAL_DATA_DIR/jato_full_archive.parquet"
LOCAL_PARTITIONS="$LOCAL_DATA_DIR/partitioned_dataset_v1"

# ── 颜色 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_step()  { printf "\n${CYAN}[STEP]${NC} %s\n" "$1"; }
log_ok()    { printf "${GREEN}  ✅ %s${NC}\n" "$1"; }
log_warn()  { printf "${YELLOW}  ⚠️  %s${NC}\n" "$1"; }
log_fail()  { printf "${RED}  ❌ %s${NC}\n" "$1"; }
log_info()  { printf "  %s\n" "$1"; }

is_truthy() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

check_cloud_active_not_newer() {
  local remote_summary_json

  if is_truthy "$ALLOW_STALE_LOCAL_DATA_SYNC"; then
    log_warn "跳过云端 freshness 防护，因为 ALLOW_STALE_LOCAL_DATA_SYNC=$ALLOW_STALE_LOCAL_DATA_SYNC"
    return 0
  fi

  remote_summary_json="$(mktemp)"
  log_step "2.5/6 检查本地候选是否会回退云端 active 数据..."
  ssh -o ConnectTimeout=30 "$SSH_ALIAS" CLOUD_REPO="$CLOUD_REPO" "bash -s" > "$remote_summary_json" <<'REMOTE_SCRIPT'
set -Eeuo pipefail
cd "$CLOUD_REPO"
PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi
"$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path

import pandas as pd

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

path = Path("04_Processed_data/jato_full_archive.parquet")
if not path.exists():
    print(json.dumps({"exists": False}, ensure_ascii=False))
    raise SystemExit(0)

df = pd.read_parquet(path)
country_col = "国家"
month_cols = [
    col for col in df.columns
    if isinstance(col, str)
    and len(col.split()) == 2
    and col.split()[0].isdigit()
    and col.split()[1] in MONTHS
]

countries: dict[str, dict[str, object]] = {}
for country, sub in df.groupby(country_col, dropna=True):
    totals: dict[str, float] = {}
    for col in month_cols:
        value = float(pd.to_numeric(sub[col], errors="coerce").fillna(0).sum())
        if value:
            totals[col] = value
    latest = next(reversed(totals), None)
    countries[str(country)] = {"latest": latest, "totals": totals}

print(json.dumps({"exists": True, "countries": countries}, ensure_ascii=False))
PY
REMOTE_SCRIPT

  python3 - "$LOCAL_PARQUET" "$remote_summary_json" <<'PY'
import json
import sys
from pathlib import Path

import pandas as pd

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def period_key(label: str | None) -> tuple[int, int]:
    if not label:
        return (0, 0)
    parts = label.split()
    if len(parts) != 2 or not parts[0].isdigit():
        return (0, 0)
    return (int(parts[0]), MONTHS.get(parts[1], 0))


def summarize(path: Path) -> dict[str, dict[str, object]]:
    df = pd.read_parquet(path)
    month_cols = [
        col for col in df.columns
        if isinstance(col, str)
        and len(col.split()) == 2
        and col.split()[0].isdigit()
        and col.split()[1] in MONTHS
    ]
    countries: dict[str, dict[str, object]] = {}
    for country, sub in df.groupby("国家", dropna=True):
        totals: dict[str, float] = {}
        for col in month_cols:
            value = float(pd.to_numeric(sub[col], errors="coerce").fillna(0).sum())
            if value:
                totals[col] = value
        countries[str(country)] = {"latest": next(reversed(totals), None), "totals": totals}
    return countries


local_path = Path(sys.argv[1])
remote_summary = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
if not remote_summary.get("exists"):
    print("  云端尚无 active parquet，允许首次同步。")
    raise SystemExit(0)

local_countries = summarize(local_path)
remote_countries = remote_summary.get("countries") or {}
regressions: list[str] = []

for country, remote_info in remote_countries.items():
    local_info = local_countries.get(country)
    remote_latest = str(remote_info.get("latest") or "")
    if local_info is None:
        regressions.append(f"{country}: 云端有 {remote_latest or '-'}，本地候选缺失该国家")
        continue
    local_latest = str(local_info.get("latest") or "")
    if period_key(remote_latest) > period_key(local_latest):
        regressions.append(f"{country}: 云端 {remote_latest} > 本地候选 {local_latest or '-'}")

if regressions:
    print("  ERROR: 本地候选会覆盖/回退云端网页月更后的 active 数据。", file=sys.stderr)
    print("  请先运行: bash 03_Scripts/sync_monthly_update_runtime_from_cloud.sh", file=sys.stderr)
    print("  然后基于同步后的本地 active parquet 再执行上传。", file=sys.stderr)
    print("  如确认要强制覆盖，可设置 ALLOW_STALE_LOCAL_DATA_SYNC=true。", file=sys.stderr)
    for item in regressions[:20]:
        print(f"    - {item}", file=sys.stderr)
    if len(regressions) > 20:
        print(f"    ... and {len(regressions) - 20} more", file=sys.stderr)
    raise SystemExit(1)

print("  本地候选未检测到国家月份回退。")
PY

  rm -f "$remote_summary_json"
  log_ok "云端 active freshness 防护通过"
}

# ── 确保 new/ 和 archive/ 目录存在 ──
mkdir -p "$NEW_DIR" "$ARCHIVE_DIR"

# ── 确定输入文件 ──
if [[ $# -ge 1 ]]; then
  # 传参模式
  NEW_XLSX="$1"
  if [[ ! "$NEW_XLSX" = /* ]]; then
    NEW_XLSX="$PROJECT_ROOT/$NEW_XLSX"
  fi
else
  # 自动扫描 new/ 目录
  shopt -s nullglob
  xlsx_files=("$NEW_DIR"/*.xlsx)
  shopt -u nullglob

  if [[ ${#xlsx_files[@]} -eq 0 ]]; then
    echo "用法:"
    echo "  方式一: 把 xlsx 文件放入 01_RAW_DATA/new/ 然后运行:"
    echo "          bash $0"
    echo ""
    echo "  方式二: 直接指定文件:"
    echo "          bash $0 '01_RAW_DATA/new/新文件.xlsx'"
    echo ""
    echo "  当前 01_RAW_DATA/new/ 目录为空，请先放入新的 xlsx 文件。"
    exit 1
  fi

  if [[ ${#xlsx_files[@]} -gt 1 ]]; then
    log_fail "01_RAW_DATA/new/ 中有多个 xlsx 文件，请只保留一个:"
    for f in "${xlsx_files[@]}"; do
      echo "    - $(basename "$f")"
    done
    exit 1
  fi

  NEW_XLSX="${xlsx_files[0]}"
fi

if [[ ! -f "$NEW_XLSX" ]]; then
  log_fail "文件不存在: $NEW_XLSX"
  exit 1
fi

NEW_XLSX_BASENAME="$(basename "$NEW_XLSX")"
echo ""
printf "${CYAN}══════════════════════════════════════════════════${NC}\n"
printf "${CYAN}  JATO 数据一键刷新 + 腾讯云同步${NC}\n"
printf "${CYAN}══════════════════════════════════════════════════${NC}\n"
echo "  输入文件: $NEW_XLSX_BASENAME"
echo ""

# ── 激活 venv ──
if [[ -f "$VENV_DIR/bin/activate" ]]; then
  source "$VENV_DIR/bin/activate"
fi

# ═══════════════════════════════════════════════════════════════════
#  Step 0: SSH 连通性检查
# ═══════════════════════════════════════════════════════════════════
log_step "0/6 检查腾讯云 SSH 连通性..."

if ssh -o ConnectTimeout=10 -o BatchMode=yes "$SSH_ALIAS" 'echo ok' >/dev/null 2>&1; then
  log_ok "SSH 免密连接正常: $SSH_ALIAS ($CLOUD_USER@$CLOUD_HOST)"
else
  # fallback: 尝试直连
  if ssh -o ConnectTimeout=10 -o BatchMode=yes -i ~/.ssh/tencent_lh.pem "$CLOUD_USER@$CLOUD_HOST" 'echo ok' >/dev/null 2>&1; then
    SSH_ALIAS="$CLOUD_USER@$CLOUD_HOST"
    log_ok "SSH 连接正常 (直连模式)"
  else
    log_fail "无法 SSH 连接到腾讯云"
    echo "  请确认:"
    echo "  1. 当前网络可达腾讯云 (ping $CLOUD_HOST)"
    echo "  2. 安全组已放通 22 端口"
    echo "  3. 密钥文件 ~/.ssh/tencent_lh.pem 存在且正确"
    echo ""
    echo "  手动测试: ssh tencent-cloud 'echo ok'"
    exit 1
  fi
fi

# ═══════════════════════════════════════════════════════════════════
#  Step 1: 读取 xlsx → 对比 → 合并 → 写 parquet
# ═══════════════════════════════════════════════════════════════════
log_step "1/6 读取新 xlsx 并合并数据..."

python3 -c "
import sys, os, json, shutil
from datetime import datetime, timezone
sys.path.insert(0, '${SCRIPT_DIR}')
from elt_worker import read_excel_with_fallback, normalize_dataframe
import pandas as pd

new_xlsx = '''${NEW_XLSX}'''
local_parquet = '${LOCAL_PARQUET}'
local_data_dir = '${LOCAL_DATA_DIR}'

# --- 读取新文件 ---
print('  📥 读取新 xlsx...')
new_df = read_excel_with_fallback(new_xlsx, 'Data Export')
new_df = normalize_dataframe(new_df)
print(f'     新文件: {new_df.shape[0]:,} 行 × {new_df.shape[1]} 列')

new_countries = sorted(new_df['国家'].unique())
print(f'     新文件国家 ({len(new_countries)}): {new_countries}')

# --- 对比已有数据 ---
if os.path.exists(local_parquet):
    existing_df = pd.read_parquet(local_parquet)
    existing_countries = set(existing_df['国家'].unique())
    new_country_set = set(new_countries)

    missing_countries = sorted(existing_countries - new_country_set)

    if missing_countries:
        print(f'     缺失国家 ({len(missing_countries)}): {missing_countries}')
        supplement = existing_df[existing_df['国家'].isin(missing_countries)].copy()
        print(f'     从已有数据补充: {supplement.shape[0]:,} 行')

        for c in missing_countries:
            cnt = len(supplement[supplement['国家'] == c])
            print(f'       {c}: {cnt:,} 行')

        merged = pd.concat([new_df, supplement], ignore_index=True, sort=False)
    else:
        print('     无缺失国家，直接使用新文件')
        merged = new_df
else:
    print('     首次导入，无已有数据')
    merged = new_df
    missing_countries = []

final_countries = sorted(merged['国家'].unique())
print(f'     最终: {merged.shape[0]:,} 行 × {merged.shape[1]} 列, {len(final_countries)} 个国家')

# --- 备份旧数据 ---
backup_dir = os.path.join(local_data_dir, '.refresh_backups',
                           f'pre-sync-{datetime.now().strftime(\"%Y%m%d-%H%M%S\")}')
os.makedirs(backup_dir, exist_ok=True)
if os.path.exists(local_parquet):
    shutil.copy2(local_parquet, os.path.join(backup_dir, 'jato_full_archive.parquet'))

manifest_path = os.path.join(local_data_dir, 'manifest.json')
if os.path.exists(manifest_path):
    shutil.copy2(manifest_path, os.path.join(backup_dir, 'manifest.json'))
print(f'     备份: {backup_dir}')

# --- 写 parquet ---
merged.to_parquet(local_parquet, engine='pyarrow', compression='snappy', index=False)
file_size = os.path.getsize(local_parquet)
print(f'     写入: {local_parquet} ({file_size / 1024 / 1024:.1f} MB)')

# --- 写 manifest ---
manifest = {
    'generatedAtUtc': datetime.now(timezone.utc).isoformat(),
    'rows': int(len(merged)),
    'columns': int(len(merged.columns)),
    'schemaVersion': '1.1',
    'manifestSchemaVersion': '1.1',
    'sourceFile': os.path.basename(new_xlsx),
    'sheetName': 'Data Export',
    'outputFile': 'jato_full_archive.parquet',
    'outputBytes': int(file_size),
    'countries': final_countries,
    'countryCount': len(final_countries),
}

if missing_countries:
    manifest['supplementedCountries'] = missing_countries
    manifest['mergeNote'] = f'{len(new_countries)}国新数据 + {len(missing_countries)}国补充数据'

with open(manifest_path, 'w', encoding='utf-8') as f:
    json.dump(manifest, f, ensure_ascii=False, indent=2)
print(f'     manifest 已更新')
" 2>&1

if [[ $? -ne 0 ]]; then
  log_fail "数据处理失败"
  exit 1
fi
log_ok "数据处理完成"

# ═══════════════════════════════════════════════════════════════════
#  Step 2: 构建分区数据
# ═══════════════════════════════════════════════════════════════════
log_step "2/6 构建分区数据集..."

python3 "$SCRIPT_DIR/build_partitioned_dataset.py" \
  --input "$LOCAL_PARQUET" \
  --output "$LOCAL_PARTITIONS" 2>&1

if [[ $? -ne 0 ]]; then
  log_fail "分区构建失败"
  exit 1
fi

partition_count=$(find "$LOCAL_PARTITIONS" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
log_ok "分区构建完成: ${partition_count} 个国家分区"

check_cloud_active_not_newer

# ═══════════════════════════════════════════════════════════════════
#  Step 3: 归档 xlsx 到 archive/
# ═══════════════════════════════════════════════════════════════════
log_step "3/6 归档 xlsx 文件..."

ARCHIVE_DEST="$ARCHIVE_DIR/$NEW_XLSX_BASENAME"
if [[ -f "$ARCHIVE_DEST" ]]; then
  # 同名已存在，加时间戳
  ARCHIVE_DEST="$ARCHIVE_DIR/$(date +%Y%m%d-%H%M%S)_$NEW_XLSX_BASENAME"
fi
mv "$NEW_XLSX" "$ARCHIVE_DEST"
log_ok "已归档: $(basename "$ARCHIVE_DEST")"

# ═══════════════════════════════════════════════════════════════════
#  Step 4: 打包并上传到腾讯云
# ═══════════════════════════════════════════════════════════════════
log_step "4/6 打包并上传到腾讯云..."

TMPDIR_ARCHIVE=$(mktemp -d)
ARCHIVE_PATH="$TMPDIR_ARCHIVE/jato_data_sync.tar.gz"

log_info "打包中..."
tar czf "$ARCHIVE_PATH" \
  -C "$LOCAL_DATA_DIR" \
  jato_full_archive.parquet \
  manifest.json \
  partitioned_dataset_v1 2>&1

archive_size=$(du -h "$ARCHIVE_PATH" | awk '{print $1}')
log_info "归档大小: $archive_size"

log_info "上传中 (scp)..."
scp -o ConnectTimeout=30 -o ServerAliveInterval=15 "$ARCHIVE_PATH" "$SSH_ALIAS:/tmp/jato_data_sync.tar.gz"

if [[ $? -ne 0 ]]; then
  log_fail "上传失败"
  rm -rf "$TMPDIR_ARCHIVE"
  exit 1
fi
log_ok "上传完成"

rm -rf "$TMPDIR_ARCHIVE"

# ═══════════════════════════════════════════════════════════════════
#  Step 5: 服务器端解压 + 替换 + 重启
# ═══════════════════════════════════════════════════════════════════
log_step "5/6 服务器端解压并替换数据..."

ssh -o ConnectTimeout=30 "$SSH_ALIAS" bash -s <<REMOTE_SCRIPT
set -Eeuo pipefail

DATA_DIR="${CLOUD_DATA_DIR}"
ARCHIVE="/tmp/jato_data_sync.tar.gz"
BACKUP_DIR="\${DATA_DIR}/.refresh_backups/pre-sync-\$(date +%Y%m%d-%H%M%S)"

echo "  备份当前数据..."
mkdir -p "\$BACKUP_DIR"

if [[ -f "\${DATA_DIR}/jato_full_archive.parquet" ]]; then
  cp "\${DATA_DIR}/jato_full_archive.parquet" "\$BACKUP_DIR/"
fi
if [[ -f "\${DATA_DIR}/manifest.json" ]]; then
  cp "\${DATA_DIR}/manifest.json" "\$BACKUP_DIR/"
fi

echo "  解压新数据..."
if [[ -d "\${DATA_DIR}/partitioned_dataset_v1" ]]; then
  rm -rf "\${DATA_DIR}/partitioned_dataset_v1"
fi

tar xzf "\$ARCHIVE" -C "\$DATA_DIR"
rm -f "\$ARCHIVE"

echo "  重启后端..."
systemctl restart ${BACKEND_SERVICE}
sleep 2

echo "  健康检查..."
if curl -fsS --connect-timeout 5 --max-time 10 http://127.0.0.1:8000/healthz >/dev/null 2>&1; then
  echo "  ✅ 后端健康检查通过"
else
  echo "  ⚠️  后端可能还在启动中，请稍后手动验证"
fi

echo "  当前数据状态:"
ls -lh "\${DATA_DIR}/jato_full_archive.parquet"
echo "  分区数: \$(find \${DATA_DIR}/partitioned_dataset_v1 -mindepth 1 -maxdepth 1 -type d | wc -l)"
REMOTE_SCRIPT

if [[ $? -ne 0 ]]; then
  log_fail "服务器端部署失败"
  exit 1
fi
log_ok "服务器端部署完成"

# ═══════════════════════════════════════════════════════════════════
#  Step 6: 远程验证
# ═══════════════════════════════════════════════════════════════════
log_step "6/6 远程验证..."

sleep 3

if ssh -o ConnectTimeout=10 "$SSH_ALIAS" \
  "curl -fsS --connect-timeout 5 --max-time 15 http://127.0.0.1:8000/healthz" 2>/dev/null; then
  echo ""
  log_ok "后端服务正常运行"
else
  log_warn "后端可能还在启动中，请稍后手动检查"
fi

echo ""
printf "${GREEN}════════════════════════════════════════════════${NC}\n"
printf "${GREEN}  数据同步完成！${NC}\n"
printf "${GREEN}  本地 + 腾讯云数据已更新${NC}\n"
printf "${GREEN}  xlsx 已归档到: 01_RAW_DATA/archive/${NC}\n"
printf "${GREEN}  网站应已可查看最新数据${NC}\n"
printf "${GREEN}════════════════════════════════════════════════${NC}\n"
echo ""
