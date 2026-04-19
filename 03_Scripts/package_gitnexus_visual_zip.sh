#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPORT_DIR="$ROOT_DIR/08_GitNexus/exports"
OUTPUT_PATH="${1:-$EXPORT_DIR/JATO_Analysis_System-gitnexus-upload.zip}"
INCLUDE_PATHS=(
  ".github"
  "02_Config_MetaData"
  "03_Scripts"
  "05_DashBoard"
  "06_AppPlatform"
  "07_ScrapingToolkit"
  "airflow"
  "Dockerfile"
  "docker-compose.yml"
  "requirements.txt"
  "Readxls.txt"
  "Markdown_Readme"
  "data_wangler"
  "dummy_nvidia.py"
  "get_columns_test.py"
  "test_rag_query.py"
)
EXCLUDE_PATHS=(
  ":(exclude)**/__pycache__/**"
  ":(exclude)**/*.pyc"
  ":(exclude)**/node_modules/**"
  ":(exclude)**/dist/**"
  ":(exclude)**/.runtime/**"
)

mkdir -p "$(dirname "$OUTPUT_PATH")"
rm -f "$OUTPUT_PATH"

cd "$ROOT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "This script must run inside a git repository." >&2
  exit 1
fi

if ! git ls-files -z -- "${INCLUDE_PATHS[@]}" "${EXCLUDE_PATHS[@]}" | grep -q .; then
  echo "No tracked files found to package." >&2
  exit 1
fi

git ls-files -z -- "${INCLUDE_PATHS[@]}" "${EXCLUDE_PATHS[@]}" | xargs -0 zip -q "$OUTPUT_PATH"

printf '%s\n' "$OUTPUT_PATH"
