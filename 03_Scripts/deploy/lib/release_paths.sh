#!/usr/bin/env bash

resolve_msrp_evidence_root() {
  local project_root="$1"
  local configured_root="${2:-}"
  local candidate=""

  if [[ -n "$configured_root" ]]; then
    candidate="$configured_root"
  else
    candidate="$project_root/04_Processed_data/ops/msrp_source_evidence"
  fi
  python3 - "$project_root" "$candidate" <<'PY'
import sys
from pathlib import Path

project_root = Path(sys.argv[1]).expanduser().resolve()
candidate = Path(sys.argv[2]).expanduser()
if not candidate.is_absolute():
    candidate = project_root / candidate
print(candidate.resolve())
PY
}

assert_path_outside_release_roots() {
  local repo_dir="$1"
  local durable_path="$2"
  shift 2
  local release_path=""
  local normalized_durable=""
  local normalized_release=""

  normalized_durable="$(python3 -c 'import sys; from pathlib import Path; print(Path(sys.argv[1]).resolve())' "$durable_path")"
  for release_path in "$@"; do
    normalized_release="$(python3 -c 'import sys; from pathlib import Path; print(Path(sys.argv[1]).resolve())' "$repo_dir/$release_path")"
    case "$normalized_durable/" in
      "$normalized_release/"*)
        echo "[ERROR] Durable path is inside release replacement root: $normalized_durable under $normalized_release" >&2
        return 1
        ;;
    esac
  done
}

replace_release_paths() {
  local repo_dir="$1"
  local release_worktree="$2"
  shift 2
  local release_path=""

  for release_path in "$@"; do
    if [[ -e "$release_worktree/$release_path" ]]; then
      rm -rf "$repo_dir/$release_path"
      (cd "$release_worktree" && tar cf - "$release_path") \
        | (cd "$repo_dir" && tar xf -)
    fi
  done
}
