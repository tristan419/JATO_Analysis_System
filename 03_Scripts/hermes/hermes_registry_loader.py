"""Load Hermes YAML registry files into typed dicts.

All registries are optional — missing files produce warnings, not crashes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _resolve_registry_dir(registry_dir: str | None = None) -> Path:
    """Find the hermes/ registry directory relative to the project root."""
    if registry_dir:
        p = Path(registry_dir)
        if p.is_dir():
            return p.resolve()

    # Try relative to CWD
    cwd = Path.cwd()
    for candidate in [
        cwd / "hermes",
        cwd.parent / "hermes",
        cwd / ".." / "hermes",
    ]:
        if candidate.is_dir():
            return candidate.resolve()

    # Fall back to script-relative location
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    return (repo_root / "hermes").resolve()


def _safe_load_yaml(path: Path) -> dict[str, Any] | None:
    """Load a YAML file. Return None if missing or broken."""
    try:
        import yaml
    except ImportError:
        print(f"  [WARN] PyYAML not installed; cannot load {path}")
        return None

    if not path.is_file():
        print(f"  [WARN] Registry file not found: {path}")
        return None

    try:
        with open(path) as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        print(f"  [WARN] Failed to parse {path}: {exc}")
        return None


def load_all_registries(registry_dir: str | None = None) -> dict[str, Any]:
    """Load all Hermes registries.

    Returns a dict keyed by registry name (e.g. 'features', 'pipelines').
    Missing registries are empty lists.
    """
    base = _resolve_registry_dir(registry_dir)
    print(f"[Hermes] Registry dir: {base}")

    files = {
        "sources": "source_registry.yaml",
        "pipelines": "pipeline_registry.yaml",
        "features": "feature_registry.yaml",
        "prompts": "prompt_registry.yaml",
        "artifacts": "artifact_registry.yaml",
        "gaps": "governance_gaps.yaml",
        "proposals": "proposal_registry.yaml",
    }

    registries: dict[str, Any] = {}
    for key, filename in files.items():
        data = _safe_load_yaml(base / filename)
        if data:
            # Each YAML has one top-level key that is the list
            list_key = _list_key_for(key)
            registries[key] = data.get(list_key, [])
            print(f"  Loaded {key}: {len(registries[key])} entries")
        else:
            registries[key] = []

    return registries


def _list_key_for(registry_name: str) -> str:
    """Map registry name to the YAML top-level list key."""
    mapping = {
        "sources": "sources",
        "pipelines": "pipelines",
        "features": "features",
        "prompts": "prompts",
        "artifacts": "artifacts",
        "gaps": "gaps",
        "proposals": "proposals",
    }
    return mapping.get(registry_name, registry_name)


def find_feature_by_id(features: list[dict], feature_id: str) -> dict | None:
    """Find a feature entry by its featureId."""
    for f in features:
        if f.get("featureId") == feature_id:
            return f
    return None


def find_pipeline_by_id(pipelines: list[dict], pipeline_id: str) -> dict | None:
    """Find a pipeline entry by its pipelineId."""
    for p in pipelines:
        if p.get("pipelineId") == pipeline_id:
            return p
    return None
