from __future__ import annotations

import os


REPO_ROOT = os.getenv("JATO_REPO_ROOT", "/opt/jato")
DEFAULT_ENV = {
    "APP_PROJECT_ROOT": REPO_ROOT,
    "PYTHONPATH": f"{REPO_ROOT}/07_ScrapingToolkit",
}


def repo_bash(command: str) -> str:
    return f"cd {REPO_ROOT} && {command}"
