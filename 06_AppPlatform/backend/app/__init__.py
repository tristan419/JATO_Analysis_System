"""JATO backend package."""

import sys
from pathlib import Path

# Add 03_Scripts to sys.path so upload_toolkit can be imported
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "03_Scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
