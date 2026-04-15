"""Configuration loading utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """Load JSON configuration from disk."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    with path.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)
