from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path("config.example.yml")


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load YAML config into a dictionary."""
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"Config at {config_path} must be a dictionary.")
    return payload


def get_config_value(config: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    """Get nested config value with a fallback."""
    current: Any = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def resolve_db_path(config: dict[str, Any], fallback: str = "data/warehouse.duckdb") -> Path:
    """Resolve warehouse db path from config."""
    db_path = get_config_value(config, ["warehouse", "db_path"], fallback)
    return Path(db_path)
