"""
Configuration loader for Edge Gateway.
"""

import yaml
from pathlib import Path


def load_config(config_path: str = "config/gateway.yaml") -> dict:
    """Load and validate gateway configuration from YAML."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    _validate_config(config)
    return config


def _validate_config(config: dict) -> None:
    """Validate required configuration fields."""
    required_sections = ["gateway", "sources", "correlation", "kafka", "wal"]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")

    if "id" not in config["gateway"]:
        raise ValueError("gateway.id is required")

    if "bootstrap_servers" not in config["kafka"]:
        raise ValueError("kafka.bootstrap_servers is required")
