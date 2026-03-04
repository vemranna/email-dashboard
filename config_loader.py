#!/usr/bin/env python3
"""
config_loader.py — Load and cache config.yaml.

All other modules call load_config() to get a plain dict of settings.
The file is read once and cached in memory for the process lifetime.
"""

import os
import yaml

_config_cache = None
_CONFIG_PATH = os.environ.get("CONFIG_PATH", "config.yaml")


def load_config() -> dict:
    """
    Parse config.yaml and return it as a nested dict.
    Results are cached after the first call.
    """
    global _config_cache
    if _config_cache is None:
        with open(_CONFIG_PATH, "r") as f:
            _config_cache = yaml.safe_load(f)
    return _config_cache


def reload_config() -> dict:
    """Force a re-read of config.yaml (useful if autosend flag changes on disk)."""
    global _config_cache
    _config_cache = None
    return load_config()


def is_autosend() -> bool:
    """Convenience: return the current autosend flag from config."""
    return load_config().get("sending", {}).get("autosend", False)


def set_autosend(value: bool):
    """
    Persist a change to the autosend flag back to config.yaml.
    This is called by the Flask dashboard when the admin toggles autosend.
    We do a simple read-modify-write of the YAML file.
    """
    cfg = load_config()
    cfg["sending"]["autosend"] = value
    with open(_CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
    reload_config()
