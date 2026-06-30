"""
Loads the external config bundle for the Assurance Agent.

All rules are configurable via JSON files in assurance_agent/config/ — nothing
about the whitelist, ordering rules, or schema is hardcoded in the checks.
Override any path via env var or function argument.
"""

import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_CONFIG_DIR = os.path.join(_HERE, "config")

DEFAULT_ALLOWED_OPS = os.path.join(_CONFIG_DIR, "allowed_operations.json")
DEFAULT_ORDERING    = os.path.join(_CONFIG_DIR, "stage_ordering.json")
DEFAULT_SCHEMA      = os.path.join(_CONFIG_DIR, "schema.example.json")


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_allowed_operations(path: str = None) -> dict:
    return _load_json(os.getenv("ASSURANCE_ALLOWED_OPS", path or DEFAULT_ALLOWED_OPS))


def load_stage_ordering(path: str = None) -> dict:
    return _load_json(os.getenv("ASSURANCE_STAGE_ORDERING", path or DEFAULT_ORDERING))


def load_schema(path: str = None) -> dict:
    return _load_json(os.getenv("ASSURANCE_SCHEMA", path or DEFAULT_SCHEMA))
