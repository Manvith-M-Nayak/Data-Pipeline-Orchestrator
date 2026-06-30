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


def normalize_schema(schema) -> dict:
    """
    Coerce any schema shape the project produces into the canonical form the
    structural checks expect: {"columns": [names], "inferred_types": {name: type}}.

    Accepts:
      - canonical:        {"columns": ["a","b"], "inferred_types": {...}}
      - detect-style:     {"columns": {"a": "string"}, "preview": ...}
      - bare column map:  {"a": "string", "b": "integer"}   (what the frontend
                          stores in localStorage `last_csv_schema`)
      - None / junk:      -> empty columns
    """
    if not isinstance(schema, dict):
        return {"columns": [], "inferred_types": {}}

    cols = schema.get("columns")
    if isinstance(cols, dict):
        return {"columns": list(cols.keys()), "inferred_types": dict(cols)}
    if isinstance(cols, list):
        return {"columns": list(cols),
                "inferred_types": schema.get("inferred_types", {})}

    # No "columns" key — treat the dict itself as a {col: type} map if it looks
    # like one (all values are type-name strings).
    if schema and all(isinstance(v, str) for v in schema.values()):
        return {"columns": list(schema.keys()), "inferred_types": dict(schema)}

    return {"columns": [], "inferred_types": {}}
