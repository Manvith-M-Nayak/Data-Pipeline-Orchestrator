"""
Name validation for pipeline plans.

Container, stage and dataset names end up inside generated notebook source,
Databricks workspace paths and ADF REST URLs. The Planner produces safe names,
but plans also arrive straight from clients (POST /api/manager/run,
/api/manager/stream/start), so every entry point re-checks them here and the
notebook builder refuses anything that fails.
"""

import re
from typing import List

# Azure Blob container rules: 3-63 chars, lowercase letters/digits/hyphens,
# starts and ends alphanumeric, no consecutive hyphens.
CONTAINER_RE = re.compile(r"^(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*$")
# Stage names become Databricks workspace paths and job names.
STAGE_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,99}$")
# ADF dataset names: letters, digits, underscores; must start with a letter.
DATASET_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,259}$")

_STAGE_CONTAINER_KEYS = ("source_container", "sink_container", "checkpoint_container")
_STAGE_DATASET_KEYS = ("source_dataset", "sink_dataset")


def is_valid_container(name) -> bool:
    return isinstance(name, str) and bool(CONTAINER_RE.match(name))


def plan_safety_issues(config: dict) -> List[str]:
    """Every unsafe or malformed name in the plan; empty list means safe."""
    issues: List[str] = []
    if not isinstance(config, dict):
        return ["plan must be a JSON object"]

    for c in config.get("containers_to_create") or []:
        if not is_valid_container(c):
            issues.append(f"invalid container name {c!r}")

    for ds in config.get("datasets") or []:
        if not isinstance(ds, dict):
            issues.append("dataset entry must be an object")
            continue
        if not isinstance(ds.get("name"), str) or not DATASET_RE.match(ds["name"]):
            issues.append(f"invalid dataset name {ds.get('name')!r}")
        if ds.get("container") is not None and not is_valid_container(ds["container"]):
            issues.append(f"dataset {ds.get('name')!r} has invalid container {ds['container']!r}")

    for s in config.get("stages") or []:
        if not isinstance(s, dict):
            issues.append("stage entry must be an object")
            continue
        name = s.get("name")
        if not isinstance(name, str) or not STAGE_RE.match(name):
            issues.append(f"invalid stage name {name!r}")
        for key in _STAGE_CONTAINER_KEYS:
            if s.get(key) is not None and not is_valid_container(s[key]):
                issues.append(f"stage {name!r} has invalid {key} {s[key]!r}")
        for key in _STAGE_DATASET_KEYS:
            if s.get(key) is not None and (not isinstance(s[key], str) or not DATASET_RE.match(s[key])):
                issues.append(f"stage {name!r} has invalid {key} {s[key]!r}")
    return issues
