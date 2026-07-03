"""
Resource Agent — predicts compute requirements, proposes right-sized
allocations, resolves parallel-group contention, enforces the student-tier
hard limits, reacts to live Monitor data, and self-corrects predictions from
historical run feedback.

Quick use:
    from resource_agent import ResourceAgent
    plan_out = ResourceAgent().analyze(plan, csv_size_bytes, schema, execution_groups)
"""

from .resource_agent import (
    ResourceAgent,
    ResourcePlan,
    StageRequirements,
    StageAllocation,
    MAX_WORKERS,
    MAX_DIU,
    MAX_CONCURRENT,
    MAX_TOTAL_MEM_GB,
    NODE_SPECS,
    DEFAULT_NODE,
)

__all__ = [
    "ResourceAgent",
    "ResourcePlan",
    "StageRequirements",
    "StageAllocation",
    "MAX_WORKERS",
    "MAX_DIU",
    "MAX_CONCURRENT",
    "MAX_TOTAL_MEM_GB",
    "NODE_SPECS",
    "DEFAULT_NODE",
]
