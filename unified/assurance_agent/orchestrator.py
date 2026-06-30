"""
Orchestration — combines the two validation layers into one AssuranceResult.

Flow:
  1. Run the deterministic STRUCTURAL layer (no model).
  2. Optionally run the probabilistic SEMANTIC layer (base LLM via Ollama).
  3. Decide overall_status:
       - "fail" if ANY structural check fails (deterministic, authoritative).
       - a semantic flag is ADVISORY by default and does NOT fail the plan
         (configurable via semantic_blocks_overall).

The Assurance Agent only VERIFIES — it never generates or mutates a plan.
"""

import json

from .config_loader import load_allowed_operations, load_stage_ordering
from .result import AssuranceResult
from .semantic import check_intent
from .structural import StructuralValidator


class AssuranceAgent:
    def __init__(
        self,
        allowed_ops: dict = None,
        ordering_rules: dict = None,
        semantic_blocks_overall: bool = False,
    ):
        self.allowed_ops = allowed_ops or load_allowed_operations()
        self.ordering_rules = ordering_rules or load_stage_ordering()
        self.semantic_blocks_overall = semantic_blocks_overall
        self.structural = StructuralValidator(self.allowed_ops, self.ordering_rules)

    def assure(
        self,
        user_request: str,
        plan,                       # parsed dict OR raw JSON string
        schema: dict,
        run_semantic: bool = True,
    ) -> AssuranceResult:
        # 1. structural (deterministic)
        structural_results = self.structural.validate(plan, schema)
        structural_pass = all(c.passed for c in structural_results)

        # 2. semantic (probabilistic) — only meaningful if the plan parsed.
        #    structural_results[0] is the json_schema check.
        semantic_result = None
        if run_semantic and structural_results[0].passed:
            parsed = json.loads(plan) if isinstance(plan, str) else plan
            semantic_result = check_intent(user_request, parsed)

        # 3. overall status
        overall = "pass" if structural_pass else "fail"
        if (
            self.semantic_blocks_overall
            and semantic_result
            and semantic_result.available
            and semantic_result.flagged
        ):
            overall = "fail"

        return AssuranceResult(
            overall_status=overall,
            structural_results=structural_results,
            semantic_result=semantic_result,
            semantic_blocks_overall=self.semantic_blocks_overall,
        )
