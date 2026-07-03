"""
Assurance Agent — independently VERIFIES Planner Agent output. Never generates.

Two layers:
  - structural : pure-Python deterministic checks (no model)
  - semantic   : base LLM (qwen2.5:7b-instruct via Ollama), prompting only,
                 no LoRA adapter — independent of the Planner.

Quick use:
    from assurance_agent import AssuranceAgent
    agent = AssuranceAgent()
    result = agent.assure(user_request, plan_dict_or_json, schema)
    print(result.summary_line())
"""

from .orchestrator import AssuranceAgent
from .result import AssuranceResult, CheckResult, SemanticResult

__all__ = ["AssuranceAgent", "AssuranceResult", "CheckResult", "SemanticResult"]
