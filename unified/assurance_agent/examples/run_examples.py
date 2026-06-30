"""
Runs every example plan through the Assurance Agent and prints the tiered output.

    # structural layer only (no Ollama needed):
    python -m assurance_agent.examples.run_examples

    # include the semantic layer (needs `ollama serve` + qwen2.5:7b-instruct):
    python -m assurance_agent.examples.run_examples --semantic

The intent-mismatch case only differs from a pass at the semantic layer, so
run with --semantic to see it flagged.
"""

import argparse
import os

from ..config_loader import load_schema
from ..orchestrator import AssuranceAgent

_HERE = os.path.dirname(os.path.abspath(__file__))

# (label, request, plan_file) — request matters mainly for the semantic layer.
CASES = [
    ("VALID PLAN",
     "Ingest raw orders, then total revenue (sum of price) per category and region.",
     "valid_plan.json"),
    ("INVALID — malformed JSON / contract",
     "Ingest raw orders into bronze.",
     "bad_json_plan.json"),
    ("INVALID — unknown column references",
     "Ingest orders and compute a net column.",
     "bad_column_plan.json"),
    ("INVALID — disallowed operation",
     "Ingest orders and compute the median price per category.",
     "bad_operation_plan.json"),
    ("INVALID — stage ordering",
     "Ingest raw orders then transform them.",
     "bad_ordering_plan.json"),
    ("INTENT MISMATCH (semantic only)",
     "Total revenue per category.",
     "intent_mismatch_plan.json"),
]


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--semantic", action="store_true", help="also run the LLM layer (needs Ollama)")
    args = p.parse_args(argv)

    schema = load_schema()
    agent = AssuranceAgent()

    for label, request, fname in CASES:
        with open(os.path.join(_HERE, fname), "r", encoding="utf-8") as f:
            plan_raw = f.read()

        result = agent.assure(request, plan_raw, schema, run_semantic=args.semantic)

        print("=" * 72)
        print(f"CASE: {label}   ({fname})")
        print(f"request: {request}")
        print("-" * 72)
        print(result.detail_text())
        fail = result.failure_text()
        if fail:
            print()
            print(fail)
        print()


if __name__ == "__main__":
    main()
