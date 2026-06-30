"""
CLI for the Assurance Agent.

    python -m assurance_agent.cli \
        --request "Ingest orders then total revenue per category" \
        --plan    assurance_agent/examples/valid_plan.json \
        --schema  assurance_agent/config/schema.example.json \
        --tier    detail

--tier: summary | detail | failure | json   (default: detail)
--no-semantic: skip the LLM layer (structural only — no Ollama needed)
"""

import argparse
import json
import sys

from .config_loader import load_schema
from .orchestrator import AssuranceAgent


def main(argv=None):
    p = argparse.ArgumentParser(description="Verify a Planner plan (structural + semantic).")
    p.add_argument("--request", required=True, help="original user request / intent")
    p.add_argument("--plan", required=True, help="path to the generated plan JSON")
    p.add_argument("--schema", default=None, help="path to target schema JSON (default: config template)")
    p.add_argument("--tier", default="detail", choices=["summary", "detail", "failure", "json"])
    p.add_argument("--no-semantic", action="store_true", help="skip the LLM semantic layer")
    p.add_argument("--block-on-intent", action="store_true",
                   help="treat a semantic intent flag as a hard overall failure")
    args = p.parse_args(argv)

    # Read the plan as RAW TEXT so the json_schema check can catch malformed JSON.
    with open(args.plan, "r", encoding="utf-8") as f:
        plan_raw = f.read()

    schema = load_schema(args.schema) if args.schema else load_schema()

    agent = AssuranceAgent(semantic_blocks_overall=args.block_on_intent)
    result = agent.assure(
        user_request=args.request,
        plan=plan_raw,
        schema=schema,
        run_semantic=not args.no_semantic,
    )

    if args.tier == "summary":
        print(result.summary_line())
    elif args.tier == "detail":
        print(result.detail_text())
        fail = result.failure_text()
        if fail:
            print("\n" + fail)
    elif args.tier == "failure":
        print(result.failure_text() or ("No failures. " + result.summary_line()))
    else:  # json
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))

    return 0 if result.overall_status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
