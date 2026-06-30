# Assurance Agent

Independently **verifies** the Planner Agent's output. It never generates or
mutates a plan — it only inspects a plan and reports whether it is sound.

The Planner produces a pipeline-config plan (JSON). The Assurance Agent takes
that plan plus the original user request and the target schema, and answers two
questions:

1. **Is the plan structurally sound?** (deterministic, no model)
2. **Does the plan actually match what the user asked for?** (LLM, advisory)

## Two layers

### 1. Structural validation — pure Python, deterministic, NO model

The bulk of the agent. 100% reliable, never calls an LLM, never edits the plan.
Four checks, each returns a binary pass/fail plus a specific violation message:

| Check                | What it verifies                                                        |
|----------------------|-------------------------------------------------------------------------|
| `json_schema`        | Plan parses as JSON and has the required top-level keys with right types |
| `column_references`  | Every column the plan references exists in the target schema            |
| `allowed_operations` | Every stage type / aggregation op is in the configurable whitelist      |
| `stage_ordering`     | Stages follow the ordering rules (load before transform, no inversion)  |

If `json_schema` fails, the other three are reported as skipped (they cannot run
reliably on a plan that does not parse).

### 2. Semantic validation — base LLM via Ollama, prompting only

Probabilistic. Hands the **base** model `qwen2.5:7b-instruct` the original
request + the generated plan and asks it to flag intent mismatches. Returns
`{flagged, reasoning}`.

- **No LoRA adapter.** The Planner's adapter was trained to *generate* plans,
  not *validate* them; reusing it would bias the validator toward agreeing with
  the Planner. The validator uses clean base weights and shares no generation
  logic with the Planner.
- **Advisory by default.** A semantic flag does **not** fail the plan — a human
  may override it. Pass `--block-on-intent` (CLI) / `block_on_intent: true`
  (API) to make an intent flag a hard failure.
- **Graceful degradation.** If Ollama is down or returns junk, the semantic
  result is marked unavailable and is not counted against the plan.

## Output contract

`AssuranceResult.to_dict()`:

```jsonc
{
  "overall_status": "pass" | "fail",          // driven by structural checks
  "summary": "Assurance: passed (structure ✓, schema ✓, intent ✓)",
  "structural_results": [ { "check", "label", "passed", "message", "tier" }, ... ],
  "semantic_result": { "flagged", "reasoning", "model", "available" },
  "tiers": { "summary": "...", "detail": "...", "failure": "..." }
}
```

### Tiered visibility

- **SUMMARY** (always): one line — `Assurance: passed (structure ✓, schema ✓, intent ✓)`
- **DETAIL** (on demand): full per-check breakdown + semantic reasoning
- **FAILURE** (prominent): exactly which check failed, the specific violation, and why

## Configuration (external, nothing hardcoded)

`assurance_agent/config/`:

- `schema.example.json`     — target data schema (columns + inferred types)
- `allowed_operations.json` — stage-type & aggregation whitelists, plus the SQL
  function/keyword tokens the column check ignores
- `stage_ordering.json`     — per-type ranks + ordering rules

Override any path with env vars: `ASSURANCE_SCHEMA`, `ASSURANCE_ALLOWED_OPS`,
`ASSURANCE_STAGE_ORDERING`. Model/host: `ASSURANCE_MODEL`, `OLLAMA_HOST`.

## Setup

```bash
# from the `unified/` directory
pip install -r requirements.txt        # only needs `requests` (already present)

# semantic layer (skip if you only use the structural layer):
ollama serve                           # start the local server
ollama pull qwen2.5:7b-instruct        # base model — NOT the planner adapter
```

## Run

```bash
# all example cases, structural only (no Ollama needed):
python -m assurance_agent.examples.run_examples

# include the semantic layer:
python -m assurance_agent.examples.run_examples --semantic

# a single plan:
python -m assurance_agent.cli \
    --request "Total revenue (sum of price) per category and region" \
    --plan    assurance_agent/examples/valid_plan.json \
    --schema  assurance_agent/config/schema.example.json \
    --tier    detail            # summary | detail | failure | json

# structural only, no model:
python -m assurance_agent.cli --request "..." --plan plan.json --no-semantic
```

Exit code is `0` when `overall_status == "pass"`, else `1`.

### Library

```python
from assurance_agent import AssuranceAgent
from assurance_agent.config_loader import load_schema

agent = AssuranceAgent()
result = agent.assure(user_request, plan_dict_or_json, load_schema())
print(result.summary_line())
print(result.to_dict())
```

### HTTP (wired into the unified backend)

`POST /api/assurance/validate`

```jsonc
{
  "request": "Total revenue per category",
  "plan":    { /* Planner output */ },
  "schema":  { "columns": ["..."], "inferred_types": { } },  // optional
  "run_semantic": true,
  "block_on_intent": false
}
```

`GET /api/assurance/health` → liveness.

## Examples

| File                        | Outcome                                            |
|-----------------------------|----------------------------------------------------|
| `valid_plan.json`           | passes every check                                 |
| `bad_json_plan.json`        | fails `json_schema` (malformed JSON)               |
| `bad_column_plan.json`      | fails `column_references` (`discount`, `shipping_status`) |
| `bad_operation_plan.json`   | fails `allowed_operations` (`median`)              |
| `bad_ordering_plan.json`    | fails `stage_ordering` (transform before load)     |
| `intent_mismatch_plan.json` | structurally valid; semantic layer flags intent    |
