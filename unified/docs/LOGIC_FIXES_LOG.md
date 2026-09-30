# Logic fixes log

Running record of fixes for the issues in
[LOGIC_REVIEW_FINDINGS.md](LOGIC_REVIEW_FINDINGS.md) (ids H1–H8, M1–M9, and
the Low list). The work is done in stages, one commit per stage. Each stage
records:
- what changed and why;
- how it was verified;
- anything left open or decided differently from the review.

| Stage | Scope | Findings | Status |
|---|---|---|---|
| 1 | Plan safety — name validation at every entry point | H1, M8 (security part) | Done |
| 2 | Schema plumbing — get the real schema to every agent | H2, H3, H4 | Done |
| 3 | Learning loops — what feeds the correction factors | H5, H6, M1, M2, M3, M9 | Pending |
| 4 | Execution — concurrent-run isolation, executor issues | H7, H8, M4, run-id collisions, retries | Pending |
| 5 | Frontend and the rest | M5, M6, M7, M8 (rest), remaining Low | Pending |

---

## Stage 1 — Plan safety

**Commit message:** `fix(security): validate container/stage/dataset names at every plan entry point`

### Problem (H1, and part of M8)

- **Unescaped names in generated code.** The notebook builder wrote container
  names straight into generated Python (`SOURCE_CONTAINER = "{name}"`).
- **Planner sanitising could be bypassed.** The Planner cleans names, but
  `POST /api/manager/run` and `POST /api/manager/stream/start` accept the plan
  JSON from the client, so a crafted plan skipped that step.
- **Nothing downstream checked names.** Neither the Assurance Agent nor
  `validate_plan` looked at them.
- **The review reproduced it.** A container name like
  `x"; __import__("os").system("id"); y = "` produced valid Python that would
  run on Databricks with the storage key in scope, and assurance returned `pass`.
- **Stage and dataset names were also unchecked.** They become Databricks
  workspace paths, job names and ADF REST URL segments.
- **The earlier filter-injection fix (#1) only covered filters.** This path was
  missed.

### Changes

| File | Change | Why |
|---|---|---|
| `executor_agent/plan_safety.py` *(new)* | `plan_safety_issues(config)` and `is_valid_container(name)`. Container: valid Azure name (3–63 chars, lowercase letters/digits, single hyphens, alphanumeric at both ends). Stage: `[A-Za-z0-9_-]`, ≤100 chars. Dataset: starts with a letter, then `[A-Za-z0-9_]`. Covers `containers_to_create`, `datasets[].name/container`, and each stage's `source/sink/checkpoint_container` and `source/sink_dataset`. | One definition of "safe name", shared by every entry point, so the checks can't drift apart. |
| `executor_agent/notebook_builder.py` | `_require_container()` guards source, sink and checkpoint names in both the batch and streaming builders; an invalid name raises `UnsupportedTransformError`. | The last line of defence: the builder can no longer produce code from an unsafe name, whoever called it. The executor already turns this error into a clean "Plan cannot be compiled" failure. |
| `executor_agent/executor.py` | `execute_pipeline` rejects unsafe plans before any cloud call. | Fail fast: no containers created, nothing uploaded. |
| `central_manager_agent/manager.py` | `validate_plan` adds any safety issues to its blocking issues. | A managed run fails in Phase 1 with a clear reason, and it shows in the run's decision log. |
| `central_manager_agent/stream_manager.py` | `start()` raises `ValueError` for unsafe plans; the router already maps that to HTTP 422. | Streams skipped all validation before (M8). |
| `planner_agent/planner_common.py` | New `_normalize_identifiers()`, called by `_structural_validate`: cleans stage and dataset names and updates `execution_order`, `execution_groups` and copy-stage dataset references. | The Planner must always produce plans that pass the new checks, even when the LLM puts spaces or punctuation in names. |

### Verification

- **The review's reproduction is rejected everywhere:**
  - both notebook builders;
  - `execute_pipeline` (before any cloud call);
  - `validate_plan` (`ok: False`);
  - stream `start()`.
- **Nothing valid is broken:** all 13 real plans in `data/adf_monitor.db` pass
  the new checks.
- **Messy LLM names are normalized:** `"Filter Rows!"` → `Filter_Rows` and
  `"DS Raw-Data"` → `DS_Raw_Data`, with references updated, and the result
  passes. The streaming conversion of that plan also passes.
- **Regressions:**
  - `scripts/integration_test.py` passes;
  - the teammate's `test_cost_model_safety.py` passes (13 tests);
  - the backend starts with no tracebacks, and all 25 GET endpoints return 200;
  - `ruff` (F, E9) is clean.

### Notes and decisions

- **Strict validation, not escaping.** Escaping names would still allow names
  Azure rejects, and they would then fail later with a vaguer error.
- **The rest of M8 is not in this stage.** Streams still skip Assurance, and
  stream ticks aren't recorded in the monitor. That is planned for stage 5.
- **A long sink name can make an invalid checkpoint name.** The streaming
  checkpoint name is `{sink}-chk`, so a user-chosen sink longer than 59
  characters produces a checkpoint name that is too long. That is now rejected
  with a clear error instead of failing inside Azure.

---

## Stage 2 — Schema plumbing

**Commit message:** `fix(schema): pass the real input schema to every agent and count aggregations consistently`

### Problems

- **H2: agents read a schema the plan never has.** The Performance and Cost
  agents read `plan["schema"]` and `plan["csv_size_bytes"]`, but the plan never
  contains them: the UI sends the schema as a separate form field. As a result:
  - the Performance model always got `row_count=0`;
  - the Cost ML candidate was always rejected (`row_count <= 0`), so in real
    runs the Cost agent never recommended or applied anything.
- **H3: the UI sent a bare column map.** It sent `{col: type}` (from
  `localStorage.last_csv_schema = result.columns`), not
  `{columns, row_count, size_hint}`. As a result:
  - the Resource model always saw 0 rows and 0 columns;
  - the anomaly detector never stored a schema, so `schema_drift` could never
    fire.
- **H4: aggregation counts were always 0.** The Performance model features and
  the Resource heuristic counted aggregations from
  `stage["aggregations"]["agg_exprs"]`, but the planner emits
  `stage["aggregation"]["aggregations"]`.

### Changes

| File | Change | Why |
|---|---|---|
| `schema_utils.py` *(new)* | `normalize_run_schema(raw, contents, filename)` always returns `{columns: {col: type}, row_count, size_hint, ...}`. It accepts the full shape, a flat `{col: type}` map, or a `columns` list. With the uploaded bytes it **measures** `row_count` (`count_rows`: CSV via `csv.reader`, JSON array, NDJSON) and `size_hint`. Also `size_hint_for(nbytes)`. | One shape for every consumer. Numbers measured from the file beat whatever a client sends, and older clients or saved browser state still work. |
| `central_manager_agent/router.py` | `/run` normalizes the schema using the uploaded file; `/stream/start` normalizes the given schema. | Every run and stream starts from the canonical shape. |
| `central_manager_agent/manager.py` | `RunState` gains `schema` and `csv_size_bytes`, set in `execute_run`. New `_agent_plan(state)` gives a **copy** of the plan with `schema` / `csv_size_bytes` added; used for Performance prediction, cost optimize, auto-apply and actual-cost estimation. | Satisfies the agents' existing contract (the one the teammate's tests use) without changing `state.plan`, which goes to the semantic LLM (more context) and the executor (doesn't need it). |
| `monitor_agent/services/runtime_predictor.py` | Passes the stored run schema and size to the Performance agent. | Monitor predictions get the same inputs as the manager's. |
| `main.py` | `/api/schema/detect` uses `size_hint_for`. | The size buckets are defined in one place. |
| `resource_agent/ml/feature_spec.py` | New public `stage_agg_count(stage)`, built on the existing helpers that accept both shapes. | One aggregation counter. |
| `resource_agent/resource_agent.py`, `performance_prediction_agent/ml_predictor.py` | Use `stage_agg_count`. | H4. |
| `frontend/src/pages/PlannerTab.jsx` | Saves `{columns, row_count, size_hint, file_format}` to `last_csv_schema` instead of just `columns`. | H3 at the source. The Manager and Executor tabs send whatever is stored. |

### Verification

- **Normalizer:**
  - a flat map plus a CSV with a quoted comma gives the right columns, `row_count 3` and `small (< 5MB)`;
  - the detect shape and the list shape are handled;
  - row counts from a JSON array, NDJSON and CSV are right.
- **H4:** a 2-aggregation stage now counts 2 in both the Performance features and the Resource heuristic (was 0).
- **H2/H3, using a real plan from `adf_monitor.db` with a flat-map schema, as a client sends it:**
  - the Resource ML model sees `(50000 rows, 3 columns)` (was `(0, 0)`);
  - Performance `row_count` is 50000 (was 0);
  - Cost features get `row_count 50000, csv_size_mb 0.69` (was 0);
  - `state.plan` is unchanged.
- **H3 drift:** with a temp DB, two runs of the same pipeline with different columns give `[[], ['schema_drift']]`. Drift never fired before.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200;
  - `/api/schema/detect` and monitor predictions still work.

### Notes and decisions

- **The plan isn't changed.** The agents' `plan["schema"]` contract is kept and
  satisfied with a copy, because putting the schema into `state.plan` would also
  send it to the semantic LLM and persist it twice.
- **The Cost ML path still rarely applies anything.** Its inputs are right now,
  but the copy-stage DIU capacity check (M4) still rejects most candidates. That
  is fixed in stage 4.
- **Old runs are unchanged.** Runs saved before this change have no `schema` on
  their state, so monitor predictions for them fall back to `row_count=0`, as
  before.
