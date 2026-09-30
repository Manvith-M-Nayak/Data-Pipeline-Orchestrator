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
| 3 | Learning loops — what feeds the correction factors | H5, H6, M1, M2, M3, M9 | Done |
| 4 | Execution — concurrent-run isolation, executor issues | H7, H8, M4, run-id collisions, retries | Done |
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

---

## Stage 3 — Learning loops

**Commit message:** `fix(learning): train correction factors on raw predictions and executed runs only`

### Problems

| Id | Problem |
|---|---|
| H5 | Resource feedback was recorded for **every** run, including aborts and failures. `get_correction_factor` had no success filter. Real data: 11 of 49 rows came from failed runs, with ratios 0.005 and about 2.9. |
| M1 | The Resource ratio was measured against the **already-corrected** prediction, but the new factor was applied to the raw one, so it settled halfway (true 0.5 → 0.81). The fixed 50% damping was recomputed on every call, so it permanently under-corrected. |
| M2 | Same problem in the learning agent: the duration and cost factors targeted actual ÷ corrected prediction, so they converged to √(true ratio) (0.5 → 0.707). |
| M3 | "Actual duration" was the whole managed run (validation, semantic LLM ≤120 s, pre-checks, retry backoff), compared against execution-only predictions. |
| H6 | `estimate_actual_cost` ignored the actual duration: "actual" always equalled the estimate. |
| M9 | The formula-path "failure" verdict (which aborts the run) used post-run assurance results from **any** pipeline, and the history adjustment included aborted and retried runs. |

### Changes

| File | Change | Why |
|---|---|---|
| `central_manager_agent/manager.py` | **Execution time and feedback records:** <ul><li>Each Executor attempt is timed; a successful attempt sets `RunState.execution_s`.</li><li>`record_feedback(state, total_elapsed_s, ...)` logs `actual_duration_s = execution_s` (or elapsed time if the run never executed), plus `total_elapsed_s`, `executed` and `pipeline_key`.</li><li>Post-run assurance timing uses `execution_s`.</li></ul> **Resource feedback:** <ul><li>Recorded **only** for `completed` runs, using `execution_s`.</li><li>Passes the correction factor that was applied.</li></ul> | M3, H5. `pipeline_key` (the anomaly detector's identity) lets history checks compare the same pipeline. |
| `central_manager_agent/router.py` | The monitor's Databricks record and the anomaly detector use `execution_s` when the pipeline ran. | Same measure everywhere a duration is compared. |
| `resource_agent/resource_agent.py` | **Recording:** `record_actual(..., correction_factor=1.0)` stores `correction_factor` and `raw_predicted_duration_s`. **`get_correction_factor`:** <ul><li>skips rows with `success False`, and legacy rows whose run the manager logged as failed (cross-checked against `manager_feedback.jsonl`, so no data is deleted);</li><li>uses the ratio against the **raw** prediction;</li><li>returns the **median** of the last 10 ratios, bounded to [0.33, 3.0].</li></ul> The accuracy report uses the same function. | H5, M1. With raw ratios, the median is itself the right multiplier; permanent damping only under-corrects, and the median plus bounds resist outliers. |
| `learning_policy_agent/feedback_collector.py` | `normalize()` adds `raw_predicted_duration_s` (from `perf_uncorrected_total_s`) and `raw_estimated_cost_usd` (from `cost_uncorrected_estimated_usd`). Both fields were already logged. | M2 |
| `learning_policy_agent/error_analyzer.py` | `duration_ratio` and `cost_ratio` (what the factors target) use the **raw** values. `duration_ape` and `cost_ape` (MAPE) stay on the **corrected** values. | The factors converge to the true ratio, while MAPE still measures the error users actually saw. That is what the policy review compares before and after a change, so rollbacks keep working. |
| `cost_optimization_agent/cost_optimizer.py` | `estimate_actual_cost` scales each allocation's `duration_s` by actual ÷ `resource_plan.estimated_total_s` before costing. | H6. Allocation durations add up to the Resource estimate, so scaling them makes actual cost follow the real duration. |
| `performance_prediction_agent/performance_agent.py` | **History:** the formula path uses only executed runs (not `failed`, `executed` not false), and this pipeline's runs when at least 5 exist. **Failure rate:** uses only this pipeline's runs, and needs at least 3. New `_pipeline_key(plan)`. | M9 |

### Verification

- **M1, simulated with the real `ResourceAgent`** (temp feedback log): raw estimates 2× too long converge to **0.5**, and 2× too short to **2.0**. They were 0.809 and 1.281 before.
- **H5:** 6 legacy failed-run rows with ratio 0.01 alongside 4 good rows → factor 1.0; the bad rows are ignored.
- **M2, through the real `normalize` → `per_run_errors` → `PolicyEngine._gradual`:** converges to **0.5** and **2.0**. It was 0.707 and 1.414 before.
- **H6:** actual cost at 105 s, 210 s and 420 s gives $0.028, $0.055 and $0.110; the estimate is $0.055. Before, all three gave $0.055.
- **Manager flow, mocked executor and temp logs:**
  - A successful run logs `actual_duration_s = 0.3` (execution) against `total_elapsed_s = 2.5`. The total includes a simulated 1 s assurance step, and assurance timing uses 0.3.
  - A failed run is logged with `executed: false` and writes **no** Resource feedback rows.
  - New rows include `correction_factor` and `raw_predicted_duration_s`.
- **M9:** 8 failed-assurance runs of **another** pipeline → outcome `success`. The same records for **this** pipeline → `failure`.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean;
  - the backend has no tracebacks, and all 25 GET endpoints return 200;
  - the real feedback logs were not touched by the tests.

### Effect on live behaviour (expected)

- **Resource factors drop sharply.** On existing data they go from copy 1.032 / notebook 0.766 to **0.657 / 0.525**. Real stages run well under the raw heuristic estimates, and the old formula hid that by including failed runs and halving every correction. Duration estimates (and anything derived from them) will be noticeably shorter.
- **History baselines mix old and new measures for a while.** Anomaly `run_metrics` and monitor baselines hold older whole-run durations; new rows use execution time. The mixing fades as new runs replace the old window.
- **Pipeline history starts empty.** Legacy feedback has no `pipeline_key`, so the M9 failure check has no history for any pipeline until new runs are logged. That is the safe default: no aborts based on unrelated runs.

### Not changed

- The formula path's own history adjustment (`DAMPING = 0.4`) is left as is. It now acts only on the residual left after the (now correct) Resource correction, so its effect is small.

---

## Stage 4 — Execution

**Commit message:** `fix(executor): isolate concurrent runs and apply resource settings the executor controls`

### Problems

| Id | Problem |
|---|---|
| H7 | **No isolation between concurrent runs.** <ul><li>Batch runs purge every container, and container names default to fixed values (`raw`/`bronze`/`silver`).</li><li>One ADF pipeline, `Orchestrator_Copy_Pipeline`, is redefined by every run.</li><li>Notebooks sit at fixed `/Shared/unified_orchestrator/<stage>` paths, and every stream stage is `Stream_Ingest_Transform`.</li></ul> Two runs or stream ticks at once could wipe each other's data or run each other's notebook. |
| Low | `run_tag = int(time.time())`: runs starting in the same second shared `dbx-<tag>` (the monitor's primary key) and job names. |
| H8 | The Resource Agent's sizing and cost auto-apply never reached execution: the executor only reads each plan stage's `diu` / `shuffle_partitions`, but was handed the original plan. |
| M4 | Cost ML rejected every copy-stage DIU reduction: copy "memory" is just `diu × 1.5`, so a lower DIU always "failed" the capacity check. |
| Low | Deterministic failures (uncompilable or unsafe plan, missing references) were retried with 10 s + 30 s backoff. |
| Low | A failed container creation was only printed; the run failed later with a vaguer error. |

### Changes

| File | Change | Why |
|---|---|---|
| `executor_agent/executor.py` | **Split into two functions:** `execute_pipeline` is now a wrapper around the old body, renamed `_execute_pipeline`. <br>**Run tag:** unique, `"{epoch}{6 hex}"`. <br>**Resource lock:** `_ResourceLocks` holds every container the run touches, plus `adf-pipeline:Orchestrator_Copy_Pipeline` when it has copy stages. Runs sharing any resource wait (logged as "Waiting for another run using …"); disjoint runs, e.g. separate streams, still run in parallel. <br>**Notebooks:** go to a per-run folder `…/run-<tag>/<stage>`, deleted in a `finally` (`delete_workspace_path`) even on failure. | H7 and the id collision. The ADF pipeline name stays fixed because the monitor groups history by pipeline name, so copy runs serialize on it instead. |
| `executor_agent/executor.py` | Deterministic failures return `"retryable": False`: unsafe plan, no containers, missing dataset/container references, undefined datasets, uncompilable notebook. `create_blob_container` raises on any status other than 200/201/409. | Avoid pointless retries; fail with the real reason. |
| `central_manager_agent/manager.py` | `execute_with_retry` stops on `retryable: False` and reports the number of attempts actually made. | Retries only help transient failures. |
| `central_manager_agent/manager.py` | New `_execution_plan(state)`: a copy of the plan with the **final** resource plan applied where the executor can use it — copy-stage `diu` (1..`MAX_DIU`) and notebook/stream `shuffle_partitions`. Settings in the stage's `pinned_settings` are left alone. What was applied goes into `RunState.execution_settings` and a `RESOURCE SETTINGS APPLIED` log line, which says workers/node_type are advisory. The executor gets this copy; `state.plan` is unchanged. | H8: cost auto-apply and Resource sizing now affect DIU and shuffle. Workers and node type can't be set on serverless or an existing cluster (the `DATABRICKS_SPARK_VERSION`/`NODE_TYPE` settings aren't used anywhere), so they're labelled advisory instead of silently ignored. |
| `planner_agent/planner_common.py` | `apply_custom_settings` records `pinned_settings` on each stage it sets. `build_default_config` now goes through it too. | A user's explicit choice in the Planner must not be overridden by an agent's recommendation. |
| `cost_optimization_agent/cost_optimizer.py` | For copy stages, `_validated_ml_candidate` recomputes memory from the candidate DIU instead of comparing the old derived value. | M4 |

### Verification

- **Locks**, with the inner function mocked at 0.4 s per run:

  | Scenario | Time | Meaning |
  |---|---|---|
  | Two runs, same containers | 0.8 s | serialized |
  | Two streams, disjoint containers | 0.4 s | parallel |
  | Two copy runs, disjoint containers | 0.8 s | serialized on the copy pipeline |

- **Run tags and cleanup:**
  - run tags are unique (e.g. `1790756294d706cd`);
  - the per-run notebook folder is deleted after a run that raised.
- **Full executor flow**, cloud calls mocked at the lowest level:
  - run status `ok`, `dbx_run_id` has the unique tag, `stages_completed ['C', 'T']`, `rows_written 42`;
  - notebook at `…/run-<tag>/T`, and the folder was cleaned up;
  - a 403 on container creation raises `Creating container 'raw' failed: 403 …`.
- **Retries:** a non-retryable failure makes **1** executor attempt ("failed after 1 attempt(s)"); an ordinary failure still makes 3.
- **Execution plan:** with the user's `custom_settings = {"diu": 8}`, the copy stage is pinned and keeps DIU 8 despite a recommendation of 2; the notebook stage's shuffle goes 8 → 16 as recommended. `state.plan` is unchanged.
- **M4:** the teammate's `test_copy_deadline_changes_diu` passes (13/13). With a real plan, the candidate is no longer rejected at the capacity check.
- **Regressions:**
  - the integration test passes, and `ruff` is clean;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Notes and decisions

- **Serialize rather than rename.** Container names are user-visible (downloads, the monitor), so batch runs with the same containers now take turns rather than getting unique names. That limits throughput for identical pipelines, but guarantees correctness.
- **Open decision: the Cost agent still rarely applies anything.**
  - Its remaining rejection is deliberate policy: *never trade runtime without an explicit deadline*, enforced by the teammate's `test_no_deadline_does_not_allow_slowdown`.
  - The manager passes no deadline, so candidates that save money but run slower (e.g. DIU 2 → 1) are refused.
  - Passing the SLA target (900 s) as the deadline would enable those savings. That's a product decision, left for the user.
- **Workers and node type** would need Databricks job clusters (`new_cluster`) instead of serverless or an existing cluster. That changes compute cost and workspace requirements, so it's out of scope for a logic fix.
