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
| 5 | Learned expected duration — replaces the fixed time limit ("SLA") everywhere | design change (user request) | Done |
| 6 | User-facing bugs — model reload, missing-run detection, ragged CSV, streams, planner | M5, M6, M7, M8 (rest), 3 Low | Done |
| 7 | Remaining low-severity items | Low list (analytics, assurance order, contention, monitor duplicates, learning cycle, deploy gate, dead code, single-worker) | Done |
| 8 | Semantic (intent) check false flags — reported on `zv.csv` | user report | Done |
| 9 | Remove the auto-added `processed_time` column everywhere | user request | Done |
| 10 | Planner self-verifies (assurance as a library); intent check leaves the run path | design change (user request) | Done |
| 11 | Streaming: single-stage merge fixed + multi-stage streaming, user-selectable; AND/OR filters | bug + feature (user request) | Done |
| 12 | Frontend review: wrong fields, dead features, demo data, failure display, lint | 17 issues (user request) | Done |
| 13 | Redesign 1/4: design system, light + dark themes, sidebar shell | feature (user request) | Done |
| 13.1 | Text contrast; Executor tab follows runs started in the Central Manager | user request + 1 bug | Done |
| 14 | Redesign 2/4: pipeline flow + agent flow diagrams (React Flow), live run status | feature (user request) | Done |
| 14.1 | Reload consistency: one shared run store, live state everywhere, data file survives refresh | bugs (user report) | Done |
| 15 | Redesign 3/4: guided "New pipeline" flow (Data → Describe → Review → Run → Results) | feature (user request) | Done |
| 16 | Redesign 4a: Overview, Monitor (4 tabs), Resource/Performance/Cost rebuilt on shared components | feature (user request) | Done |
| 17 | Redesign 4b: Planner, Central Manager, Executor, Run Insights, streaming console rebuilt — redesign complete | feature (user request) | Done |

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

---

## Stage 5 — Learned expected duration (the fixed "SLA" limit is removed)

**Commit message:** `feat(performance): replace fixed SLA with a per-pipeline expected duration learned from past runs`

### Why

The project had two separate, unconnected "SLA" (time-limit) values:
- **A hard-coded 900 s prediction target** (`DEFAULT_SLA_TARGET_S`; `manager.predict_performance(sla_target_s=900)`). It only drove an "SLA breach risk" warning, and it wasn't configurable for managed runs.
- **An `SLA_SECONDS` env value for a `sla_breach` anomaly.** It was unset, so the anomaly never fired.

One fixed limit makes no sense when pipelines range from 1 KB test files to 200 MB datasets. At the user's request, the concept was removed entirely and replaced by what each pipeline normally does.

This stage also settles the open decision from stage 4: the Cost agent now gets a deadline, but a learned and bounded one.

### Design

`PerformancePredictionAgent._expected_duration(plan, predictions, predicted_total_s)`:

- **Comparable runs:** same `pipeline_key`; `final_status == "completed"`; `executed`; input size within **0.5–2×** of this run's (a 10× bigger file is not "slow"); and **not** a run where the Cost agent accepted a slower configuration (`cost_slowdown_applied`).
- **Expected duration** = 95th percentile of the last 20 comparable runs, needing at least 3 of them. Otherwise `expected_duration_basis = "insufficient_history"` and nothing is flagged.
- **`slower_than_usual`** = prediction > expected duration.
- **`max_acceptable_s`** (the Cost agent's ceiling) = `min(prediction × 1.2, expected)`, never below the prediction:
  - at most +20% extra runtime;
  - and only within the pipeline's usual range;
  - `None` without history, so the Cost agent keeps its fail-closed "never trade runtime" default.
- **Ratchet guard:** runs where the Cost agent accepted a slowdown are excluded from learning, so accepted slowdowns can't raise the target and permit bigger ones.

### Changes

| File | Change |
|---|---|
| `performance_prediction_agent/performance_agent.py` | **Removed:** `DEFAULT_SLA_TARGET_S`, the `sla_target_s` parameter, and the `sla_breach_risk` / `sla_target_s` output fields. **Added:** `_expected_duration()` and the constants `EXPECTED_MIN_RUNS`, `EXPECTED_WINDOW`, `EXPECTED_SIZE_BAND`, `COST_SLOWDOWN_MARGIN`. **Output fields:** `expected_duration_s`, `expected_duration_basis`, `expected_duration_runs`, `slower_than_usual`, `max_acceptable_s`. `predict()` wraps the ML/formula paths (now `_predict_core`) and adds these fields to every result. |
| `performance_prediction_agent/router.py` | `sla_target_s` request field removed. |
| `central_manager_agent/manager.py` | <ul><li>`predict_performance(state)` has no limit parameter, and logs "slower than usual (this pipeline normally takes ≤Xs)".</li><li>After the learning correction changes the prediction, the expected-duration fields are recomputed, so they describe the corrected number.</li><li>New `_cost_constraints(state)` → `{"deadline_s": max_acceptable_s}` or `{}`, used by `optimize_cost` and `auto_apply_cost_optimization`.</li><li>New `RunState.cost_slowdown_applied`, set when auto-apply lengthened any stage.</li><li>Feedback records `file_size_mb` and `cost_slowdown_applied`.</li></ul> |
| `monitor_agent/services/anomaly_detector.py` | `sla_breach` kind and `_sla_seconds()` / `SLA_SECONDS` removed. `slow_runtime` already compares each run with its own pipeline's p95. |
| `monitor_agent/routers/anomalies.py`, `services/db_service.py` | Kind list and comments updated. |
| `frontend/src/pages/ManagerTab.jsx`, `RunInsights.jsx` | "SLA breach risk" → **"vs usual duration"**: `✔ within usual (≤Xs)` / `⚠ slower (usually ≤Xs)` / `still learning (n/3 runs)`; `—` for runs saved before this change. |
| `frontend/src/pages/PerformancePredictionTab.jsx` | The runtime card shows "Usually ≤ X" (or "still learning"). The "SLA breach risk" card becomes **"Slower than usual"**. The reference table explains how the usual duration is learned. |
| `frontend/src/pages/AnomaliesPage.jsx`, `api.js`, `ResourceTab.jsx` | `sla_breach` filter chip removed; `slaTargetS` argument removed; wording updated. |
| Docs and comments | `unified/README.md` (the `SLA_SECONDS` row), `Performance_Prediction_Agent.md` (fields, API example, tuning section), `docs/RESPONSIBILITIES.md`, `PROJECT_STATUS_REPORT.md`, the teammate's `COST_MODEL_AUDIT.md` and `cost_optimization_agent/README.md` ("SLA validation" → "runtime validation"), Resource Agent comments, the Kaggle notebook note, and a `generate_cost_dataset.py` comment. |
| `scripts/seed_anomalies.py` | The demo `sla_breach` event becomes a `slow_runtime` event. |

**Deliberately left:** `Datasets/.../ORIGINAL_DATASETS_OVERVIEW.md` says the *external raw dataset* has "no SLA tiers". That describes third-party data, not a feature of this project. The earlier review and changelog docs mention SLA as history.

### Verification

- **`_expected_duration`**, with a temp feedback log:

  | Case | Result |
  |---|---|
  | No history | `insufficient_history`, no verdict, no ceiling |
  | 3 runs of 100 / 120 / 110 s, prediction 150 s | expected 120 s, **slower**, ceiling 150 (no slack: already above usual) |
  | Same history, prediction 105 s | within usual, ceiling 120 |
  | 3 runs on 10× bigger input | not comparable → insufficient |
  | Plus 5 cost-slowed runs of 900 s | ignored (expected stays 120) |
  | Failed/aborted runs; another pipeline's runs | ignored |

- **Manager flow, mocked executor and temp logs:**
  - First run (no history): the Cost agent gets `{}`, so no trade-offs.
  - After 3 comparable runs (400/420/410 s): prediction 207 s, expected 420 s → Cost gets `{"deadline_s": 248.4}`, i.e. 207 × 1.2, within the usual range.
  - A simulated slower pick sets `cost_slowdown_applied = True`, and the feedback records `file_size_mb` and `cost_slowdown_applied`.
- **API:** `POST /api/performance-prediction/predict` works without the old field and returns no `sla*` keys.
- **No SLA references are left** in project code or docs (repo-wide grep), apart from the history docs and the external dataset description.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Notes

- **Cost savings become possible, but only after history exists.** A new pipeline needs 3 completed runs of similar input size first; until then the Cost agent stays fail-closed.
- **No saved data changes.** Old saved runs still contain `sla_breach_risk`; the UI now ignores it.

---

## Stage 6 — User-facing bugs

**Commit message:** `fix: reload changed models, detect missing runs by status, handle ragged CSVs, assure and record streams`

### Changes

| Id | Problem | Change |
|---|---|---|
| M5 | ML predictors cached models per process forever: a retrained Performance model wasn't used until restart, and a failed load (file missing or half-written) was cached permanently. | New `model_files.files_signature(*paths)` (mtime + size). The Performance, Resource and Cost predictors store the signature they loaded (`_loaded_sig`) and reload when it changes. Unchanged files keep the cached object: one `stat` per file per prediction. |
| M6 | `ManagerTab` / `ExecutorTab` detected a missing run with `msg.startsWith("404")`, but `api.js` `req()` throws the backend `detail` ("Run not found"), so a missing run polled forever. | `req()` sets `err.status = res.status`; both pages check `e?.status === 404` (and 410). |
| M7 | `/api/schema/detect` returned 500 on a CSV row with fewer fields than the header (`DictReader` fills `None`; `_infer_type` called `.strip()` on it). | `_infer_type` skips `None`. Headers come from `reader.fieldnames` (the real header row), excluding the `None` key `DictReader` uses for extra fields. |
| M8 (rest) | Streams skipped assurance entirely, and stream ticks were never recorded in the monitor or anomaly detector. | `StreamManager.start` runs the deterministic structural assurance (no LLM) and rejects failures; the column check is waived only when the client sent no schema. New `_record_tick` gives each tick's result to `_notify_monitor` (a `Databricks_Streaming_Pipeline` record plus AI analysis) and to `detect_and_store` (per-pipeline metrics and anomalies). Failures there are logged, never fatal. |
| Low | Two drops of the same file in the same second overwrote each other (seconds-only blob name). | Blob name includes an 8-hex uuid. |
| Low | `validate_plan`'s "Planner used fallback" warning and the feedback log's `used_fallback` never fired: the flag was returned *next to* the config, and the plan travels alone. | The planner router also sets `config["used_fallback"]`. |
| Low | The Ollama planner ignored `container_names` sent without `num_containers` (Groq applied them). | The count is taken from the names when not given. |

### Verification

- **M5:**

  | Step | Result |
  |---|---|
  | Cost bundle file missing | unavailable |
  | File appears | available (was cached as failed forever before) |
  | File rewritten | new object loaded |
  | File unchanged | cached object reused |
  | Performance model file touched | reloaded |

- **M6:** the real `req()` from `api.js`, run under Node with a mocked 404 response, gives `message "Run not found"` and `status 404`. The new check matches; the old `startsWith("404")` check did not.
- **M7:** `/api/schema/detect` returns **200** for a short row (`id/name/amount`, 3 rows) and for a row with an extra field (columns `id/name`, the extra value ignored). Before: **500**.
- **Streams** (temp DB):
  - an unknown column in the filter → rejected ("Column references: …");
  - no schema but a bad `execution_order` → rejected ("Stage ordering: …");
  - no schema and otherwise valid → starts;
  - a mocked tick creates a monitor row (`Databricks_Streaming_Pipeline`, `Succeeded`) and 1 `run_metrics` row;
  - 3 same-second drops of `same.csv` → 3 distinct blob names.
- **Planner:**
  - the Ollama backend, with its HTTP call mocked, applies `["landing","clean","gold"]` given without a count;
  - `/api/planner/plan` returns `config.used_fallback` matching the top-level flag, both `True` and `False`.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Mistakes during this stage

- **Wrong attribute name.** My first version of `_structural_failures` read `c.name`, but `assurance_agent.result.CheckResult` calls the field `check`. The stream test raised `AttributeError`. The "unknown column" case had passed only because it failed before reaching that attribute. Fixed and re-tested.
- **Model timestamp touched.** The M5 test updated the timestamp of the real `performance_prediction_agent/models/duration_regressor.pkl` (via `os.utime`) to prove the reload. The contents are unchanged, and `.pkl` files are gitignored.

---

## Stage 7 — Remaining low-severity items

**Commit message:** `fix: order-aware assurance, accurate analytics, dedupe monitor analyses, fair retrain gate, remove dead cost code`

### Changes

| Finding | Change |
|---|---|
| **Combined analytics.** The duration ratio included failed/aborted runs; `feasible_plans` counted any run with a `complexity`; `anomalies_detected` used the legacy Groq log; manager runs were windowed but feedback wasn't. | `combined.py`: feedback is limited to the same run window; the duration ratio uses only successful, executed runs ("completed", or the legacy "feedback" status); `feasible_plans` / `infeasible_plans` count the new `resource_feasible` value, which the manager now records; `anomalies_detected` counts `anomaly_events` and `ai_slow_run_verdicts` counts the legacy log. |
| **Assurance column check ignored order.** A transform could use a column created later in the same stage, and a filter could use an aggregation alias. | `assurance_agent/structural.py` checks in execution order (transforms one at a time → filter → aggregation inputs; aliases become known only after that). Violations say where: `(transformation)`, `(filter)` or `(aggregation)`. |
| **Contention spill.** `resolve_contention` merged a spilled stage into the next group, which could depend on it. | The spilled stage gets its own group right after the current one. |
| **Duplicate monitor analyses.** The poll loop, `sync_historical` and backfill could analyze the same run twice. Backfill asked ADF about `dbx-` Databricks ids, which always failed. | `MonitorService._in_flight` claims a `run_id` before fetching or analyzing (`_analyze` → `_analyze_locked`). Backfill analyzes `dbx-` records from their stored `raw_json` instead of calling ADF. |
| **Learning cycle.** It ran synchronously on the event loop, and the `runs_since_cycle` counter was a non-atomic read-modify-write. | The manager runs `on_run_recorded` via `asyncio.to_thread`; `learning_agent._CYCLE_LOCK` serializes counting and the cycle trigger. |
| **Retrain deploy gate.** It compared MAE across different held-out sets. | `run_training.py` writes its held-out set to `data/holdout_test.csv`. `RetrainingManager._old_model_mae` scores the snapshotted old model on those same rows, and the gate compares like with like (the stored old MAE is kept as `mae_before_stored`). If the old model can't be scored there (different feature set), the new one is kept. |
| **Dead cost code.** Nine methods were unused since the fail-closed change. | Removed `_apply_cluster_downsize`, `_apply_node_downgrade`, `_apply_shuffle_tuning`, `_enforce_constraints_single` and `_suggest_*` ×5, plus the four constants only they used (`UTILIZATION_LOW_THRESHOLD`, `TINY_STAGE_THRESHOLD_S`, `OFF_PEAK_DISCOUNT`, `MERGE_SAVING_FACTOR`). A reference check across `unified/` and the teammate's root scripts showed 0 uses. |
| **Single-worker assumption.** `mark_interrupted_manager_runs` would fail another worker's live runs. | **Documented, not changed:** the `db_service` docstring and `unified/README.md` now say to run one server process. Run state, the executor's resource locks, streams and the monitor poll loop are all per-process, so real multi-worker support would need shared state (a database or Redis), which is a larger design change. |

### Verification

- **Analytics** (real DB, through the API): 10 duration samples, average ratio 0.55, failed runs excluded; `anomalies_detected 2` (events) and `ai_slow_run_verdicts 5`.
- **Assurance:**
  - all real plans still pass the column check;
  - a transform using a column defined later is flagged "unknown column 'z' (transformation)";
  - a filter on an aggregation alias is flagged "unknown column 'total' (filter)";
  - a correctly ordered stage passes.
- **Contention:** groups `[a,b,c,e]` then `[d]` become `[b,a,c]`, `[e]`, `[d]`: the spilled `e` sits alone, before `d`.
- **Monitor** (temp DB, mocked Groq/ADF):
  - three concurrent analyses of one run make 1 Groq call;
  - a `dbx-` record is backfilled and saved without ADF.
- **Learning:** 20 concurrent `on_run_recorded` calls give `runs_since_cycle = 20`.
- **Deploy gate:**
  - the old model's MAE is computed on a new held-out file (125.3 on synthetic rows);
  - a mismatched feature set returns `None` (no comparison);
  - a **copy** of `run_training.py` run in a temp directory (real models untouched) wrote `holdout_test.csv` with 21,000 rows × (19 features + `actual_duration_s`).
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Mistakes during this stage

- **Legacy status missed.** The first analytics filter accepted only `final_status == "completed"`, which dropped every legacy success: older records use `"feedback"`, from before bug #8 was fixed. That gave 0 duration samples. Now both are accepted. I checked the other new filters: `_expected_duration` requires `"completed"` but also needs `pipeline_key`, which legacy records don't have, so it's unaffected.
- **Bad test harness, twice.** One `asyncio.sleep` patch called itself recursively, and one contention scenario never actually spilled. Both were test bugs, fixed before relying on the results.

---

## All findings — where each was fixed

| Finding | Stage | Finding | Stage |
|---|---|---|---|
| H1 container-name injection | 1 | M1 resource factor convergence | 3 |
| H2 schema missing from plan | 2 | M2 learning factor convergence | 3 |
| H3 flat schema from UI | 2 | M3 whole-run duration | 3 |
| H4 aggregation count 0 | 2 | M4 copy-stage memory check | 4 |
| H5 failed runs in Resource feedback | 3 | M5 model cache never refreshed | 6 |
| H6 actual cost ignored duration | 3 | M6 frontend 404 detection | 6 |
| H7 no concurrent-run isolation | 4 | M7 ragged CSV 500 | 6 |
| H8 resource settings not executed | 4 (DIU/shuffle; workers/node advisory) | M8 streams unvalidated/unrecorded | 1 + 6 |
| Fixed "SLA" time limit | 5 (replaced by learned duration) | M9 cross-pipeline aborts | 3 |
| Low items | 4, 6, 7 (multi-worker: documented) | | |

### Honest limits that remain

- **Worker count and node type are still advisory.** Applying them needs Databricks job clusters (stage 4).
- **One server process only.** This is documented, not changed (stage 7).
- **The Cost agent needs history.** It only trades runtime for savings once a pipeline has 3 comparable completed runs (stage 5, by design).
- **Frontend changes were checked by build and by running `req()` under Node, not by clicking through a browser.** Cloud behaviour was tested with mocks; no real Azure/Databricks runs were made during these stages.

---

## Stage 8 — Semantic intent check: false flags

**Commit message:** `fix(assurance): give the intent check real columns and plan operations; drop hallucinated and no-op issues`

### Problem (reported by the user on `data/zv.csv`)

The check returned:

> FLAGGED — "The filter_condition 'predator = 1' is incorrect; it should be 'is_predator = 1'. Additionally, there are no transformations needed…"

Both claims were wrong:
- `zv.csv` has a column literally named `predator`.
- The "unnecessary transformation" was the `processed_time = currentTimestamp()` step the planner adds automatically (removed in stage 9).

**Root cause:** `check_intent` sent qwen2.5-7b only the user request and the raw plan JSON — never the dataset's columns — so the model invented a "correct" name. Live testing then showed three more failure modes of the 7B model:
- it reported a filter as **missing** while reading raw JSON that contained it;
- it flagged the **mandatory ingest copy** stage as "unnecessary";
- it "suggested" changes that were **already in the plan**: re-stating the same aggregation, renaming only an output column, or "Add `sum of price`" when the stage already computed it.

### Changes (`assurance_agent/semantic.py`, `assurance_agent/orchestrator.py`)

| Change | Why |
|---|---|
| `check_intent(..., schema=None)` sends `DATASET_COLUMNS` (name → type); the orchestrator passes the schema it already has. The prompt says names in that list are correct as written and must never be "corrected". | The model can't know real column names otherwise. |
| The raw plan JSON is replaced by `PLAN_OPERATIONS` (`plan_operations(plan)`): one plain line per stage. Filters read "filter (keep rows where): predator = 1"; aggregations read "aggregate — for each distinct legs: count the rows (output column n)". | The 7B model misread the raw JSON (containers, datasets, settings) and "count(*) as n"; the plain lines fixed both. |
| The ingest copy stage and empty stages are labelled `[infrastructure]`, and the prompt says they never count as a mismatch. | The model flagged the required ADF ingest as "unnecessary". |
| **Guard 1:** an issue whose quoted expressions reference a column that is in neither the data nor the plan (transform outputs, aggregation aliases) is discarded. SQL and English words (`group`, `by`, `distinct`, …) are never treated as columns. | Deterministic protection against hallucinated columns. |
| **Guard 2** (`_is_noop_suggestion`): an issue is discarded when its "fix" is a stage's existing operation (verbatim or with only the output column renamed), or an "add/include/keep X" where every quoted X is already in the plan's operations. | A fix that changes nothing is not a mismatch. |
| If every issue is discarded, `flagged` becomes `false`, and the reasoning says what was discarded and why (e.g. "…columns not in the data or the plan…: is_predator"). | Transparent: nothing is hidden silently. |

**Tried and reverted:** an extra prompt rule ("counting per X needs a group by") confused the 7B model: 12/21 correct, with correct plans now rejected. It was removed; the plain aggregation wording fixed the same case instead.

### Verification (live qwen2.5:7b-instruct via Ollama, 3 runs per case)

| Case | Expected | Result |
|---|---|---|
| Predators, `predator = 1` (**the reported case**) | pass | pass ×3 |
| Same plus an empty pass-through stage | pass | pass ×3 |
| Aquatic predators, two filters | pass | pass ×3 |
| Count per legs, correct group-by | pass | pass ×3 |
| Shipped example `valid_plan.json` (revenue per category and region) | pass | pass ×3 |
| Predators, but filters `aquatic = 1` | flag | flag ×3 |
| Predators and no venom, venom filter missing | flag | flag ×3 |
| Count per legs, but a filter instead of a group-by | flag | flag ×3 |
| Shipped example `intent_mismatch_plan.json` | flag | flag ×3 |

**27/27 correct.** The progression while fixing: 15/18 → 12/21 (reverted rule) → 15/21 → 18/21 → 21/21 → 27/27.

- **Replay of the reported result through the guard:** `flagged false`, "Discarded 1 issue(s)… is_predator".
- **The API end to end** (`POST /api/assurance/validate`, reported case): overall `pass`, intent "The plan matches the user request."
- **Genuine issues still get through:** "use 'aquatic = 1'", "add filter 'venomous = 0'", "group by tail instead", and "add `sum of discount`" are all kept.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean;
  - the backend has no tracebacks, and all 25 GET endpoints return 200;
  - the `assurance_agent` examples behave as designed.

### Notes

- **The check stays probabilistic and advisory.** It doesn't block runs unless `block_on_intent` is set. These results come from a fixed test set, not a guarantee.
- **Mistakes during testing:** one run passed `None` as the request (a regex in my test failed), and the model then invented requirements. It was re-run with the real request; the results above use that re-run.

---

## Stage 9 — Remove the automatic `processed_time` column

**Commit message:** `refactor(planner): stop auto-adding a processed_time column to every stage`

### Why

- **The planner added a column nobody asked for.** It appended `processed_time = currentTimestamp()` to every notebook stage, and the Groq prompt told the model to do the same. The notebook builder then **re-added** it after every aggregation.
- **The intent check rightly flagged it** as an unnecessary transformation. In the real run history it caused most of the false flags (10 of 11 runs were flagged, mostly citing "unnecessary transformations").
- **The user asked for it to be removed everywhere.**

### Design decision

- **Model output is cleaned, not just new plans.** The fine-tuned planner learned the habit from its training data (488 dataset rows contain it), so it still emits the transform. `strip_auto_timestamp(config, user_prompt)` removes exactly `processed_time = currentTimestamp()` from model output.
- **A real request is kept.** The step stays **unless the user's prompt asks for a timestamp / processing time**, so an explicit request isn't silently dropped.
- **Saved plans are not rewritten.** The executor runs a plan as given; plans saved before this change still contain the step until they are re-planned.

### Changes

| File | Change |
|---|---|
| `planner_agent/planner_common.py` | **New:** `strip_auto_timestamp()` (with the `_AUTO_TIMESTAMP_RE` / `_TIMESTAMP_REQUEST_RE` patterns). **Removed** the automatic timestamp from: default stages (`_build_stages`), the streaming default, redistribution, padding stages (`enforce_container_count`), and the `_structural_validate` append. The filters that ignored it are simplified (`_stage_op_load`, the copy-stage operations check). Pass-through stages now have `transformations: []`. |
| `planner_agent/ollama_planner.py`, `groq_planner.py` | Both call `strip_auto_timestamp` right after parsing model output. **The Groq prompt:** "ALWAYS include processed_time" → "Only add transformations the user asked for — never add extra columns"; the "Always add processed_time" rule is replaced; the "(and processed_time)" survivor note is removed. |
| `executor_agent/notebook_builder.py` | The batch and stream builders no longer skip it before an aggregation and no longer **re-add** it after `groupBy`. |
| `assurance_agent/structural.py` | `processed_time` is no longer treated as surviving an aggregation. |
| `frontend/src/pages/PlannerTab.jsx` | Shows every transform, so a requested timestamp is visible. "Pass-through" means no operations; the label reads "copies data unchanged". |
| `assurance_agent/examples/*.json` (5 plans), root `README.md` | Timestamp transforms removed from the example plans. Minimal text edits — a first attempt re-serialized the JSON (154-line diff), so the files were restored from `HEAD` via `git show` and edited as text instead (10-line diff). |
| **Training tools** (`planner_agent/training/`) | `generate_dataset.py`: `processed_time_prob` and the stamping code removed. `validate_dataset.py`: the `timestamp_stamp` classification removed. `build_finetune_notebook.py`: the fine-tune prompt says "never add extra columns" instead of "ALWAYS add processed_time". `eval_live_planner.py` (local): the `processed_time` pass criterion is now `no_auto_timestamp`. `README.md`: config row removed. |
| `finetune_qwen_planner.ipynb` | Regenerated from the builder. The diff also includes the dataset-path and JSONL-loading fixes made to the builder in an earlier session (the notebook had never been regenerated after them). |

### Verification

- **Planner:**
  - the default plan contains no timestamp;
  - Ollama output that includes the timestamp (mocked HTTP), prompt "keep predators and uppercase the name" → only `upper_name = upper(name)` is kept;
  - the prompt "…add a processing time timestamp" → the timestamp is **kept**;
  - padded to 5 containers → no timestamp; the two added stages are true pass-throughs (`[]`);
  - the streaming conversion contains no timestamp.
- **Notebook:** the aggregation notebook no longer references `processed_time` and compiles.
- **Training data:** generating 400 rows gives **0** with `processed_time`, and the validator passes (0 violations). The builder's `--verify` passes.
- **Assurance:**
  - every example still passes or fails for its intended reason;
  - live qwen: your `zv.csv` case passes ×2, `valid_plan` passes ×2, `intent_mismatch_plan` is flagged ×2.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Not changed (flagged for the user)

- **Old prototype folders at the repo root.** `py_files/`, `databricks/`, `planner_finetune/` and `resource_finetune/` still add `processed_time`, but no code in `unified/` imports them; only three docs mention them.
- **The fine-tuned adapter and existing datasets are unchanged.** The model keeps the habit until it's retrained on newly generated data; the planner strips its output meanwhile.

---

## Stage 10 — The Planner verifies its own plan (assurance becomes a library)

**Commit message:** `feat(planner): self-verify plans with the assurance library and re-plan once; manager keeps a structural gate`

### Why (user decision)

- **Assurance "contradicted" the Planner after the fact.** It ran as a separate step, so plans were flagged after being handed over; the real run history shows 10 of 11 runs flagged, all false alarms.
- **A disagreement is only useful if something acts on it,** so the fix loop moved into planning, where it can.
- **Assurance is kept as a library,** not deleted: the checks are needed either way. Plans can still be hand-edited in the UI or sent straight to `/api/manager/run`, so the run itself keeps a cheap, deterministic gate.

### Design

**Planner** — new `planner_agent/self_check.py`, `plan_with_verification(build, schema, prompt)`:
1. `build(review_feedback)` produces a plan: the model call, all deterministic repairs, the user's execution groups, and the streaming conversion. The plan is verified in its final form.
2. It verifies with `AssuranceAgent().assure(..., run_semantic=True)`: structural rules plus the intent check (with the stage 8 fixes).
3. If a structural check failed or the intent check reported valid issues, it re-plans **once** (`MAX_REPLANS = 1`) with the problems as `REVIEW FEEDBACK`.
4. **The deterministic fallback** (model unreachable) is verified but never re-planned: a retry would hit the same unreachable model.
5. **The best attempt wins:** fewest structural failures, then fewest intent issues; on a tie, the later attempt, since it was made with the feedback.
6. **It returns a `verification` report:** `verified`, `attempts`, `replanned`, per-attempt `history`, and `final` (the full AssuranceResult, in the same shape as `/api/assurance/validate`).

**Feedback goes only to the model.** `decide_pipeline_config(..., review_feedback=None)` in both backends appends it to the LLM message only. Every deterministic step (container count from numbered stages, filter restoration, stage naming, timestamp stripping) keeps using the user's original prompt, so issue text such as "Stage 7" can't change the plan's shape.

**Manager** — `run_plan_assurance` runs **structural rules only** (`run_semantic = False`), still as a hard gate. That removes 3–5 s (up to a 120 s timeout) from every run, and plans are no longer re-judged at run time.

**UI** (`PlannerTab.jsx`):
- The Planner's own verification is shown as soon as a plan arrives: "Planner self-check: verified on the first attempt / after re-planning (N attempts)".
- "Validate Plan" is now **"Re-check Plan"**, an on-demand check after manual edits.
- The "Fix & Re-plan" button still appears when advisories remain.

### Changes

| File | Change |
|---|---|
| `planner_agent/self_check.py` *(new)* | The verify → re-plan loop and the report. |
| `planner_agent/router.py` | `/api/planner/plan` builds through `_build`, runs `plan_with_verification` in the threadpool, and returns `verification`. |
| `planner_agent/ollama_planner.py`, `groq_planner.py` | `review_feedback` parameter, used only in the model message. |
| `central_manager_agent/manager.py` | The plan phase is a structural gate only; comments and log label updated. |
| `frontend/src/pages/PlannerTab.jsx` | Shows the self-check; the button is renamed. |
| `unified/README.md`, `docs/RESPONSIBILITIES.md` | Assurance described as a library used by the Planner and the Manager gate. |

### Verification

- **Loop** (mocked build, mocked intent check):

  | Scenario | Result |
  |---|---|
  | Passes first time | 1 attempt |
  | Unknown column, then fixed | 2 attempts, the fixed plan is returned, and the feedback contained the violation |
  | Fallback plan | 1 attempt, no retry |
  | Fails twice | 2 attempts, `verified False`, history recorded |
  | Intent issue | the retry feedback carries the issue |

- **Feedback routing** (mocked Ollama HTTP): the model message contains `REVIEW FEEDBACK`, and feedback mentioning "Stage 7" / "step 9" did **not** change the container count (still 3).
- **Manager:** with the intent check replaced by a function that fails if called, the plan gate passed and the semantic layer did not run.
- **Live, end to end** (real fine-tuned planner + qwen, `/api/planner/plan`, `zv.csv` columns, "Keep only the animals that are predators"):
  - **batch:** verified on the first attempt; stages ingest → `predator = 1`; no `processed_time`; intent "The plan matches the user request." (33 s);
  - **streaming:** verified; one stream stage with `predator = 1` (34 s).
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - all 6 assurance examples behave as designed;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Limits

- **Planning is slower when a re-plan happens:** one more model call (about 20–30 s with the local planner).
- **A re-plan can't fix a persistent model mistake.** The best attempt is returned with `verified: false` and its open issues shown, and the user can still "Fix & Re-plan" or edit.
- **The re-plan path wasn't triggered in the live runs.** The model produced a correct plan first time; the retry path is covered by the mocked tests.

---

## Stage 11 — Streaming layouts (single / multi-stage) and AND/OR filters

**Commit message:** `feat(streaming): selectable single or multi-stage streams; fix merged filters; support AND/OR filters`

### Problems

- **Silent data loss (bug).** `to_streaming_plan` squeezed every stage into one stream stage but kept only the **first** filter, silently dropping the rest.
  - Reproduced: a plan with `aquatic = 1` then `predator = 1` became a stream with only `aquatic = 1`.
- **Filters with `AND`/`OR` never compiled (bug).** The filter converter rejected them outright, including the shipped `valid_plan.json` (`price > 0 AND quantity > 0`). Any plan using them failed with "Plan cannot be compiled to a notebook".
- **Feature request:** a way to have multiple stages in a streaming pipeline, doing different tasks.

### Changes

| File | Change |
|---|---|
| `executor_agent/notebook_builder.py` | New `_convert_boolean` plus `_split_top` / `_strip_outer_parens`. Handles AND / OR with SQL precedence (AND binds tighter), brackets, BETWEEN's own `and`, and `and` inside quoted values. Each condition goes through the existing escaping converter, and the whole result through the stage 1 safety validator. |
| `planner_agent/planner_common.py` | `to_streaming_plan(config, container_names, layout)` supports `"single"` and `"multi"`. **Single:** all transforms in order and **all** filters combined with AND. `single_stage_blockers()` detects when one stage would change the results (more than one aggregation, operations after an aggregation, a filter using a column a later stage changes); the plan is then built multi-stage, and `streaming.layout_note` explains why. It also records `streaming.merged_steps`. **Multi:** one stream stage per step that does work, chained `src → s1 → … → sink`, each with its own checkpoint container (`{sink}-chk`), run in order. Per-plan unique container names as before. `streaming.sink_container` is the last sink. |
| `planner_agent/router.py` | Accepts `stream_layout` (`single` by default) and passes it through the self-checked build. |
| `central_manager_agent/stream_manager.py` | A stream's output is the **last** stream stage's sink (it used the first stage's, which is wrong for chains). |
| `assurance_agent/semantic.py` | **Filters:** a combined AND filter is described as separate filter steps. **Single-stage streams:** the `merged_steps` are judged one by one, as the user requested them. The 7B model otherwise flagged every correct merge ("both stages combined into one filter"), even with a prompt note or a line added to the request — both tried and removed. |
| `frontend/src/pages/PlannerTab.jsx` | **"Streaming Stages"** selector when Streaming is chosen: *Single stage* ("one Databricks job per run, fastest/cheapest") or *Multiple stages* ("one incremental stage per step, own checkpoints"). A plan badge shows the layout, the layout note is displayed, and a caveat explains that aggregations cover each run's new rows only. |

### How multi-stage streaming processes new data

Each stream stage keeps a manifest of the files it has already processed, in its own checkpoint container, and writes a uniquely named part file per run. On each run:
- stage 1 processes new source files;
- stage 2 then sees exactly stage 1's new part files, and so on.

New data flows through the whole chain on every run.

### Verification

- **Filter converter:**
  - `price > 0 AND quantity > 0` → `(col("price") > 0) & (col("quantity") > 0)`;
  - `(aquatic = 1 or fins = 1) and predator = 1` → correct precedence;
  - `a = 1 or b = 2 and c = 3` → `a | (b & c)`;
  - `price between 10 and 50 and region = 'EU'` keeps the BETWEEN;
  - `name = 'Tom and Jerry'` is not split;
  - the stage 1 injection string is still an inert literal.
- **Conversion**, across 3 plans × 2 layouts:

  | Plan | Single | Multi |
  |---|---|---|
  | 2 filters + count | 1 stage, filter `(aquatic = 1) AND (predator = 1)` | 3 chained stages, unique checkpoints |
  | Aggregation in the middle | single requested → multi built, with a note | 2 chained stages |
  | Filter uses a later column | multi with a note | 2 chained stages |

  All pass the stage 1 safety checks, and every generated notebook compiles. Assurance passes the first two plans and correctly fails the third: that plan is genuinely broken (the filter uses `legs2` before it exists).
- **Stream manager and executor** (mocked cloud): a 3-stage stream uses the chain's last sink; a tick runs `Stream_S1 → Stream_S2 → Stream_S3` in order; the monitor records the run.
- **Live** (real planner + qwen, `/api/planner/plan`, zoo columns, "Stage 1: keep only aquatic animals. Stage 2: keep only predators."):
  - **multi:** two chained stages, verified on attempt 1 (29 s);
  - **single:** one stage with the combined filter, verified on attempt 1, "The plan matches the user request exactly." (53 s). Before the `merged_steps` fix it was flagged, and the pointless re-plan also produced a worse plan.
- **Intent check, live, 3 runs each:** single-stage correct merge passes ×3, a wrong second step is flagged ×3, multi passes ×3.
- **9-case regression suite:** 24/27 against my original expectations. The 3 "misses" are all `valid_plan.json`, which is now flagged for aggregations nobody asked for (average quantity, order count) and extra filters. On inspection that flag is **correct**: the request is only "total revenue per category and region", and the example is "valid" structurally, not intent-wise. With that expectation corrected: **27/27**.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the assurance examples behave as designed;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Limits

- **Multi-stage runs cost more.** Each run executes one Databricks job per stage in sequence (about 90 s cold start each).
- **Streaming aggregations cover only each run's new rows**, in both layouts. Cumulative results would need a stateful design (not in scope).
- **OR is supported in filters, but single-stage merging only ever combines filters with AND** (steps apply one after another).


## Stage 12 — Frontend review

The user asked for a check of the frontend for any issues. I read every file in
`frontend/src` against the backend responses, linted with React rules
(`eslint:recommended` + `react` + `react-hooks`, run from a scratch folder so the
project gets no new config or dependencies), then loaded every page in the browser
against the running backend.

### Bugs fixed

| # | Where | Problem | Fix |
|---|---|---|---|
| 1 | `ManagerTab` phase bar | A failed run never showed the red ✗: `isFailed` required `isActive`, which is forced false for finished runs. | Mark the phase at the failure index. |
| 2 | Manager + Run Insights | `record_feedback()` enters the "feedback" phase even for failed runs, so every failure looked like it failed in *Feedback* and the step read "Recording outcome to feedback log" instead of the error. | Backend: `record_feedback` restores the failed run's `phase`/`step` after logging. Frontend: for runs saved before this fix, take the last phase before the trailing `PHASE:FEEDBACK`. |
| 3 | `RunInsights` run detail | Cost recommendations read `r.action`; the field is `change`, so each line rendered as "— ~12%". | Use `r.change`. |
| 4 | `RunInsights` download | Plain `<a href>` cannot send `x-api-key`; it breaks once `API_KEY` is set. | Use `executor.download` (fetch + blob) like the other tabs. |
| 5 | `RunInsights` stats | `avg_error_pct !== null` showed "undefined%" when the field was missing; an analytics load failure silently showed zeros. | `!= null`; show the load error. |
| 6 | `CostOptimizationTab` | "Run cost optimization" optimized a **hard-coded demo plan** (made-up stages, 500k rows, 70 MB), not the user's pipeline. The assumptions text claimed a 30% off-peak discount that the cost model does not have, and the subtitle still said "never breaks deadlines". | Show the Cost agent result the Manager already computes for each run (newest run that has one, with its run id and streaming advice). Assumptions are rendered from `GET /cost-optimization/node-rates`. Subtitle describes the learned-duration limit. |
| 7 | `ResourceTab` | `liveRp` was never set, so "Stage Allocations" never appeared and "Check live re-allocation" always answered "No active resource plan". The limits card was hard-coded. | Use the resource plan of the run shown in the Manager tab (context); limits come from `GET /resource/limits`. |
| 8 | `PerformancePredictionTab` | Only looked at the newest run; if that run failed before prediction the tab showed "no data". Durations printed raw decimals (e.g. `12.345s`). | Scan the 10 newest runs for one with a prediction; round durations. |
| 9 | `StreamingConsole` | A drop or click while a tick ran started a second tick; re-picking the same file did nothing (input value never reset); polling continued forever after Stop; `busy` could stay stuck; leaving the Manager tab lost the stream id while the stream stayed active on the backend. | Guard on `busy`; reset the input; poll only while active; clear `busy` on stop; remember the stream id in `localStorage`, restore it on mount, and clear it if the server returns 404. Added "New stream" after stopping. |
| 10 | `PlannerTab` | If schema detection failed, the file stayed selected and the card showed "undefined columns · undefined rows". Re-selecting the same file after "Change" did nothing. After a reload, the mode buttons reset to Batch even for a restored streaming plan, so "Re-generate" silently produced a batch plan. `detected.preview[0]` crashed on older saved schemas. Cmd+Enter only (no Ctrl+Enter). | Clear the file on failure; reset the input; seed mode/layout from the restored plan; guard `preview`/`columns`; accept Ctrl+Enter. `reset()` now clears the assurance result too. |
| 11 | `ExecutorTab` | "Change" / drop accepted any file type (or `undefined`) and never told the user the plan was built from a different file. | Validate the extension. The Planner now stores `file_name` in `last_csv_schema`; the Executor warns when the chosen file differs. |
| 12 | `HomePage` | An unreachable backend showed "No pipeline data yet". The "Sync" link only logged to the console. The saved plan was re-read from `localStorage` with focus/storage listeners. | Show the load error; the sync link shows its own progress and errors; read the plan from `AppContext`. |
| 13 | `LiveDashboard`, Manager/Executor download | `alert()` blocks the page (and browser automation). | Inline error messages. |
| 14 | `PredictionsPage` | After switching pipeline, the previous pipeline's prediction stayed on screen under the new name. | Clear the result on change. |
| 15 | `AnomaliesPage` | `replace("_", " ")` only replaces the first underscore; `e.kind` unguarded. | `replaceAll`; guard. |
| 16 | `LogsPage` | Empty-state text pointed to a "sidebar" button that does not exist. | Points to the header's "Sync (48h)". |
| 17 | `api.js` download | `URL.revokeObjectURL` ran right after `click()`, which can cancel the download in Firefox. | Revoke after 1 s. |

Lint: 19 findings → 0 (unused imports/variables, empty `catch {}` blocks given a
comment, one intentional mount-only effect marked).

### Verification

- **Lint and build:** ESLint (React rules) reports 0 problems, and `vite build` passes.
- **Browser**, against the live backend: every route and all four Monitor sub-tabs load with no console errors.
  - **Resource** now shows the Manager run's allocations.
  - **Cost** shows run `dcca01c0`'s real result ($0.0318, with streaming advice) and the backend assumptions.
  - **Performance** shows the latest real prediction.
  - **Failed run `56fa5439`:** Run Insights and the Manager phase bar now mark *executing* in red, not *feedback*.
- **Backend change:** checked directly. A failed state keeps `executing` / `Failed: boom`, and the decision log still contains `PHASE:FEEDBACK`. A completed run is unchanged. The test wrote to a temp dir; the real feedback log is untouched.

### Not changed

- **`npm audit`:** `react-router` has a moderate advisory with a non-breaking fix (`npm audit fix`). The `vite`/`esbuild` advisories are dev-server only and need a major Vite upgrade. Left for the user to decide, since it changes `package-lock.json`.
- **Layout:** fixed-column grids are not responsive on narrow screens. That is cosmetic, and out of scope for a bug pass.
- **Duplicated run flow:** Executor and Manager tabs both start runs. Both go through the Manager, so behaviour is consistent; merging them is a UX decision.


## Stage 13 — Redesign, part 1: design system and light theme

The user asked for a light theme and a much better frontend, one that doesn't look
AI-generated, plus flows. Their decisions, via the question tool:
- a **light/dark toggle** (the app follows the OS until the user picks one);
- **all three flows** (pipeline graph, agent flow, guided user journey);
- a **full redesign**;
- **React Flow is the only new library**, and no UI kit.

The redesign is split into stages:
- **13:** foundation (this stage);
- **14:** flow diagrams;
- **15:** guided flow;
- **16 onward:** rebuild each page on the shared components.

### Design direction

- **Warm neutrals.** Paper-white light theme and a warm charcoal dark theme, not the usual slate blue. Everything comes from one set of colour variables.
- **One ink-blue accent.** Status colours (ok/warn/bad) are muted, so dense operational screens stay calm.
- **Type:** IBM Plex Sans / Plex Mono, with tabular numbers in tables.
- **Deliberately avoided:** purple gradients, glassmorphism, emoji, oversized hero text.

### What changed

| File | Change |
|---|---|
| `src/index.css` | All design tokens (colours, radii, shadows, focus ring) for light and dark. `[data-theme]` overrides the OS preference. Base typography, focus-visible rings, scrollbars, reduced-motion support. |
| `index.html` | Plex fonts. A tiny inline script applies the saved theme before first paint (no flash). The title is now "Pipeline Orchestrator". |
| `src/ui/theme.js` | `useTheme()` (stored choice, otherwise follows the OS live), `tint()` (translucent colour via `color-mix`, which works with CSS variables), `TONES`. |
| `src/ui/ui.css`, `src/ui/components.jsx` | Shared components with real hover/focus/disabled states: `Button`, `Card`, `PageHeader`, `Badge`, `Dot`, `Alert`, `Stat`, `Empty`, `Segmented`, `Tabs`, `Field`, `KV`, `Spinner`, plus shell/table/grid classes. Responsive below 900 px. |
| `src/App.jsx` | A new shell replaces the 9 crowded top tabs. The sidebar is grouped by the work (Workspace / Build / Run / Observe / Agents), with backend status and the theme toggle. The top bar shows the section, the page and "Sync ADF runs". |
| `src/ErrorBoundary.jsx` | Uses `Alert` / `Button`. |
| All 14 pages | Every hard-coded colour (51 distinct hex values) is mapped to a token by a script. Borders, text and background tints each have their own mapping, so the same hex can land in different roles. The `color + "22"` alpha trick (which breaks with variables) is converted to `color-mix`. Primary buttons now use the accent. |

### Verification

- **Lint and build:** ESLint (React rules) reports 0 problems, and `vite build` passes.
- **Leftover colours:** `grep` finds no hard-coded hex colour left in `pages/`.
- **Browser, light:** Overview, Central Manager, Run Insights and Planner.
- **Browser, dark:** Run Insights and Planner, after the toggle; the choice persists across reloads.
- **Layout:** there is no horizontal overflow (checked `scrollWidth` against the viewport).

### Known and planned

The pages still use their own inline layouts (headers with pill badges, varying
widths). Stages 16 onward rebuild them on the shared components; this stage only
guarantees that both themes are correct everywhere.


## Stage 13.1 — Readable text; Executor follows Central Manager runs

The user asked for two things:
- "make the text more visible";
- when a pipeline is executing from the Central Manager, show it in the Executor tab too.

### Text contrast

Measured against the three backgrounds (surface, page, inset), worst case:

| Token | Light before → after | Dark before → after |
|---|---|---|
| `--text-2` | 8.65 → 11.36 : 1 | 9.80 → 12.24 : 1 |
| `--text-3` | 4.52 → 6.71 : 1 | 5.09 → 7.98 : 1 |
| `--text-4` | **2.58** → 4.71 : 1 | **2.88** → 5.78 : 1 |

`--text-4` previously failed WCAG AA (4.5 : 1). Much of the page text maps to it
(the old `#475569`), so it looked washed out. Every text level now passes AA in both
themes.

Other readability fixes:
- **Small labels:** 24 pages used 9–10 px labels; these are raised to 11 px.
- **Executor pending steps:** opacity raised from 0.3 to 0.6.

### Executor follows Manager runs

Every run goes through the Central Manager, so its run list is the source of truth.

- **Attaching:** while the Executor tab is not running anything itself, it polls `GET /manager/runs` every 4 s. When a run is in progress, it attaches to it: it shows the status immediately, polls every 3 s, advances the step list, and shows the result and download link when the run finishes.
- **Banner:** a run the tab did not start itself shows "Following run xxxxxxxx, started from the Central Manager", with a link to open it in the Manager.
- **No plan in this browser:** the Execution card now also shows when this browser holds no plan but a run is being followed.
- **"Run again"** is hidden while a run is live.
- **Refactor:** the poll tick is split into `_applyStatus()`, used by both the poller and attaching.
- **Other direction:** the Manager tab already attached to runs started from the Executor.

**Bug found while testing.** The step dots never rendered. The failed-step override
set `background: undefined`, which wiped the dot colour on every row. Now it overrides
only for the failed step.

### Verification

- **Lint and build:** ESLint reports 0 problems, and `vite build` passes.
- **Browser:** tested without starting a real cloud run. I stubbed `fetch` for `/api/manager/runs` and `/api/manager/status/<id>` with a fake live run.
  - **Live:** the Executor attached within one tick, showed the banner, and advanced to "Running notebook stage group 1/1" with earlier steps done.
  - **Completion:** after switching the stub to `completed`, it showed "Pipeline completed successfully", Download output and Run again.
  - **Dots:** now visible, green when done.
  - **Cleanup:** the test's `exec_*` keys were removed from `localStorage` afterwards.
- **Light theme:** Run Insights, labels readable.


## Stage 14 — Redesign, part 2: flow diagrams

The second redesign stage adds two diagrams, both built with React Flow
(`@xyflow/react` 12, the only new dependency the user approved). Both follow the app
theme, and both update live during a run.

### Pipeline flow (`src/flows/PipelineFlow.jsx`)

The plan is drawn as a data-flow graph:

    input file → ADF copy → step 1 (parallel stages stacked) → step 2 → … → output

- **Nodes.** Each shows the stage type (coloured rail), its name, and what it does: transforms, filter, group-by/aggregations, "incremental · checkpointed" for streams, and a warning for pass-through stages.
- **Edges.** They follow the data: a stage's source is the container another stage writes, and the container name is shown on the edge. Copy stages record ADF *dataset* names (`DS_Transform`), so datasets are matched to containers with the planner's naming rule (`planner_common._dataset_name`). Without container info, each column feeds the next.
- **Columns.** They follow the executor's grouping exactly (`execution_groups` filtered to compute stages, plus any stage the groups missed).

### Agent flow (`src/flows/AgentFlow.jsx`)

It shows how a run moves through the agents:

    Planner → Validate → Verify plan → {Resource, Performance, Cost} → Execute → Verify output → Learn

The Monitor hangs off Execute with a dashed "observes" link. Without a run, the same
map is shown as "How a run works".

### Live status (`src/flows/status.js`)

The statuses come from what the backend already reports; no new API was needed.

- **Agents:** from the manager phase.
- **Stages:** from the executor's own progress text. Examples: "Waiting for ADF copy pipeline…" means the copy stage is running; "Running stage group 2/3 (parallel): A, B" means A and B are running and earlier groups are done; "Monitoring Databricks run … (stage: X)" means X is running.
- **Finished stages:** from `executor_result.stages_completed` and `stages`.
- **Failures.** The failed stage is the one the error message names, by stage name or by quoting its filter text. Otherwise it is the first unfinished stage, but only if some stage had already finished. A failure before any stage ran (for example, notebook code generation) blames no stage, and the Executor agent node shows it instead. *Mistake caught in the browser:* the first version always blamed the first stage. That painted the ADF copy red for run `56fa5439`, which actually failed while compiling a notebook's filter.
- **Old runs:** runs saved before stage 12's backend fix end in the "feedback" phase; the failed phase is taken from the decision log.

### Where they appear

| Page | Change |
|---|---|
| Planner | The plan's stage-card grid is replaced by the pipeline flow. |
| Central Manager | The phase bar is replaced by the agent flow ("Orchestration"). A new "Pipeline" card shows the live pipeline flow. The empty state shows "How a run works". |
| Executor | Live pipeline flow above the step list, for its own runs and for runs it follows. |
| Run Insights (run detail) | The text phase timeline is replaced by the agent flow plus that run's pipeline flow, with final statuses. |

### Other details

- **Read-only diagrams:** no dragging, and the page still scrolls over them; zoom buttons are on the pipeline graph.
- **Alignment:** nodes are vertically centred (`nodeOrigin`), so edges stay straight.
- **Names:** long snake_case names wrap at underscores.
- **Bundle:** React Flow is loaded only with the pages that use it (about 59 KB gzipped).

### Verification

- **Tests:** `node src/flows/status.test.mjs` has 15 cases, all passing. They cover idle, pre-checks, copy running, a parallel group, a single stage, completed, failure by error-named stage, failure by quoted filter, a failure before any stage ran, the legacy "feedback" phase, and agent statuses.
- **Lint and build:** ESLint reports 0 problems, and `vite build` passes.
- **Browser:**
  - **Manager:** a completed run shows every agent done.
  - **Run Insights, failed run `56fa5439`:** Execute shows red, and the notebook stage whose filter failed is red while the copy stage is waiting.
  - **Executor live:** tested with a stubbed live run. The copy stage is done, the notebook stage is running with an animated edge, and the legend is visible.
  - **Planner, dark theme:** the computed colours come from the tokens.
- **Browser caveat:** the screenshot tool mis-captured scrolled pages, so the dark-theme check used computed styles instead.


## Stage 14.1 — After a reload, everything agrees

User report: "When I refresh the page, it's like half of the data is there and the other
is not. And the status of the pipeline shows running in one place and completed in the
other."

### Root causes

1. **Backend: two copies of a run.** A live run is kept in memory, but saved to SQLite only at start (`validating`) and at the end. `/manager/status` reads memory, while Run Insights (`/manager/combined/run`, `/analytics`) read SQLite only. For the whole run, Run Insights showed "validating" while the Manager showed "executing".
2. **Frontend: one saved copy per tab.** The Manager (`mgr_state`) and the Executor (`exec_job_state`, `exec_step`) each saved their own snapshot and polled on their own. After a reload each tab showed its own stale copy, and the Executor resumed only if its copy said "running". Live Monitor, Run Insights and the Overview each fetched their own lists at different times, and Run Insights loaded once and never refreshed.
3. **The schema was duplicated.** It lived in `planner_schema` and in `last_csv_schema`, which only the Planner wrote.
4. **"Half the data".** A reload kept the plan and schema but dropped the data file (a `File` cannot go into localStorage), and dropped the Planner's self-check result.

### Fixes

**Backend**
- `CentralManager._enter()` also saves the run in the background at every phase change (`_persist_soon`, tracked tasks, skipped safely off the event loop). SQLite is now at most one phase behind.
- `combined.py`: run detail prefers the live in-memory state (the same source as `/status`). Analytics overlays the live status, phase and step on SQLite rows, and adds live runs not yet saved.

**Frontend: one shared run store** (`AppContext`)
- **One copy:** only `run_id` (plus how it was started) is stored. The run state always comes from the server: fetched straight after a reload, then polled every 2.5 s while live.
- **One run list:** `/manager/runs` every 5 s. When the current run is finished, or there is none, the store follows a newer in-progress run, so a run started anywhere shows everywhere. The followed run's row is overlaid with its live state, so the list and the run panel cannot disagree.
- **Actions:** `startRun()`, `followRun()`, `clearRun()`, used by the Central Manager and Executor tabs. Their private pollers, discovery loops and snapshots are removed.
- **Executor step rows:** derived from the live progress text, remembering the furthest step reached. After a reload, a run that failed before execution marks the pre-flight row.
- **Run schema:** `runSchema` is derived from the single detected schema, which now carries `file_name` (used for the Executor's different-file warning). `last_csv_schema` is gone.
- **Migration:** on first load, `mgr_run_id` is adopted as `run_id` and the old snapshot keys are deleted.
- **Other pages on the shared list:** Live Monitor's "Active managed runs" uses it. Run Insights re-fetches analytics while any run is live and once when it finishes; the run detail re-fetches every 3 s while that run is live. The Overview refreshes its counts when a run starts or finishes.

**Frontend: data survives a reload**
- **Data file:** kept in the browser's IndexedDB (`src/fileStore.js`, up to 200 MB, local only). A reload restores it. Pages show "Restoring your data file…" meanwhile, and the warning banner appears only if restoring was impossible (file too large, private mode, storage cleared).
- **Planner self-check:** the result (`plan.verification`) is shown again after a reload. Editing the stage groups drops it, because it no longer describes the plan.
- **Loading state:** after a reload the Manager shows "Loading run …" instead of flashing its empty state.

### Verification

- **Backend:**
  - **Fake DB:** `_enter()` saved `executing`, then `assurance`, with no tasks left pending, and a call from outside the event loop is a no-op.
  - **Real DB (read-only):** `combined_analytics` and `combined_run_detail` work.
  - **Ruff:** no new findings; the 44 pre-existing ones are unchanged, checked by diffing against `HEAD`.
- **Frontend:** ESLint reports 0 problems, `vite build` passes, and the flow status tests pass (15/15).
- **Browser, file:** a stored file was restored after a reload; the Manager showed it as ready and the banner was gone.
- **Browser, simulated run** (`fetch` stubbed for the run list and status):
  - within one list poll, the Manager, the Executor ("Following run feedc0de") and Live Monitor all showed it as executing;
  - after flipping it to completed, the Manager panel, the Manager's recent-runs row and the Executor all showed completed, and Live Monitor dropped it.
- **Browser, real failed run `56fa5439` after a reload:** the Executor showed "Pipeline failed" with the failed row marked; the Manager showed the same run, "failed", with its error. No console errors.
- **Cleanup:** the test file stored in IndexedDB was deleted afterwards, and the test `run_id` was cleared.

**Needs a backend restart** (unless it runs with `--reload`) for the phase-change saves and the live-state reads in Run Insights.


## Stage 15 — Redesign, part 3: guided "New pipeline" flow

A new page, `/new` ("New pipeline", second item in the sidebar, plus a button on the
Overview), walks a first-time user from a file to a finished run in five steps:

| Step | What it does |
|---|---|
| **1. Data** | Drop or choose a CSV/JSON file. Shows the detected schema: row and column counts, format, size, every column with its type, and the first 5 rows. Choosing a file clears any old plan (a plan belongs to the data it was designed for). |
| **2. Describe** | Plain-English request, plus options: batch/streaming, single or multiple streaming stages, number of containers. While the planner works, it shows elapsed time and what is happening (designing, then self-checking). |
| **3. Review** | The designed pipeline as the pipeline flow diagram, the planner's reasoning, the self-check (verified, or the structural/intent problems it could not fix), and the starting resources, noting that the Resource and Cost agents refine them. Then Run, or "Run once (seed data)" for streaming plans. |
| **4. Run** | Current step, elapsed time and retries, plus the live agent flow and pipeline flow. Moves to Results by itself when the run ends. |
| **5. Results** | Success or failure banner with the output container and Download output, or the error. Stat cards for duration vs predicted, stages completed, estimated cost and output checks, then the final diagrams. Links to Run Insights, "Change the request" (after a failure) and "Start another pipeline". |

### Design decisions

- **No state of its own.** Every step reads and writes the shared `AppContext` (data file, schema, prompt, plan, run), so the Planner, Central Manager and Executor pages always show the same thing. A run started here has origin `wizard`.
- **The current step is derived from that state,** so a reload lands on the right step: a guided run in progress goes to Run, a finished one to Results, a plan to Review, a file to Describe. Clicking a completed step goes back. While a run is in progress the other steps are locked.
- **Prompt ideas use the dataset's own columns.** The sample rows decide each column's role: numeric 0/1 columns are flags (filters), other numbers are measures, and text columns with repeated values are group keys. *First version mistake:* on the zoo data it suggested "Total hair per animal_name", which sums a 0/1 flag per unique name. The rewrite suggests "Keep only rows where hair is 1 …" and "Average legs and count rows for each hair".
- **UI:** the new step indicator, drop zone, column chips and preview-table styles live in `ui/ui.css`; the page is built from the shared components; the options column stacks below 900 px.

### Verification

- **Lint and build:** ESLint reports 0 problems, and `vite build` passes (page chunk 7.3 KB gzipped).
- **Browser (dark theme), real backend and real planner**, with a 10-row zoo sample:
  - **Data:** 17 columns, types and preview shown.
  - **Describe:** the new column-aware ideas.
  - **Design:** the real planner produced ingest copy → notebook `avg(legs), count(*)` by `hair`, self-check "verified".
  - **Review:** it rendered as a graph.
- **Run and Results:** simulated by stubbing `POST /manager/run`, `/runs` and `/status`, so no Azure job was started. Pre-checks showed Resource, Performance and Cost running; executing showed the notebook stage running. On completion the page moved to Results with "Your pipeline ran successfully", duration 57 s vs predicted 2 m, 2/2 stages, $0.0318 and "Passed".
- **Test-harness mistake (mine), and how it was fixed.** I backed up the browser's saved state into the test tab's `sessionStorage`; that tab closed before the restore, so the backup was lost. Restored instead:
  - **Data file:** the stored file set back to the real `data/zv.csv` (101 rows) as "zv.csv";
  - **Schema:** re-detected from that file;
  - **Prompt:** the previous one, "filter animals that are predators";
  - **Plan:** redesigned from that prompt by the real planner. It has the same shape as before (ingest copy → notebook `predator = 1`), verified, but it is a new plan, not the byte-identical old one.
  - **Run pointer:** cleared.
- **Observed, not a bug:** with 2 containers the planner's notebook reads and writes the same container. Batch runs purge every container first (`executor.purge_container`), so old output cannot be re-read. The plan's reasoning text said "3 containers", which is cosmetic.


## Stage 16 — Redesign, part 4a: read-only pages on the shared components

Stage 16 was split in two so each part fits one commit:
- **16 (this part):** the pages that mostly display data;
- **17:** the Planner, Central Manager, Executor, Run Insights and the streaming console.

Data fetching and logic are unchanged unless listed below. Every page now uses
`PageHeader`, `Card`, `Stat`, `Badge`, `Alert`, `Empty`, `KV`, `Tabs`/`Segmented`,
and the `.table` / `.list` styles, so it follows both themes with no per-page
colours.

| Page | What changed |
|---|---|
| **Overview** (`HomePage.jsx`) | Page header with a live badge, Refresh and New pipeline. A banner for the managed run in progress (shared store) with a link to the Manager. Four stat cards. "Needs attention" (running now, anomalies, recent failures) and "Recent ADF runs" lists. "Current plan" shows the plan as a pipeline diagram, with Edit in Planner / Run in Manager. The no-data state offers "Sync last 48h" in place. |
| **Monitor** (`MonitorTab.jsx`) | Shared page header and underline tabs. |
| Live | Status line; cards for "Just finished" and "Managed runs in progress" (shared list); one card per ADF run. **Cancel now needs a second click** ("Click again to cancel"), because cancelling an ADF run cannot be undone. |
| Run logs | Filter form (Enter searches), expandable table rows with the AI analysis sections, and a "new runs finished" alert with Refresh. |
| Anomalies | Kind filter as a segmented control; events and AI verdicts as readable lists with severity badges and metrics. |
| Runtime predictions | Stat cards (predicted, confidence, history), and the range chart restyled with theme colours (predicted bar in the accent colour, tooltip on the surface colour); past-runs stats. |
| **Resource agent** | Stats (sizing engine, runs recorded, accuracy, correction factors); re-allocation results in a dismissable card; the current run's allocations as a table (compute, memory, node, time, ML-sized / right-sized / contention badges) plus the execution groups; per-type accuracy cards; subscription limits from the API. |
| **Performance agent** | Outcome banner, stat cards, stage forecast bars coloured by risk, prediction history as a table with ratio and check badges, and the method summary. |
| **Cost agent** | Stats (cost, cheaper options, engine), streaming advice, breakdown including the learned correction, recommendations (change, saving, risk, reason, trade-off, new total), node rates and backend assumptions. |

### Small fixes found while rebuilding

- **Resource:** "Check live re-allocation" showed nothing when no ADF run was live (an empty list rendered no panel). It now says "No ADF runs are live right now — nothing to re-allocate."
- **`KV` component:** it keyed rows by their label, and the Cost page's node-rate labels are elements, which gives duplicate keys. It now keys by index.

### Verification

- **Lint and build:** ESLint reports 0 problems, and `vite build` passes.
- **Browser, every page against the live backend:** Overview; Resource, Performance and Cost; Monitor → Live, Run logs (76 rows), Anomalies (7) and Runtime predictions. None crashed, and no `console.error` was reported (React key warnings included), in dark and light.
- **Real data shown:**
  - **Resource:** run `b88bd9a0`'s allocations (copy 2 DIU; notebook 4 vCPU, 8 shuffle).
  - **Performance:** the latest slowdown prediction.
  - **Cost:** $0.0364 with the learned ×0.8644 correction.
- **Capture artifact, not a bug:** some screenshots looked faded because the page's 0.18 s fade-in is paused while the tab is in the background; computed opacity was 1.


## Stage 17 — Redesign, part 4b: the working pages (redesign complete)

This stage rebuilds the last five pages on the shared components. **State and handler
code was copied unchanged**; only the rendering was rewritten. After this stage no
page keeps its own inline style object (`const S = {…}` / `const C = {…}`), and every
colour comes from the theme tokens.

| Page | Layout now |
|---|---|
| **Planner** | Page header with "Guided mode". A drop zone, or a file card with column chips and a collapsible preview. Side by side: the request (examples, Design) and Settings (processing, streaming stages, containers, names, collapsible compute overrides). The plan card holds the reasoning, pass-through warning, pipeline diagram, resource summary, collapsible execution-order editor, the self-check / re-check result with "Fix & design again", and Re-check / Discard / Send to Manager. |
| **Central Manager** | Page header with Run and Clear. Plan and data-file cards. Request & context, with schema and transformations collapsible. Streaming console for streaming plans. Run status banner with Download. Orchestration and pipeline diagrams, decision log, then result cards: resources & cost, parallelism, performance, plan checks, output checks. Plan warnings, and "How a run works" when idle. Recent runs as a table (click one to open it). |
| **Executor** | Page header with Run / Run again. Plan and data-file cards (with a compact drop zone and the different-file warning). An Execution card with the "Following run …" notice, live pipeline diagram, step list with status icons, and a result banner (error, ADF message, run details, Download). Monitor feed card. |
| **Run Insights** | Stats, agent-health cards and a runs table. The run detail has a back link, a title with a status badge, readable local times and duration, Download, the error and request notes, both diagrams, two columns of agent cards, and the decision log. |
| **Streaming console** | A card with a status badge, stream / trigger / sink line, drop zone, Refresh / Stop / New stream, last-trigger error, and an output table. |

### Small improvements

- **Run Insights times:** raw ISO timestamps (`2026-09-30T15:01:23.165Z`) are now local and readable, with the run's duration ("30 Sept 2026, 20:31:23 → 20:32:55 · 92s").
- **Less code:** the pages are about 3,700 lines, down from about 5,700 before the redesign.

### Verification

- **Lint, build and tests:** ESLint reports 0 problems (after removing one unused import), `vite build` passes, and the flow status tests pass (15/15).
- **Browser, against the live backend**, with the user's real finished run `ce7c2c2d`:
  - **Planner:** shows zv.csv (101 rows, 17 columns), the prompt and settings.
  - **Central Manager:** "Pipeline completed" with Download, two diagrams, 12 cards.
  - **Executor:** "Following run ce7c2c2d, started from the Central Manager", the pipeline diagram, all 8 step rows, "Pipeline completed".
  - **Run Insights:** 19 runs; the detail view has both diagrams and 13 cards.
  - **Errors:** no crashes and no `console.error`, in dark and light.
- **Browser state:**
  - **Backup:** the run pointer and theme were backed up first to a scratchpad file (`browser_backup_stage17.json`), not tab storage — see the stage 15 mistake.
  - **Untouched:** the checks only read; afterwards `run_id`, `run_origin` and `theme` were unchanged, and the plan, schema, prompt and stored file were never written.


---

## Stage 18 — Results catalogue for the paper (docs only)

**Commit message:** `docs: add paper results catalogue with measured, computed and planned results`

### Why

The user is writing a journal paper and asked for one document listing every result the project can support: metrics, with/without agents, agent combinations.

### Changes

| File | Change |
|---|---|
| `docs/PAPER_RESULTS.md` *(new)* | Every result tagged MEASURED (saved artifact), COMPUTED (recomputed from real logs on 2026-10-03) or TO RUN (protocol only). Covers each agent, real-run accuracy, a learning-agent with/without comparison, the ablation and agent-stack tables, figures, threats to validity and reproduction commands. |

No code, model or data file was changed.

### Verification

- **Re-run today:** dataset validator (5,000/5,000 valid), legacy v1 dataset through the same validator (5,000/5,000 rows violate at least one rule), assurance examples (6/6 as designed), resource examples, integration test (pass), cost safety tests (13/13).
- **Recomputed from real logs:** runtime MAPE by predictor (`manager_feedback.jsonl`), raw vs corrected stage error (`resource_feedback.jsonl`), cost MAPE with/without the learned factor, monitor history with the 15 demo rows excluded, Ollama call latency (`ollama.log`).

### Found while doing this (not fixed)

- `resource_agent/examples/run_examples.py` section F still expects the old "damped halfway" correction factor and fails since stage 3 changed it to the median of raw ratios. The test is stale, not the agent.
- The performance duration model was retrained at 14:35 on 2026-09-30 by the learning loop, so 3 of the 6 ML-path real runs may overlap its training data; the doc reports the 3 clean runs separately.

### Stage 18, continued — experiments for the paper (2026-10-03)

**Commit message (whole stage):** `docs: add paper results with with/without ablations, live Azure benchmark and eval scripts`

| File | Change |
|---|---|
| `scripts/paper_eval/` *(new)* | `real_run_metrics.py`, `ablation_offline.py`, `ablation_planner.py`, `live_benchmark.py`, `README.md` — reproduce every Part A/B number. They drive the real agents from outside; no agent code changed. |
| `docs/PAPER_RESULTS.md` | Part B: method, results and explanation for every experiment; headline rows H16–H24. |
| `performance_prediction_agent/models/metrics.json` | **Changed by the system itself**: the learning agent triggered an automatic retrain during the live runs (ML MAPE 27% > 20%); new model deployed, MAE 224.05 → 224.02. Commit it or `git checkout` it together with the `.pkl` files (snapshot `20261003_180820_perf_models` holds the previous model). |

**Experiments run:** planner ablation (24 prompts, 7 conditions, local Ollama), offline Resource / Assurance-gate / manual-effort ablations, 12 live batch runs + 8 parallel/sequential runs + 6 streaming drops on the user's Azure for Students subscription (≈ $0.30–0.40 by the cost formula). Data: `data/paper_eval/` (git-ignored), including a backup of the feedback logs, monitor DB and learning state from before the live runs.

**Found (not fixed — the user decides):**
1. Assurance column check treats `double`/`integer` in `cast(x as double)` as columns → 614/5,000 (12.3%) valid plans wrongly rejected. Fix: add SQL type names to `sql_keywords` in `assurance_agent/config/allowed_operations.json`.
2. Learning rollback review (`policy_engine._review_pending_changes`, duration branch) counts runs aborted before execution (actual ≈ 0.1 s) → post-change MAPE 68,094.7% → a correct duration correction was rolled back. Fix: exclude `success is False` / `executed is False` records from `post_change`.
3. Performance gate aborts on a coin-flip (P(failure) 0.49–0.52): 4/12 live runs aborted; the same stages grouped in parallel passed and completed. Suggest a confidence threshold.
4. Stale test: `resource_agent/examples/run_examples.py` section F (from the first part of this stage).

**Mistakes:** result files first written into `unified/` (moved; scripts fixed); a fault-injection tuple-assignment bug (found, fixed, re-run); planner experiment run alongside live planning overheated the laptop (stopped, finished later in low-heat mode); an opt-in perf-gate bypass switch was blocked by the environment's safety policy and reverted — `manager.py` unchanged (`git diff` empty).

### Stage 18, review pass — results checked against the code and data (2026-10-03)

**Commit message:** `docs: correct paper results after code review; add per-agent side-by-side tables`

**Why:** the user asked for side-by-side "with vs without" tables for every agent and an error-free document (Codex reviews it). Every claim in `PAPER_RESULTS.md` was re-checked against the code, the saved run states and the logs.

**Corrections (each was wrong before this pass):**

| Where | Was | Now | How found |
|---|---|---|---|
| B2 Resource with/without | 24.8% of plans over limits; "half the workers" | **18.6%** (all xlarge, via DIU); Resource Agent gives *more* workers (1.14 → 2.07) | Baseline used training targets; the planner's repair layer already caps workers by size (`RECOMMENDED_SETTINGS`, size cap). Re-run with the repaired plan as baseline. |
| B3 gate with/without | skipped Manager Phase-1 validation; assumed the executor rejects unknown stage types | real `validate_plan` included; unknown stage types are **silently skipped** and unsupported aggregations **silently dropped** → 400 of 600 faults give wrong output | Read `executor._execute_pipeline` (filters stages by type) and `notebook_builder._build_agg_expr` (returns "" for unknown ops). Re-run. |
| B5.5 learning | only the duration rollback was attributed to the bug | the **cost** rollback was also wrong: 195.6% reproduced exactly from 5 records incl. 1 aborted run; 18.5% without it | Reconstructed with `FeedbackCollector` + the review filter |
| B1 self-check | "fixed by the re-plan" without caveats; "checked by hand"; "≈ 1,000+ tokens" | final `s_agg` plan correct but still marked not verified; base-model verifier false accepts 5/12; unmeasured claims removed | Results file fields `*_full_attempts/verified` |
| A9.2 | 9 anomaly kinds | 8 | `anomaly_detector.py` `_add(...)` calls |
| A10.2 / B12.7 | retried failures 347–505 s | 48.3, 347.5, 485.5, 505.4 s | `manager_feedback.jsonl` |
| B4 | real plans n = 14, pre-checks 2–7 s | n = 24 (incl. Part B), pre-checks 0.56 s median | re-run; phase timestamps |
| B8 / B8.1 | total 2.5–4.5 min incl. a guessed 1 min of user time; "1.5–8 min per failed attempt"; "100% of faults caught by the gate" | 2–3.5 min + unmeasured user time; 48–505 s per failed run incl. retries; gate + earlier layers stop all, gate alone 3 classes | arithmetic re-checked |
| B6 / B7 | quoted a merged filter text not in the data; start-up cost "40–60 s" assumed | wording from the logs; 2-stage notebook phase median 43.8 s (measured) | backend log; run states |
| Part A | stale (repair layer, self-check, parallel groups marked TO RUN; 8.8% as headline; C1 claim of replacing a cloud LLM) | point to Part B results; 8.8% labelled in-distribution; C1 reworded (no cloud comparison run) | — |

**Added:** §B12 side-by-side tables for every agent (Planner, Assurance, Resource, Performance, Cost, Learning, Executor features, Monitor, design choices); B5.3 figures over all 14 completed live runs; Manager quick-estimate error on today's runs (84.3%); default-plan time (0.008 ms, measured); README rows for `live_benchmark.py` and the low-heat options.

**Data:** `data/paper_eval/offline_results.json` now holds the corrected B2/B3/B4 run (the first, wrong run was replaced).

**Verification:** all scripts compile; `ablation_offline.py` re-run end to end; every changed number traced to a file or a command in the table above.

## Stage 19 — Review of teammate commit `9aa21b6` and corrections to the results notes (2026-10-05)

**Commit message:** `docs: correct 2026-10-05 results — wrong filter outputs, in-sample rescore, gate sweep`

**What was reviewed:** every file in `9aa21b6` (Nithin078, 2026-10-05). Code changes are valid; all suites pass on this machine: `learning_policy_agent.test_rollback_filter` 8/8, `resource_agent.examples.run_examples` 0 FAIL, `scripts/integration_test.py`, `test_cost_model_safety` 13/13, assurance examples 6/6. Real data files unchanged by the tests (md5 before/after). No secrets in the commit. The offline numbers were reproduced here with `offline_remainder.py` (fuzz and anomaly check identical; Resource ML vs heuristic same direction on the original dataset).

**Corrections made in `PAPER_RESULTS.md` and `EXTRA_METRICS_SUGGESTIONS.md` (no code changed):**

| # | Problem | Fix in the notes | Evidence |
|---|---|---|---|
| 1 | §B13.2 "12/12 completed" read as correct, but filter runs kept 924 / 45,172 / 359,921 rows where the request keeps 172 / 7,623 / 59,817 on the seeded CSVs | warning in §B13.2; H30, A0, B9, top note flagged "completed ≠ correct" | recomputed from `live_benchmark.make_csv`; candidate filters checked |
| 2 | Suggestions doc said those counts "match" | §1 rewritten | same |
| 3 | §B14 8.3% / 17.8% / 14.3% scored on the runs the factors were learned from | in-sample warning in §B14; H31, B9, B11, B12 flagged; abstract sentence replaced | size-band rule (≥ 3 runs) + only that batch in the 5–50 MB band |
| 4 | B12 row implied "without" (8.3%) beats "with" (17.8%) | row states both are in-sample and cannot show that | — |
| 5 | Dataset difference attributed to the Windows checkout | cause stated: training file predates `226c0cc` (still has `processed_time` in 488 rows); regenerating on the Mac also differs | sha256 and `grep -c processed_time` |
| 6 | §5.5 / H28 / A9 used the regenerated dataset, §B2/§B3 the original | original-file numbers primary (43.3%, 47.9%, 186), regenerated shown beside | `offline_remainder.py` on this machine |
| 7 | §B10.1 said the threshold sweep cannot run | sweep measured here: current rule 53.9% precision / 68.3% recall; ≥ 0.7: 73.2% / 45.5%; table + H32 | classifier present on this machine; split matches training |
| 8 | Groq latency 22.4 s vs 22.5 s | 22.4 s everywhere | §B13.1 source line |
| 9 | Rollback test fixture labels the 2026-10-03 change 0.7902 (it was 0.8644 → 0.7856) | note in §B5.5 (test file not changed) | `learning_log.jsonl` |
| 10 | Container-creation change implied the old path was wrong | note: it worked on 2026-10-03; 403 is a later Azure change | `bench-*` containers created on 2026-10-03 |
| 11 | Imputation of pinned rows not disclosed | disclosed in §B14 and limitations | `_run_ratios` |
| 12 | Cast-fix side effect not stated | one line in limitations | `allowed_operations.json` |

**Not fixed (needs the teammate's data or a decision):** which filter the Groq plans actually ran (their saved states are on the second checkout); an out-of-sample batch for the duration fix; the test fixture label in `test_rollback_filter.py` (code, left as is).

## Stage 20 — Remaining results: offline group A, live Azure group D, Groq group C (2026-10-05)

**Commit message:** `docs: add offline, live and Groq results for 2026-10-05; new eval scripts`

**Run (user approved A, C, D; B deferred until asked):**
- A — `offline_more.py` (retry replay, fixed limit vs learned usual, re-sizing replay, learning simulation) and `perf_no_baseline.py` (temp retrain without `baseline_s`). → §B15, H33–H36.
- D — 24-run batch with downloaded-output checks, 1 re-check run, 12 pinned runs (Resource Agent settings vs planner's). → §B16, H37–H39.
- C — `groq_bare_eval.py`, 3 repeats, raw vs repaired paired. → §B16.6, H40.

**Safety:** feedback logs, monitor DB, learning state and Performance model files backed up to `data/paper_eval/state_backup_before_live_20261005/` before the live runs. Offline scripts only read history; md5 of the real logs/policies unchanged after them; production model files untouched by `perf_no_baseline.py`. Backend and Ollama stopped afterwards.

**Changed by the system itself during the live runs:** 6 learning cycles (cost factor 0.795 → 1.106, duration factor → 1.097, all 9 reviews confirmed) and an automatic Performance-model retrain at 16:01 UTC → `performance_prediction_agent/models/metrics.json` and the `.pkl` files changed (previous copies in the backup folder).

**Findings (not fixed — reported in the notes):**
1. Repair-layer bug: duplicated container names are de-duplicated by name, so a stage can read and write the same container; 4 runs completed with wrong output and passed every check (§B16.2). Reproduced offline.
2. Notebook builder does not compile `greater(toDouble(x), n)` — function style taught by the Groq prompt (§B16.6).
3. The intent regexes undercount Groq (function-style filters); fair scorer written, re-score blocked by Groq's 200,000 tokens/day limit.
4. Resource Agent's DIU choice at 25 MB slowed the copy (81 s at 2 DIU vs 60 s at 4) (§B16.7).
5. `dynamic_reallocate` is not called during runs (§B15.3).
6. Removing `baseline_s` makes live runtime error 36.5% → 142.1% (§B15.5) — keep or replace it.
7. Rollback review delays adaptation after a regime change (simulation, §B15.4).

**Mistakes during this stage:**
- Learning simulation first used record timestamps on a different timeline from the policy clock, so no review ever ran; found from the flat trace, fixed, re-run.
- First perf comparison passed the plan without schema/file size (the Manager passes both); found when recomputed verdicts did not match the logged ones; fixed (20/20 match).
- A queued follow-up job waited on `pgrep -f "live_benchmark.py batch"`, which matched its own command line, so it never started; killed and run directly.
- A hash snapshot was first written to `/tmp`; removed.
- Earlier summary said 19 completed / 4 gate aborts; correct figures are 20 completed, 3 gate aborts + 1 upload failure.
- The Groq fair re-run was started without checking the daily token budget; it hit the limit and was stopped (its log is labelled ABORTED).

### Stage 20, clean-up — stale "to run" markers (2026-10-06)

**Commit message:** `docs: mark completed results and remove stale to-run markers`

Notes only, no runs. Items already measured but still marked open were updated to point at their results: local vs cloud latency table (§3.8, from §B1 + §B16.6), cost results with an armed deadline (§7.4 → §B16.4), Groq bare call / single-LLM baseline (A4, §13 → §B16.6), live Resource with/without (§B2 caveat, §13 → §B16.7), retrain effect on new runs (§B5.3 → §B16.3), full-system row S8 of the agent-stack table (from the §B16 batch: 20/24 completed, 16/20 correct, Perf ML 18.4%, cost 24.5% / 17.9%, $0.051 per run, 567.7 s on wrong-output runs + 553.7 s on the failed upload), the Performance-gate summary row (adds the 2026-10-05 aborts and the false-abort evidence), §3.4 marked superseded by §B1, and the B10 list. Remaining 🧪/⏱ markers are all genuinely open.
