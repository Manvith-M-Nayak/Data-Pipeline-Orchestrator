# Results for the paper — complete catalogue

> Written 2026-10-03 from the code, models, logs and docs in `unified/`.
> Every number below was either **read from a saved artifact** or **recomputed today
> from the real run logs**. Each one says where it came from, so it can be checked and
> reproduced. Nothing here is an estimate unless it says so.

---

> **Part B (end of this file)** holds the experiments run on 2026-10-03: with vs without
> each agent, with vs without the whole system, live Azure runs, streaming, parallel runs,
> human-vs-system time, how each was tested, what else can be done, and a paper-writing kit.
> Its §B9 table is the one-page "with vs without" summary.

## 0. How to read this document

Every result has a status tag:

| Tag | Meaning | Can go in the paper as is? |
|---|---|---|
| ✅ **MEASURED** | Number exists in a saved artifact (metrics file, notebook output, test run, log). | Yes, with the caveat listed. |
| 🧮 **COMPUTED** | Recomputed on 2026-10-03 from real logs (`data/manager_feedback.jsonl`, `data/resource_feedback.jsonl`, `data/adf_monitor.db`, `learning_policy_agent/data/`). | Yes. Sample sizes are small — always report *n*. |
| 🧪 **TO RUN** | The experiment is designed and the code supports it, but the numbers don't exist yet. Protocol given. | No — run first. |

**Most important caveats (read before writing any claim):**

1. **Real-cloud sample size is small.** 34 managed runs (26 succeeded), 13 real Databricks
   job runs recorded in the monitor DB, 60 per-stage resource feedback rows. Report *n*
   next to every real-run number, and use medians alongside means.
2. **All ML agents except the planner were trained on synthetic data** (calibrated to real
   telemetry in some cases). Their test metrics measure agreement with a synthetic labeler,
   not real-world accuracy. Say so explicitly.
3. **"Actual cost" is not Azure billing.** It is the cost formula evaluated at the real
   measured duration. Call it "formula cost at observed runtime".
4. **15 of 78 rows in `pipeline_runs` and all 5 rows in `anomaly_log` are demo rows**
   (`run_id LIKE 'demo-%'`, written by `scripts/seed_anomalies.py`). They are excluded from
   every number below. Never report them as real anomalies.
5. **The planner's live out-of-distribution score (84%) was measured on an earlier model
   revision** (when the `processed_time` rule existed). Re-run `eval_live_planner.py` on the
   current model before publishing (see §3.4).

---

## 1. What the system is (one paragraph for the paper)

A multi-agent orchestrator that turns a CSV file plus a plain-English request into a
running Azure Data Factory (ingest) + Databricks (PySpark transform) pipeline. Nine
cooperating agents, each owning exactly one decision (`docs/RESPONSIBILITIES.md`):

| Agent | Decides | Technique |
|---|---|---|
| **Planner** | Pipeline design (containers, stages, transforms, filters, aggregations, execution groups) | QLoRA fine-tuned Qwen2.5-7B-Instruct served locally by Ollama + deterministic repair layer + self-verification loop (Groq LLM as alternate backend, deterministic default as last resort) |
| **Assurance** (library) | Is the plan structurally valid and does it match the intent | 4 deterministic rule checks + base Qwen2.5-7B intent check with 2 deterministic anti-hallucination guards |
| **Central Manager** | Run lifecycle, gates, retries, parallel groups, $ estimate | Phased state machine (validate → structural gate → pre-checks → execute → post-assurance → feedback) |
| **Resource** | Per-stage compute settings + feasibility under hard limits | HistGradientBoosting (500k rows, labels calibrated to 779k real Databricks job runs) + heuristic fallback + self-correcting duration factor |
| **Performance Prediction** | Runtime, bottleneck, outcome (success/slowdown/failure), "slower than usual" | GradientBoosting regressor + RandomForest classifier (synthetic + real-run blend) + formula fallback |
| **Cost Optimization** | Cheaper configurations that respect runtime | HistGradientBoosting trained on brute-force cost-optimal labels + fail-closed validator |
| **Executor** | Builds notebooks, deploys ADF, runs jobs | Code generation with an injection-safe expression compiler, resource locks, parallel groups |
| **Monitor** | Live status, history, anomalies, root-cause text | ADF polling + SQLite history + 9-kind rule detector + LLM (Groq) analysis |
| **Learning & Policy** | Correction factors, retrain triggers, flagged pipelines | Evidence-gated gradual updates with automatic rollback |

**Candidate contribution claims (each backed by a section below):**

- C1. A small (7B) locally-served fine-tuned model can replace a large cloud LLM for
  pipeline planning when paired with a deterministic repair layer (§3).
- C2. Validated, rule-checked synthetic training data (§3.5) — 0 violations vs 100% of
  rows violating at least one rule in the earlier templated dataset.
- C3. Strict single-owner decision boundaries remove a circular dependency between
  prediction agents (§6.4).
- C4. A closed feedback loop with evidence gating and automatic rollback measurably
  reduces estimate error on real runs (§8).
- C5. Fail-closed cost optimization: no recommendation is applied unless runtime is
  re-validated (§7).
- C6. Making an LLM verifier useful requires grounding + deterministic guards: 15/18 →
  27/27 correct (§4.2).

---

## 2. Headline results (ready to use)

| # | Result | Value | n | Tag | Section |
|---|---|---|---|---|---|
| H1 | Fine-tuned vs base Qwen2.5-7B, live free-form prompts, overall check pass rate | **84% vs 50% (+34 pts)** | 8 prompts × 8 checks | ✅ (older revision) | 3.4 |
| H2 | Fine-tuned vs base, in-distribution held-out | **100% vs 32%** | held-out synthetic rows | ✅ | 3.3 |
| H3 | Valid-JSON / contract rate, base → fine-tuned (Kaggle) | 50→100% / 0→100% | 2 prompts | ✅ | 3.3 |
| H4 | Trainable parameters | 40.37M of 7.66B (**0.53%**) | — | ✅ | 3.1 |
| H5 | Training dataset validity | **5,000/5,000 rows pass all 19 rules** (v1 dataset: 0/5,000) | 5,000 | ✅ re-run today | 3.5 |
| H6 | Runtime prediction error on real runs: Performance ML model vs formula vs Resource heuristic | **MAPE 8.8% vs 81.3% vs 72.3%** | 6 / 13 / 26 | 🧮 | 6.5 |
| H7 | Learning agent: cost-estimate MAPE without → with learned correction (same runs) | **50.3% → 29.9%** | 8 | 🧮 | 8.2 |
| H8 | Resource self-correction: per-stage duration MAPE raw → corrected | copy **151% → 60%**, notebook **224% → 62%** | 5 / 6 | 🧮 | 5.4 |
| H9 | Intent checker accuracy after grounding + guards | **27/27** (from 15/18) | 9 cases × 3 runs | ✅ | 4.2 |
| H10 | Cost model, fresh-sample worker R² before → after safety rebuild | **0.239 → 0.962** | 5,002 / 5,000 | ✅ | 7.2 |
| H11 | Cost safety regression tests | **13/13 pass** | 13 | ✅ re-run today | 7.3 |
| H12 | End-to-end managed runs | **26/34 succeeded (76.5%)**; 0 planner fallbacks | 34 | 🧮 | 11.1 |
| H13 | Local LLM call latency (Apple M5, 16 GB) | median **5.0 s**, p90 9.6 s | 280 calls | 🧮 | 3.8 |
| H14 | Resource model, workers / DIU exact-match accuracy | **96.4% / 97.9%** (within ±1: 99.98% / 99.65%) | 100k test rows | ✅ | 5.2 |
| H15 | Performance outcome classifier, balanced accuracy | **0.713** (5-fold CV 0.719 ± 0.004) | 21,006 test | ✅ | 6.2 |
| H16 | Planner correct (executable + intent): no AI / base / fine-tuned raw / fine-tuned + repair / full | **0% / 0% / 33% / 96% / 100%** | 24 (full: 12) | ✅ new | B1 |
| H17 | Faulty plans reaching the cloud without vs with the assurance gate | **43% → 0%** | 1,400 injected | ✅ new | B3 |
| H18 | Plans exceeding subscription limits without vs with Resource Agent | **24.8% → 0%** | 1,000 | ✅ new | B2 |
| H19 | Live Azure runs: executed runs that completed with correct output | **8/8** (+4 aborted by perf gate) | 12 | ✅ new | B5 |
| H20 | Cost-estimate MAPE without vs with learning, new out-of-sample live runs | **51.9% → 27.4%** | 8 | ✅ new | B5.3 |
| H21 | Parallel vs sequential execution groups (fan-out, live) | **143.7 s → 92.4 s (−36%)** | 2+2 | ✅ new | B7 |
| H22 | Streaming tick, multi vs single stage (same output, exactly-once) | **78.8 s → 46.8 s (−41%)** | 3+3 drops | ✅ new | B6 |
| H23 | System decision time per run (gates + sizing + prediction + codegen) vs total | **< 1 s of 116 s** median | 14 | ✅ new | B8 |
| H24 | Runtime prediction on new live shapes (out-of-sample) | MAPE **51.1%** (ML) vs 53.6% (heuristic) | 8 | ✅ new | B5.3 |

---

## 3. Planner Agent

### 3.1 Fine-tuning setup ✅

Source: `planner_agent/model/planner_finetune.ipynb` (cell outputs).

| Item | Value |
|---|---|
| Base model | `unsloth/Qwen2.5-7B-Instruct-bnb-4bit` (4-bit NF4) |
| Method | QLoRA via Unsloth, `train_on_responses_only` (loss only on the config JSON) |
| LoRA | r = 16, α = 32, dropout 0, targets q/k/v/o/gate/up/down (28 layers) |
| Trainable params | 40,370,176 of 7,655,986,688 (**0.53%**) |
| Data | 5,000 rows (`planner_config_dataset.jsonl`, the validated v2 dataset) |
| Batch | 2 × grad-accum 4 = 8 effective |
| Steps / epochs | 625 / 1 |
| LR / schedule / optimizer | 2e-4, linear, 10 warmup steps, `adamw_8bit`, weight decay 0.01 |
| Max sequence length | 2,048 (inference uses `num_ctx` 4,096) |
| Seed | 3407 |
| Hardware | Kaggle **Tesla T4**, 14.6 GB VRAM, fp16 |
| Training time | **16,097 s (4 h 28 min)** |
| Peak VRAM | 14.37 GB reserved |
| Output | LoRA adapter 161.5 MB (safetensors) → **80.8 MB GGUF** for Ollama |
| Serving | Ollama, `FROM qwen2.5:7b-instruct` + `ADAPTER planner-agent-lora.gguf`, temperature 0.2, top-p 0.8 |

**Why it matters:** shows the planner is trainable for free on a single consumer-class
GPU and served with an 81 MB file on top of a public base model — no cloud LLM cost,
data stays local.

### 3.2 Training loss curve ✅ (figure-ready)

The step-by-step loss for all 625 steps is stored in the notebook output (cell 24, HTML table).

| Steps | Mean loss | Min loss |
|---|---|---|
| 1 | 1.020 (first step) | — |
| 1–50 | 0.1655 | 0.0017 |
| 51–100 | 0.0020 | 0.0007 |
| 101–150 | 0.0011 | 0.0002 |
| 151–200 | 0.0010 | 0.0001 |
| 201–625 | ≤ 0.0001 | ~0 |
| 625 (last) | 0.000012 | — |

**How to explain it:** the loss collapses within ~100 steps. That is expected: the
assistant output is a deterministic function of the input (the dataset is generated
from rules), so the model learns the grammar fast. It also tells the reader the in-distribution
score (§3.3) will be near-perfect, which is why the out-of-distribution test (§3.4) is
the one that matters. **No validation loss was logged** — 🧪 add a 5% validation split
and log eval loss every 25 steps if you want a train/val curve.

Figure: *Fig. P1 — training loss (log scale) vs step*. Extract with:
`re.findall(r'<td>(\d+)</td>\s*<td>([\d.e-]+)</td>', html)` on cell 24's `text/html` output.

### 3.3 Exam A — in-distribution (held-out synthetic rows) ✅

Source: `docs/PLANNER_MODEL_BEGINNER_NOTES.md` §7.1 (Kaggle eval).

| Check | Base Qwen2.5-7B | Fine-tuned |
|---|---|---|
| valid JSON | 96% | 100% |
| num_containers correct | 50% | 100% |
| containers set consistent | 0% | 100% |
| stage-type sequence (copy then notebooks) | 72% | 100% |
| stage count | 80% | 100% |
| filter conditions | 26% | 100% |
| transformations | 13% | 100% |
| routing (stage chaining) | 0% | 100% |
| **Overall** | **32%** | **100%** |

Also in the training notebook (cell 26, 2 hand-written prompts): valid-JSON 50% → 100%,
full contract 0% → 100%.

**Caveat:** the number of held-out rows was not recorded in the doc. 🧪 Re-run on a
fixed 250-row held-out split and report *n*.

### 3.4 Exam B — out-of-distribution live test ✅ (older revision) / 🧪 re-run

Source: same doc §7.1; script `planner_agent/training/eval_live_planner.py` (8 free-form
prompts: canonical filter, "drop the cheap stuff", "only EU region rows please", derived
column, uppercase, aggregation, numbered stages, typo "filtr rows whre quantiy > 10").

| Check | Base | Fine-tuned | Δ |
|---|---|---|---|
| Valid JSON | 100% | 100% | — |
| 9-key contract | 100% | 100% | — |
| Stage type sequence | 100% | 100% | — |
| Container count | 12% | 88% | +76 |
| Container set consistency | 0% | 100% | +100 |
| Intent (filters/transforms) | 38% | 88% | +50 |
| Routing | 0% | 12% | +12 |
| **Overall** | **50%** | **84%** | **+34** |

**Key qualitative findings to write up:**
- The base model *understood* the request but put the filter in the stage's display name and
  left the machine-readable `filter_condition` null — useless for execution.
- Fine-tuned model mapped "drop the cheap stuff" → `unit_price >= 100` and the typo
  `quantiy` → real column `quantity`.
- Weaknesses: routing (inventing container names and keeping them consistent across
  the JSON) 12%; free-form aggregation phrasing was dropped ("silent intent loss").
- The deterministic repair layer (§3.6) fixes routing completely; it cannot recover a
  dropped aggregation.

🧪 **Re-run for the paper** (current model, more prompts):
```bash
ollama serve &      # planner-agent model must be built
python planner_agent/training/eval_live_planner.py planner-agent
python planner_agent/training/eval_live_planner.py qwen2.5:7b-instruct   # base
```
Recommended: extend `CASES` to ~40 prompts (5 per category: filter, derived, normalize,
aggregation, numbered stages, typos, multi-op, ambiguous), 3 seeds each, and report
mean ± std per check. Also add a **domain-held-out** set (schemas from domains not in
training).

### 3.5 Training-data quality ✅ (re-run today)

`python planner_agent/training/validate_dataset.py` → **PASS: all 5,000 rows valid (0 violations)**.

| Rule group | Rules | Result |
|---|---|---|
| Structural | S1–S8 (counts, ordering, routing, lineage, sample types) | 0 violations |
| Quality | F1 deterministic settings, F2 one filter grammar, F3 prompt = render(config), F4 no float equality, F5 realistic values, F6 pass-through ≤ 25% | 0 violations; F6 ratio 9.6% |
| Semantic | FA contradictory filter chains, FB dominated/duplicate ops, FC identity renames, FD domain ranges, FE no all-pass-through | all 0 |

Diversity (5,000 rows):

| Dimension | Distribution |
|---|---|
| Domains | 18 (sales, IoT, finance, web logs, HR, healthcare, gaming, flights, …); max single-domain share **6.4%** |
| Stage count | 2: 34.0%, 3: 31.1%, 4: 19.7%, 5: 15.2% |
| Container scheme | medallion 36.8%, generic 34.0%, ELT 14.9%, lakehouse 14.3% |
| Size bucket | small 29.1%, medium 30.3%, large 20.5%, xlarge 20.0% |
| Columns per schema | 4–10 (7 is 50.8%) |
| Operations (per stage occurrences) | filter 5,298; aggregation 1,406; derived arithmetic 1,210; pass-through 1,037; normalize 887; cast 633; sort 610; concat 607; dedup 520; rename 488; round 465 |

**Dataset-quality comparison 🧮 (computed today):** the earlier templated dataset
`synthetic_planner_dataset.jsonl` (5,000 rows) run through the same validator:

| Rule | Rows violating (v1) | v2 |
|---|---|---|
| F5 unrealistic values (e.g. humidity 9,982%, salary 2,884) | 2,550 (51.0%) | 0 |
| F1 inconsistent resource settings for same size | 2,450 (49.0%) | 0 |
| F3 prompt does not match config | 2,450 (49.0%) | 0 |
| FB duplicate derived column | 1,389 (27.8%) | 0 |
| F2 function-style filters (`equals(...)`) | 549 (11.0%) | 0 |
| **Rows with ≥ 1 violation** | **5,000 (100%)** | **0 (0%)** |

Caveat: some F3/FB hits in v1 involve the automatic `processed_time` column that the spec
later removed; even excluding those, every v1 row still fails F1 or F5.
🧪 **Ablation worth running:** fine-tune the same recipe on v1 vs v2 and compare §3.4
scores — directly measures the value of validated data.

### 3.6 Deterministic repair layer (guardrails) — 🧪 TO RUN, high value

The planner never returns raw model output. `planner_common.py` applies, in order:
`enforce_container_count → redistribute_operations → reconcile_prompt_filters →
apply_custom_settings → apply_prompt_stage_names → _structural_validate` (Azure-safe
names, copy/notebook typing, size-aware resource clamps, aggregation validation,
execution-group repair).

**Experiment:** for each prompt in the extended live set, score
(a) raw model output, (b) after the repair layer, (c) after repair + self-check.
Report the same 8 checks plus "executable" (passes `plan_safety_issues` + notebook
compiles). Also count, per repair function, how often it changed the plan — a
"repair-hit rate" table is a strong figure.

Expected shape (from §3.4): routing 12% raw → ~100% repaired; aggregation drop not
recoverable.

Implementation hint: `ollama_planner.decide_pipeline_config` already builds raw JSON
before the chain; log `raw` and each intermediate in an eval script.

### 3.7 Self-verification loop ✅ (functional) / 🧪 (rates)

`planner_agent/self_check.py`: plan → assure (structure + intent) → re-plan once with the
problems as feedback → keep the best attempt.

Verified (`docs/LOGIC_FIXES_LOG.md` stage 10), mocked: passes first time (1 attempt);
unknown column then fixed (2 attempts, fixed plan returned); fallback not retried; fails
twice → `verified false`. Live: zoo dataset "keep only predators" verified on first attempt
in 33 s (batch), 34 s (streaming); multi-stage streaming 29 s; single-stage merged
streaming 53 s.

Also measured: before this change, the separate assurance step flagged **10 of 11 real
runs, all false alarms** — the motivation for moving verification into planning.

🧪 Report on ≥ 40 prompts: % verified on attempt 1, % fixed by the re-plan, % still
unverified, added latency per re-plan (~20–30 s).

### 3.8 Planner latency & cost 🧮

From `data/ollama.log` (all 280 local `/api/chat` calls; mixes planner and intent-check
calls, both 7B models on the same machine):

| Statistic | Seconds |
|---|---|
| median | 5.0 |
| mean | 5.9 |
| p90 | 9.6 |
| min / max | 1.0 / 44.4 (max includes model load) |

Hardware: Apple M5, 16 GB unified memory, Ollama, Q4 base + LoRA.

End-to-end `/api/planner/plan` with self-check: **29–53 s** in the live tests above.
$ cost per plan: **0** (local). 🧪 For the paper: time 30 planner calls separately from
intent calls and compare with the Groq backend (`PLANNER_BACKEND=groq`) on the same
prompts — gives a latency/cost/quality table: *local 7B fine-tuned vs cloud large LLM*.

### 3.9 Fallback rate in production 🧮

`used_fallback` = **0 of 34** managed runs (the deterministic default plan was never needed).

---

## 4. Assurance Agent

### 4.1 Structural checks ✅ (re-run today)

`python -m assurance_agent.examples.run_examples` — 6 shipped cases, all behave as designed:

| Case | Expected | Result |
|---|---|---|
| valid_plan | pass | pass |
| bad_json_plan | fail `json_schema` | fail |
| bad_column_plan | fail `column_references` (`discount`, `shipping_status`) | fail |
| bad_operation_plan | fail `allowed_operations` (`median`) | fail |
| bad_ordering_plan | fail `stage_ordering` | fail |
| intent_mismatch_plan | structural pass, semantic flag | pass (structural) |

Deterministic, no model, 100% repeatable.

### 4.2 Semantic intent check — grounding + guards ✅

Source: `docs/LOGIC_FIXES_LOG.md` stage 8 and 11. Live `qwen2.5:7b-instruct`, 3 runs per case.

Progression while fixing: **15/18 → 12/21 (prompt rule, reverted) → 15/21 → 18/21 →
21/21 → 27/27.**

What changed (each is an ablation-able component):
1. Send real column names + types (`DATASET_COLUMNS`) — fixes hallucinated "corrections"
   (`predator` → `is_predator`).
2. Replace raw plan JSON with plain-English `PLAN_OPERATIONS` lines.
3. Mark ingest / empty stages `[infrastructure]`.
4. **Guard 1** — drop issues that reference columns in neither data nor plan.
5. **Guard 2** — drop no-op suggestions (fix equals what the plan already does).

Final test set (9 cases × 3 runs = 27): 5 should-pass, 4 should-flag; all correct.
Real failure modes of a 7B verifier worth listing in the paper: invented column names,
reported a present filter as missing, flagged the mandatory ingest as "unnecessary",
suggested changes already in the plan. Also: an extra prompt rule *lowered* accuracy
(12/21) — evidence that small verifiers need deterministic guards rather than more
instructions.

🧪 **Ablation table to produce:** run the 9-case suite with each component removed
(1–5 above) and report accuracy + false-flag rate. Also a 50-case set for tighter CIs.

### 4.3 Gates in real runs 🧮

From 34 managed runs: pre-run plan assurance passed **28**, failed **1**, not recorded 5
(older log format). Post-run assurance passed on all **26** successful runs.

---

## 5. Resource Agent

### 5.1 Grounding in real telemetry ✅

Labels come from `resource_agent/ml/calibration.py`, anchored to the cleaned real datasets:

| Real dataset | Size | Constant extracted |
|---|---|---|
| `job_runs_cleaned.csv` | 779k Databricks runs | p50 80 s, p95 534 s; CONTINUOUS p50 76 s vs CRON p50 489 s (6.4×) → **1,600 rows/core/s** |
| `pipeline_runs_cleaned.csv` | ADF copy runs | p25/p50/p75 = 35/50/65 s → target copy 55 s |
| `queries_cleaned.csv` | 60k SQL runs | cost ordering: group-by 22.8× > join 12.6× > aggregation 9.9× of base |
| `dbquery_statistics_cleaned.csv` | statement stats | CPU / I/O magnitudes → memory and shuffle blow-up |

Hard limits (student tier): 4 workers, 8 DIU, 3 concurrent stages, 64 GB memory.

### 5.2 Model accuracy (synthetic test set) ✅

Source: `resource_agent/models/metrics.json` — 500,000 rows, 80/20 split, HistGradientBoosting.

| Target | MAE | R² | Exact | Within ±1 |
|---|---|---|---|---|
| workers | 0.067 | 0.9896 | 96.44% | 99.98% |
| DIU | 0.030 | 0.9922 | 97.94% | 99.65% |
| memory (GB) | 0.452 | 0.939 | — | — |
| shuffle partitions | 3.069 | 0.9363 | — | — |
| node type (classifier) | — | — | **balanced acc 0.457** | — |

**Report honestly:** node-type is the weak spot (0.46 balanced accuracy, 6 classes;
chance ≈ 0.17). Workers on serverless are advisory anyway (§10.3).

### 5.3 Invariant tests ✅ (re-run today)

`python -m resource_agent.examples.run_examples`: ML sizes both stage types; heavy
aggregation gets ≥ workers of a small stage; an oversized 5-stage parallel group is split
into 5 sub-groups that each respect limits; `dynamic_reallocate` recommends scale-up at
3.5× predicted elapsed. **One stale test fails:** section F expects the old "damped
halfway" correction, which was intentionally replaced by the median-of-raw-ratios method
(stage 3). Update the test before citing a pass count.

### 5.4 Self-correcting duration factor on real runs 🧮 (strong result)

From `data/resource_feedback.jsonl`, rows that log both the raw and corrected prediction:

| Stage type | n | MAPE raw heuristic | MAPE after correction | mean actual / predicted |
|---|---|---|---|---|
| copy (ADF) | 5 | **151.2%** | **60.3%** | 0.40 → 0.63 |
| notebook (Databricks) | 6 | **224.3%** | **61.9%** | 0.31 → 0.62 |

Correction factors in use: copy 0.62–0.66, notebook 0.42–0.53 (current live values from the
integration test: copy 0.530 over 23 records, notebook 0.339 over 37).

Also measured (stage 3, simulation with the real agent): the old method converged to
0.809 / 1.281 for true ratios 0.5 / 2.0; the fixed method converges to **0.5 / 2.0**.
Failed-run rows (ratio 0.005) are now excluded.

**How to write it:** "The heuristic over-estimates real serverless runtime by 2.5–3.2×;
the feedback loop cuts error by ~60–70% relative after a handful of runs. Residual
over-estimation remains because the median of the last 10 ratios lags the latest
improvements."

### 5.5 🧪 To run

- Resource ML vs heuristic on the same plans: settings agreement, feasibility verdicts.
- Real-run validation: run the same CSV at 3 sizes × 2 complexities with the recommended
  settings vs a fixed default; compare runtime and formula cost.

---

## 6. Performance Prediction Agent

### 6.1 Training data ✅

`run_training.py` (v4): 100,000 general + 5,000 tiny-file synthetic rows; real runs blended
with a leakage-safe split (real rows split 80/20 **before** oversampling). From
`models/metrics.json`: 93,340 train / 21,006 test; real rows available 26 → 20 blended into
train (oversample weight 467), 6 held out; failure rate 9.31%.

Logical-correctness checks on the generator (correlation with risk):
stage_count +0.270, file_size +0.189, parallel_ratio −0.179, correction_deviation +0.337,
network_quality −0.266, n_execution_groups +0.385; row_count vs file_size 0.988.

### 6.2 Model metrics (synthetic test set) ✅

| Model | Metric | Value |
|---|---|---|
| Duration regressor (GBR, 500 trees, log target) | MAE | 224.05 s |
| | R² | 0.871 |
| Outcome classifier (RF, 1000 trees, balanced) | accuracy | 0.734 |
| | balanced accuracy | **0.713** |
| | macro F1 | 0.696 |
| | 5-fold CV balanced acc | 0.719 ± 0.004 |

Per class:

| Class | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| failure | 0.555 | 0.664 | 0.605 | 1,473 |
| slowdown | 0.662 | 0.728 | 0.694 | 8,675 |
| success | 0.836 | 0.748 | 0.789 | 10,858 |

Training time: 129 s total.

### 6.3 Version history (good for an "iterations" table) ✅

| Version | Data | Duration R² | Outcome balanced acc | Failure F1 |
|---|---|---|---|---|
| v1 (notebook) | 20k synthetic, 1.4% failures | 0.867 | 0.636 | 0.43 |
| v2 (README) | 22k (+2k tiny files) | 0.868 | 0.656 | ~0.44 |
| v4 (current) | 105k + real blend | 0.871 | **0.713** | **0.605** |

Tiny-file spot check (0.0011 MB, real runs 126–144 s): first ML run predicted 425 s
(2.95×) → after tiny-file tier 199 s (1.58×) → current 169 s.

### 6.4 Finding: circular dependency between agents ✅ (design result)

In the v1 model, the single strongest feature was `baseline_s` — the Resource Agent's own
duration formula — with importance **0.908**. The "ML" runtime was mostly a rescaled
heuristic. This motivated the single-owner rule (`docs/RESPONSIBILITIES.md`): Resource owns
*settings*, Performance owns *runtime*. Good paper material for C3; the clean fix (drop
`baseline_s`/`resource_estimate_s` from features) is still 🧪 open — run it and compare.

### 6.5 Runtime prediction on real runs 🧮 (key real-world result)

All successful managed runs, predicted runtime vs measured execution time:

| Predictor | n | MAPE | median APE | mean bias | within ±20% |
|---|---|---|---|---|---|
| Resource heuristic (baseline duration) | 26 | 72.3% | 64.2% | +71.0% (over) | 8% |
| Performance agent, **formula** path | 13 | 81.3% | 80.5% | +81.3% (over) | 0% |
| Performance agent, **ML** path | 6 | **8.8%** | 8.8% | −4.3% | **100%** |
| Resource heuristic on the same 6 ML-era runs | 6 | 61.9% | 63.6% | +61.9% | 0% |

**Caveat that must be stated:** the duration model file was retrained at 14:35 on
2026-09-30 by the learning loop, which blends exported real runs. The 3 runs *after* that
time are clean out-of-sample: predicted 89/88/81 s vs actual 93.6/93.7/91.6 s → **MAPE 7.5%
(n = 3)**. The 3 earlier ones may overlap training data. All 6 come from 1–2 stage, small-file
pipelines. 🧪 Collect ≥ 20 new runs over varied sizes/stage counts before claiming
generalization.

Real durations by stage count (successful runs): 1 stage 75.2 ± 7.0 s (n=7);
2 stages 114.9 ± 22.4 s (n=17); 5 stages 317.3 ± 19.1 s (n=2).

### 6.6 Learned "usual duration" (replaces a fixed SLA) ✅

Per pipeline, p95 of the last 20 comparable completed runs (input 0.5–2× size, no
cost-accepted slowdown; ≥ 3 runs needed). Verified cases: history 100/120/110 s +
prediction 150 s → expected 120 s, "slower"; 10× bigger input → not comparable;
cost-slowed runs excluded (ratchet guard). Cost ceiling = min(1.2 × prediction, usual).

---

## 7. Cost Optimization Agent

### 7.1 Model ✅

`cost_optimization_agent/models/cost_metrics.json` — 30,000 feasible synthetic rows, 17
features incl. `deadline_s`; labels = brute-force cheapest feasible config.

| Target | Test MAE | Test R² | Exact | Train–test R² gap |
|---|---|---|---|---|
| workers | 0.027 | 0.967 | 99.27% | 0.010 |
| DIU | 0.008 | 0.992 | 99.63% | 0.002 |
| memory (GB) | 1.061 | 0.974 | — | 0.008 |
| shuffle partitions | 0.282 | 0.981 | — | 0.006 |
| node type | — | — | balanced 98.86% | 0.011 |

No overfitting flags.

### 7.2 Audit: before vs after the safety rebuild ✅ (strong "rigor" table)

Source: `COST_MODEL_AUDIT.md`, `cost_model_audit_results.json`. Fresh independent synthetic
samples (not the training set).

| Target | Before (fresh, n=5,002) R² | After (fresh, n=5,000) R² |
|---|---|---|
| workers | **0.239** | **0.962** |
| DIU | 0.341 | 0.992 |
| memory | 0.749 | 0.971 |
| shuffle | 0.982 | 0.978 |
| node balanced acc | 98.96% | 99.39% |

Seven confirmed defects in the original (deadline bypass, rejected suggestions restored,
deadline missing from features, savings not recalculated, empty-plan crash, copy DIU labels
wrong, misleading metrics). Example: with a 151 s deadline the old agent applied a change
that raised the labeler's runtime 141 s → 204 s while claiming 75% savings. After the
fix this is rejected; empty plans return no recommendations; node price changes now move
cost (0.2308 → 0.1608).

### 7.3 Safety tests ✅ (re-run today)

`python -m unittest test_cost_model_safety` (repo root) → **13/13 OK** in 1.3 s
(deadlines, memory, missing workload data, unchanged input, empty plans, pricing, copy DIU,
model loading, safe fallback, valid saving candidate).

### 7.4 Behaviour in real runs 🧮

Auto-apply changed nothing in the logged real runs (`cost_slowdown_applied` false in all 6
runs that log it) — by design it is **fail-closed**: no runtime trade without a learned
deadline (needs 3 comparable runs). Integration test: 0 recommendations, source
"heuristic" (no safe candidate).

🧪 **To make cost results publishable:** run each test pipeline ≥ 3 times (to unlock the
learned deadline), then measure % of runs with an accepted recommendation, formula-cost
saving, and runtime change vs prediction. Ideally reconcile against Azure Cost Management
for a few days.

---

## 8. Learning & Policy Agent

### 8.1 Mechanism ✅

Every 5 runs: evidence gate (≥ 10 relevant runs) → gradual move (30% of the gap,
bounded [0.3, 2.0]) → pending → review after ≥ 5 new runs → confirm, or roll back if
error worsened by > 5 pts. Every change snapshotted (`versions/`).

### 8.2 With vs without learned cost correction on real runs 🧮 (key ablation, real data)

The manager logs both the raw and corrected estimate, so this is a clean paired comparison:

| Estimate | n | MAPE | median APE | within ±50% |
|---|---|---|---|---|
| Uncorrected (no learning agent) | 8 | 50.3% | 59.4% | 38% |
| Corrected (factor 0.8644) | 8 | **29.9%** | 37.8% | **100%** |

The agent's own review log agrees: pre-change cost MAPE **91.6%** (its window) → post-change
**29.9%** over 8 runs → change **confirmed**. Second update 0.8644 → 0.7856 (pre-change MAPE
65.6%) is pending review.

Policy history (`learning_policy_agent/data/learning_log.jsonl`): 1 signature flagged
(`2stages_low`, 33% failure rate over 12–15 runs → human review); 2 cost-factor updates;
1 review confirmed. A documented earlier event (README): duration factor learned, then
**rolled back automatically** (post-change MAPE 95.6% vs 56.2% before) — shows the safety
net firing in the other direction. (That log was later reset; cite the README.)

### 8.3 Convergence correctness ✅

Stage 3 fix: factors originally targeted actual ÷ *corrected* prediction and converged to
√(true ratio) (0.707 for a true 0.5, 1.414 for 2.0). After targeting raw predictions they
converge to exactly 0.5 / 2.0 (simulated through the real normalize → analyze → update path).

### 8.4 Silent bugs found (good for "lessons" section) ✅

Four silent failures, each invalidated a factor without any error: field-name mismatch
(`estimated_cost_usd` never logged), `or`-fallback picking the wrong field (ratios 11–14
instead of 0.5–1.15, factor driven to the 2.0 ceiling), filtering on a field removed by
normalization, and a cost stub that never moved.

### 8.5 🧪 To run

- Same paired analysis for duration once ≥ 10 ML-path runs exist.
- Simulation: replay a synthetic stream with a known bias (e.g. true ratio 0.6) and plot
  factor vs run count for learning rates 0.1 / 0.3 / 0.5 — convergence speed and stability figure.
- Inject a bad update and show the rollback firing (figure: MAPE before/after/rollback).

---

## 9. Monitor Agent & anomaly detection

### 9.1 Real run history 🧮 (`data/adf_monitor.db`, demo rows excluded)

| Pipeline | Real runs | Status | Duration (s) |
|---|---|---|---|
| Orchestrator_Copy_Pipeline (ADF) | 45 | all Succeeded | not recorded by ADF API for these rows |
| Databricks_Notebook_Pipeline | 6 | all Succeeded | mean 90.7 (70.2–103.5) |
| Databricks_Streaming_Pipeline | 7 | all Succeeded | mean 71.9 (47.2–90.5) |
| Unified_Orchestrator_Pipeline (early ADF→Databricks design) | 4 | all Failed | — |

Date range: 2026-06-25 → 2026-09-30.

**Design finding:** the 4 early failures were "Only serverless compute is supported in the
workspace" / linked-service errors — ADF's Databricks activity could not use serverless.
This forced the final architecture: ADF for copy, Databricks Jobs API directly for compute.
Worth one paragraph in the paper.

**Streaming incremental effect:** same pipeline, first tick 70.5 s, next tick 47.2 s (−33%)
because the checkpoint skips already-processed files. n = 1 pair — 🧪 repeat 10 ticks.

### 9.2 Anomaly detector ✅ (design) / 🧪 (accuracy)

9 detected kinds: failure, timeout, retry_storm, slow_runtime (> 1.2× own p95, ≥ 3 runs),
cold_start (slow + idle > 6 h), zero_rows, cost_spike (> 2× trailing avg), schema_drift,
(data_skew documented as not detectable on serverless). Real events: 2 (one `failure`, one
`retry_storm`, same run: filter `predator IS TRUE` could not compile → 2 retries).

🧪 **Detector evaluation:** use `scripts/seed_anomalies.py`-style injection to create
labelled runs for each kind (e.g. 20 normal + 5 per anomaly kind), report precision/recall
per kind and false-positive rate on normal runs.

### 9.3 LLM root-cause analysis 🧮

62 real runs analysed by the Groq LLM: severity low 52, medium 5, high 5. All 4 real
serverless failures were correctly attributed to the compute-type restriction. 🧪 For a
quantitative result: have 2 people label root causes for ~30 runs and report agreement
with the LLM (accuracy / Cohen's κ).

---

## 10. Executor Agent

### 10.1 Isolation and locking ✅

Mocked 0.4 s runs (stage 4): two runs on the same containers 0.8 s (serialized); two
streams on disjoint containers 0.4 s (parallel); two copy runs 0.8 s (serialized on the
shared ADF pipeline). Unique run tags; per-run notebook folders deleted even on failure.

### 10.2 Retry efficiency ✅

Deterministic failures (unsafe/uncompilable plan, missing references) now return
`retryable: false` → **1 attempt instead of 3**, saving 40 s of backoff (10 s + 30 s) plus
two wasted executions. Real logs show 4 failed runs that each used 2 retries (347–505 s),
e.g. the uncompilable `predator IS TRUE` filter — exactly the case this fix removes.

### 10.3 Safety of generated code ✅

The scan found code injection through filter strings into generated notebooks (critical #1).
The expression compiler now escapes literals and validates identifiers; AND/OR with
correct precedence, BETWEEN, and `and` inside quoted values handled; the injection string
compiles to an inert literal. 🧪 Fuzz: generate 1,000 adversarial filter strings and report
0 executable injections + % of benign filters compiled.

### 10.4 Parallel execution groups 🧪

`execution_groups` run concurrently (ThreadPoolExecutor, max 3). No real fan-out run is
logged (all real plans were linear chains). 🧪 Build a fan-out plan (copy → 3 independent
notebook branches) and compare sequential vs grouped wall time on Databricks.

---

## 11. End-to-end system results

### 11.1 Managed runs 🧮 (`data/manager_feedback.jsonl`, 2026-06-25 → 2026-09-30)

| Outcome | Runs | Where it stopped | Cause |
|---|---|---|---|
| Completed | 26 (76.5%) | — | — |
| Failed at validation | 1 | Phase 1 | copy stage missing container fields (early plan format) |
| Failed fast (≤ 4.5 s) | 3 | gates / early execution | plan assurance reject (1); 2 failed right after passing assurance (cause not logged in the feedback record) |
| Failed after 2 retries | 4 | execution | uncompilable filter / cloud errors (48–505 s) |

Planner fallback 0/34. Post-run assurance passed on all 26 completed.

Explain in the paper that early failures came from the system's evolution (old plan format,
compiler gaps since fixed), which motivates §10.2 and §10.3.

### 11.2 Formula cost of real runs 🧮

Per run (formula at observed runtime): 1-stage ≈ $0.021–0.028, 2-stage ≈ $0.026–0.041,
5-stage ≈ $0.09. The Manager's quick Phase-2b estimate under-estimates by ~8× (MAPE 86.5%,
n = 18) and is display-only; the Cost agent's estimate is the one the learning loop corrects.

### 11.3 Integration test ✅ (re-run today)

`python scripts/integration_test.py` → **ALL TESTS PASSED (4.1 s)**: Resource → Performance
→ Cost → Manager without cloud calls.

---

## 12. Ablation studies — with / without agents and combinations

This is the section reviewers will look for. Some cells can be filled **today** from
logged paired data; the rest need runs. Two kinds of runs:

- **Offline (no Azure cost):** replay logged plans/states through the agents with
  components switched off. Good for planner, assurance, resource, performance, cost, learning.
- **Live (Azure cost):** needed for real runtime / success rate. Keep a fixed benchmark of
  ~10 pipelines (3 CSV sizes × linear/fan-out × with/without aggregation) and 3 repeats.

### 12.1 Single-component ablations (remove one, keep the rest)

| ID | Configuration | Metric(s) | Status | Known value |
|---|---|---|---|---|
| A0 | **Full system** | success rate, plan validity, intent, runtime MAPE, cost MAPE | 🧮 partial | 76.5% success; runtime MAPE 8.8% (ML); cost MAPE 29.9% |
| A1 | Planner LLM → deterministic default plan only | intent accuracy, executable rate | 🧪 offline | default plan ignores intent → intent ≈ 0% except trivial prompts |
| A2 | Planner raw output, no repair layer | 8 checks + executable | 🧪 offline | routing 12% (§3.4) |
| A3 | Base Qwen2.5-7B instead of fine-tuned | 8 checks | ✅ | 50% vs 84% overall |
| A4 | Groq cloud LLM instead of local fine-tuned | 8 checks, latency, $ | 🧪 offline | — |
| A5 | No self-check / re-plan | % verified plans, latency | 🧪 offline | — |
| A6 | No intent guards (Guard 1/2) | intent-check accuracy, false flags | 🧪 offline | progression 15/18 → 27/27 (§4.2) |
| A7 | No structural assurance gate | % bad plans reaching executor (inject the 5 bad example plans + fuzzed plans) | 🧪 offline | — |
| A8 | No Resource Agent (planner's settings used) | feasibility violations, cost, runtime | 🧪 live | — |
| A9 | Resource heuristic only (no ML) | settings agreement, runtime | 🧪 live | — |
| A10 | No Resource correction factor | per-stage MAPE | 🧮 | 151%/224% vs 60%/62% (§5.4) |
| A11 | Performance formula only (no ML) | runtime MAPE | 🧮 | 81.3% vs 8.8% (§6.5) |
| A12 | No Performance gate (never abort on predicted failure) | wasted cloud seconds on doomed runs | 🧪 offline+live | — |
| A13 | No Cost Optimization | formula cost, runtime | 🧪 live | currently identical (fail-closed, no change applied) |
| A14 | No Learning agent (factors = 1.0) | cost MAPE | 🧮 | 50.3% vs 29.9% (§8.2) |
| A15 | No retries | success rate, time | 🧪 replay | 4 of 34 runs retried twice; all still failed (deterministic causes) |
| A16 | No `retryable:false` classification | wasted time on deterministic failures | ✅ | 3 attempts → 1, −40 s backoff (§10.2) |
| A17 | Sequential vs parallel execution groups | wall time | 🧪 live | — |
| A18 | No Monitor feedback to `dynamic_reallocate` | reaction to slow stages | 🧪 simulation | — |

### 12.2 Cumulative "agent stack" table (adding agents one at a time)

Run the benchmark with progressively more agents enabled. This is the "combination of
agents" table:

| Stack | Agents on | Plan valid % | Intent % | Run success % | Runtime pred. MAPE | Cost est. MAPE | Avg formula cost | Wasted cloud s |
|---|---|---|---|---|---|---|---|---|
| S1 | Planner (raw LLM) + Executor | 🧪 | 🧪 | 🧪 | n/a | n/a | 🧪 | 🧪 |
| S2 | + repair layer | 🧪 | 🧪 | 🧪 | n/a | n/a | 🧪 | 🧪 |
| S3 | + Assurance (self-check + gate) | 🧪 | 🧪 | 🧪 | n/a | n/a | 🧪 | 🧪 |
| S4 | + Resource | 🧪 | 🧪 | 🧪 | 🧪 (heuristic) | n/a | 🧪 | 🧪 |
| S5 | + Performance | 🧪 | 🧪 | 🧪 | 🧪 | n/a | 🧪 | 🧪 |
| S6 | + Cost | 🧪 | 🧪 | 🧪 | 🧪 | 🧪 | 🧪 | 🧪 |
| S7 | + Learning (full) | 🧪 | 🧪 | 🧪 | 🧪 | 🧪 | 🧪 | 🧪 |
| S8 | + Monitor/anomaly | (same) | (same) | 🧪 | 🧪 | 🧪 | 🧪 | detection P/R |

"Wasted cloud s" = cloud seconds spent on runs that failed or produced zero rows.

### 12.3 Pairwise interaction experiments

| Pair | Question | Design |
|---|---|---|
| Planner × Assurance | Does verification help the fine-tuned model less than the base model? | 2×2: {base, FT} × {self-check on, off} on the 40-prompt set |
| Resource × Performance | Does removing `baseline_s` (circular feature) hurt or help? | retrain perf model without it; compare real-run MAPE |
| Resource × Cost | Does cost optimization undo resource sizing? | log both plans; count conflicts |
| Performance × Cost | Does the learned deadline unlock savings safely? | runs 1–3 (no deadline) vs 4+ (deadline): % accepted, runtime overrun |
| Learning × Performance | Does the duration factor oscillate? | track factor + MAPE over 30 ML runs (open hypothesis in learning README) |
| Monitor × Resource | Does live reallocation fire correctly? | inject slow stages in simulation |

### 12.4 How to add switches (needed for offline ablations)

The only existing switch is `constraints["auto_apply_cost"]`. Suggested minimal additions
(env flags read in `manager.py`): `ABLATE_ASSURANCE_GATE`, `ABLATE_PERF_GATE`,
`ABLATE_COST`, `ABLATE_LEARNING`, `ABLATE_RESOURCE_ML`, `ABLATE_RETRIES`; in the planner:
`ABLATE_REPAIR`, `ABLATE_SELF_CHECK`. Log the active flags into each feedback record so
results can be grouped.

---

## 13. Baseline comparisons for the paper

| Baseline | What to compare | Status |
|---|---|---|
| Manual authoring (ADF UI + notebook by hand) | time-to-pipeline, errors; 3–5 users, 3 tasks | 🧪 small user study |
| Single general LLM (GPT-class / Groq) writing the whole config, no agents | plan validity, intent, executability | 🧪 offline |
| Rule-based template (the deterministic default) | intent coverage | 🧪 offline |
| Base Qwen2.5-7B (no fine-tune) | 8 checks | ✅ §3.3–3.4 |
| Heuristic resource sizing | settings, runtime | 🧮 §5.4, 🧪 live |
| Fixed SLA vs learned usual duration | false "slow" alarms across file sizes | 🧪 replay |

---

## 14. Qualitative case studies (good for figures/boxes)

1. **"Drop the cheap stuff"** → `unit_price >= 100`; typo `quantiy` mapped to `quantity` (§3.4).
2. **Hallucinated correction blocked:** verifier proposed `is_predator`; Guard 1 discarded it
   because no such column exists (§4.2).
3. **Uncompilable filter → retry storm:** `predator IS TRUE` failed to compile, retried twice,
   raised `failure` + `retry_storm` anomalies; now non-retryable and compiled correctly (§10.2).
4. **Serverless-only workspace** forced the split ADF-copy / Databricks-Jobs architecture (§9.1).
5. **Cost agent unsafe recommendation** (75% "saving" that broke a deadline) caught by audit (§7.2).
6. **Silent `or` fallback** drove the cost factor to its ceiling (§8.4).
7. **Streaming silent data loss:** merging stages kept only the first filter; fixed with
   `merged_steps` and multi-stage layout (stage 11).

---

## 15. Engineering quality results ✅

| Item | Value | Source |
|---|---|---|
| Full-project scan findings | 35 (4 critical, 11 high, 11 medium, 7 low, 2 no-action) + 80 lint findings | `docs/PROJECT_SCAN_FINDINGS.md` |
| Fixed and verified in staged logs | stages 1–14 with verification per stage | `docs/LOGIC_FIXES_LOG.md` |
| Regression checks per stage | integration test, 13 cost tests, ruff clean, 25 GET endpoints 200 | same |
| Security | injection-safe codegen, secrets moved to `.env` + Databricks secret scope, API key (timing-safe), localhost-only default, upload caps | scan + fixes |
| Codebase | 172 commits since 2026-03-22; FastAPI backend, React dashboard (15 pages) | git |

---

## 16. Figures and tables to produce

| Fig/Table | Content | Data source | Status |
|---|---|---|---|
| Fig 1 | Architecture / agent flow | `RESPONSIBILITIES.md`, frontend AgentFlow | draw |
| Fig 2 | Run lifecycle state machine (phases + gates) | `manager.py execute_run` | draw |
| Fig 3 | Planner training loss | notebook cell 24 | ✅ data |
| Fig 4 | Base vs FT per-check bars (in-dist + OOD) | §3.3–3.4 | ✅ data |
| Fig 5 | Dataset diversity (domains, stages, ops) | validator report | ✅ data |
| Fig 6 | v1 vs v2 dataset violations | §3.5 | 🧮 data |
| Fig 7 | Predicted vs actual runtime scatter (3 predictors) | `manager_feedback.jsonl` | 🧮 data |
| Fig 8 | Per-stage raw vs corrected duration error | `resource_feedback.jsonl` | 🧮 data |
| Fig 9 | Cost MAPE with/without learning | §8.2 | 🧮 data |
| Fig 10 | Correction factor over time + rollback | learning log | partial |
| Fig 11 | Intent-check accuracy progression 15/18 → 27/27 | §4.2 | ✅ data |
| Fig 12 | Cost model audit before/after R² | §7.2 | ✅ data |
| Fig 13 | Confusion matrix, outcome classifier | metrics.json report | ✅ data |
| Fig 14 | Feature importance (shows `baseline_s` = 0.908) | perf notebook | ✅ data |
| Table | Agent stack ablation (§12.2) | — | 🧪 |
| Table | Latency: local 7B vs cloud | ollama.log + Groq run | 🧮/🧪 |

---

## 17. Threats to validity / limitations (write these in, reviewers will ask)

- Small real-run sample (26 successful managed runs; ML runtime result n = 6, clean n = 3).
- Most real runs are small files (≈ 1 KB–4 MB) with 1–2 stages; results may not hold at scale.
- Synthetic training data for resource, performance, cost models; metrics measure agreement
  with labelers. Resource labels are calibrated to real telemetry; others are assumptions.
- "Actual cost" is formula-based, not billing.
- Planner in-distribution score is inflated by template-like data; OOD eval had 8 prompts
  and was on an earlier revision.
- Workers / node type are advisory on serverless Databricks; only DIU and shuffle
  partitions are actually applied.
- Performance model still uses `baseline_s` (circular feature).
- Student-tier Azure limits (4 workers, 8 DIU) bound all results.
- One user/environment; no user study yet.

---

## 18. Reproduction commands

Run from `unified/` with the project venv unless noted.

| Result | Command |
|---|---|
| Dataset validity + diversity | `python planner_agent/training/validate_dataset.py` |
| v1 dataset violations | `python planner_agent/training/validate_dataset.py planner_agent/training/datasets/synthetic_planner_dataset.jsonl` (per-rule counts: loop `validate_row` over rows) |
| Planner live eval | `python planner_agent/training/eval_live_planner.py [model]` (Ollama running) |
| Assurance examples | `python -m assurance_agent.examples.run_examples [--semantic]` |
| Resource invariants | `python -m resource_agent.examples.run_examples` |
| Perf model retrain + metrics | `cd performance_prediction_agent && python run_training.py` |
| Cost tests (repo root) | `python -m unittest test_cost_model_safety` |
| Cost audit (repo root) | `python audit_cost_model.py` |
| Integration | `python scripts/integration_test.py` |
| Real-run metrics (§6.5, §8.2, §11) | read `data/manager_feedback.jsonl`; success = `final_status in {feedback, completed}`; APE = \|pred − actual\| / actual; pairs: `predicted_duration_s` (resource), `perf_predicted_total_s` by `prediction_source`, `estimated_cost_usd` vs `cost_uncorrected_estimated_usd` vs `actual_cost_usd` |
| Per-stage correction (§5.4) | `data/resource_feedback.jsonl`, rows with `raw_predicted_duration_s` |
| Monitor history (§9) | `sqlite3 data/adf_monitor.db` — always add `WHERE run_id NOT LIKE 'demo%'` |
| LLM latency (§3.8) | parse `[GIN] … POST "/api/chat"` lines in `data/ollama.log` |

---

## 19. Suggested paper outline mapped to results

1. **Introduction** — problem, contributions C1–C6.
2. **Related work** — LLM agents for data engineering, text-to-pipeline/SQL, AutoML for
   resource tuning, self-correcting systems.
3. **System design** — §1, Fig 1–2, ownership matrix, single-owner principle (§6.4).
4. **Planner** — data generation + validation (§3.5), fine-tuning (§3.1–3.2), repair layer,
   self-check.
5. **Prediction & optimization agents** — resource (§5), performance (§6), cost (§7).
6. **Closed-loop learning** — §8.
7. **Evaluation** — headline table (§2), planner (§3.3–3.4, 3.6), verifier (§4.2), real-run
   prediction (§6.5, §5.4), learning ablation (§8.2), cost safety (§7.2–7.3), ablations (§12).
8. **Case studies** — §14.
9. **Limitations** — §17.
10. **Conclusion.**

**Minimum extra work before submission (in priority order):**
1. Re-run the planner OOD eval on the current model with ~40 prompts, base vs fine-tuned
   (+ Groq) — §3.4, A3/A4.
2. Raw vs repaired vs self-checked planner scores — §3.6, A2/A5.
3. ≥ 20 more real runs across sizes/stage counts → firm up §6.5 and §8.2, unlock cost
   deadline for §7.4.
4. Add ablation flags (§12.4) and run the agent-stack table (§12.2) on a 10-pipeline benchmark.
5. Anomaly detector P/R with injected runs (§9.2).

---
---

# PART B — Experiments run on 2026-10-03 (live log, updated as results arrive)

> Part A above catalogues what existed before today. **All Part B experiments are finished**
> (last update 2026-10-03, evening). Part B is new experiments run
> for the paper: **with vs without each agent**, **with vs without the whole system**,
> live Azure runs, streaming, parallel execution, and human-vs-system time.
> Each experiment says **what was done, how, the result, why the result came out that
> way, and what it means for the paper.** Status markers: ✅ done, ⏳ running, 🧪 planned.

## B0. Test setup — how everything was tested

| Item | Value |
|---|---|
| Machine | Apple M5, 16 GB unified memory (Ollama sees 11.8 GiB GPU memory), macOS |
| Local models | `planner-agent` (Qwen2.5-7B + our LoRA, Q4_K_M, built 2026-06-29 from the adapter trained 2026-06-28) and base `qwen2.5:7b-instruct` (Q4_K_M), both via Ollama |
| Sampling | temperature 0.2, top-p 0.8, `num_ctx` 4096, JSON mode — identical to production |
| Cloud | the user's **Azure for Students** subscription: ADF (copy), Databricks **serverless** jobs, Blob storage |
| Backend | `uvicorn main:app` single process, exactly as in production (`unified/README.md`) |
| Safety before live runs | `data/manager_feedback.jsonl`, `data/resource_feedback.jsonl`, `data/adf_monitor.db` (sqlite `.backup`) and `learning_policy_agent/data` + `versions/` copied to a scratch backup first, so today's runs can be separated from history |
| Code changes | **none to the agents.** All experiments drive the real code from outside (`scripts/paper_eval/`). Where an agent had to be "removed", the script calls the pieces separately (e.g. the planner's raw model output vs the same output after the repair layer) instead of editing the agent |
| Reproduce | `scripts/paper_eval/README.md` — one command per experiment |

**Why this design:** (1) the paired design — the *same* raw model output scored before
and after repair — removes sampling noise from the with/without comparison; (2) running
through the real HTTP API means live numbers include every real overhead (upload, gates,
pre-checks, polling); (3) no agent code was edited, so results describe the system as shipped.

**Mistakes made while testing (disclosed):**
- The first version of the experiment scripts changed directory before writing output, so
  result files landed in `unified/` — moved to scratch; the scripts now resolve output paths
  from the caller's folder.
- One injected fault (stage order) initially did nothing because of a Python tuple-assignment
  order bug; found because its catch rate equalled the clean-plan reject rate; fixed and re-run.
- The planner experiment ran concurrently with live planning, which slowed both (shared
  local model) and overheated the laptop; it was stopped at 12/24 cases and the other 12 were
  finished later without the self-check conditions (low-heat mode), so `*_full` has n = 12.
- An attempt to add an opt-in switch that bypasses the Performance gate (to measure false
  aborts, §B5.3) was blocked by the environment's safety policy and reverted; `manager.py`
  is unchanged (verified with `git diff`).

---

## B1. Planner — with vs without each planner component ✅ (24 prompts)

**What:** 7 conditions on the same prompts.

| Condition | Meaning |
|---|---|
| `default` | **without the AI** — deterministic default plan (what the system does if the model is down) |
| `base_raw` | base Qwen2.5-7B, raw JSON, nothing else |
| `base_repair` | same raw JSON + our deterministic repair layer |
| `base_full` | base model + repair + self-check/re-plan |
| `ft_raw` | **our fine-tuned model**, raw JSON |
| `ft_repair` | same raw JSON + repair layer |
| `ft_full` | **full planner as shipped**: fine-tuned + repair + self-check |

**How:** `scripts/paper_eval/ablation_planner.py`. 24 prompts over 3 schemas — sales
orders (12), zoo animals (6), IoT sensors (6) — covering canonical, free-form numeric and
string filters, derived columns, normalisation, aggregation, numbered stages, typos,
multi-condition, between and combined requests. One sample per prompt per model
(temperature 0.2). Each output is scored on:
`valid_json`, `structural` (Assurance rules), `safe` (executor's Azure-name/injection
check), `compiles` (every notebook stage turns into PySpark), `executable` (all three),
`intent` (regexes for what the user asked, e.g. `unit_price >= 100`), and
**`correct` = executable AND intent**. Paired design: the `*_repair` condition re-uses the
exact raw output of `*_raw` (the HTTP call is replayed), so the difference is caused by the
repair layer alone. The self-check conditions (`*_full`) need 2–3 extra model calls per
case; they were run on the 12 sales prompts only (stopped for laptop heat, see B0).

**Result:**

| Condition | n | Valid JSON | Structural | Safe | Compiles | **Executable** | **Intent** | **Correct** |
|---|---|---|---|---|---|---|---|---|
| default (no AI) | 24 | 100% | 100% | 100% | 100% | 100% | 0% | **0%** |
| base_raw | 24 | 100% | 0% | 4% | 0% | 0% | 33% | **0%** |
| base_repair | 24 | 100% | 100% | 100% | 100% | 100% | 4% | **4%** |
| base_full | 12 | 100% | 100% | 100% | 100% | 100% | 0% | **0%** |
| ft_raw | 24 | 100% | 100% | 33% | 33% | 33% | 96% | **33%** |
| ft_repair | 24 | 100% | 100% | 100% | 100% | 100% | 96% | **96%** |
| **ft_full (shipped)** | 12 | 100% | 100% | 100% | 100% | 100% | **100%** | **100%** |

By schema (correct): sales — ft_raw 2/12, ft_repair 11/12; zoo — ft_raw 0/6, ft_repair 6/6;
IoT — ft_raw 6/6, ft_repair 6/6. The only `ft_repair` miss is `s_agg` (below).

Other measurements: base model output was unusable after repair → **fallback to the default
plan in 15/24** cases (fine-tuned: **0/24**). Fine-tuned self-check verified 11/12 on the
first attempt and re-planned 1/12; base model verified only 5/12. One full-JSON plan generation
takes **~41 s** (fine-tuned, median, 39–48 s) and ~35 s (base) on the M5 when the model is
not shared; the plans are long JSON documents (≈ 1,000+ tokens).

**Why it came out this way:**
- *Default = 0% intent:* the default plan is a safe pass-through pipeline; it never applies
  the user's filter or aggregation — "a pipeline that runs" ≠ "the pipeline asked for".
- *Base raw = 0% executable:* the base model does not know our 9-key contract (wrong
  container/dataset shapes). It understands a third of the requests but in an unusable form.
- *Base + repair ≈ default (4%):* the repair layer fixes a plan's *shape* but needs roughly the
  right contract to start from; base output usually can't be read into it, so the system
  falls back to the default plan (15/24) — hence intent drops from 33% to 4%.
- *FT raw = 96% intent but 33% executable:* checked directly on a failing case — when the
  prompt names no containers, the model invents **capitalised names** (`Ingest`,
  `Transform`; Azure requires lowercase) and wires a notebook stage to read and write the
  **same** container — the "routing" weakness of Part A §3.4. How often this happens depends
  on the schema: all 6 IoT plans were executable raw, 0/6 zoo and 2/12 sales (observed, not
  yet explained — likely how close the schema is to the training domains' naming).
- *FT + repair = 96%:* the repair layer lowercases names, rewires stage chaining and fixes
  types — every fine-tuned plan becomes executable without touching intent.
- *Full = 100%:* the one remaining miss (`s_agg`: "average unit_price per region and a row
  count" — the model dropped the aggregation) is caught by the intent check and fixed by the
  single re-plan. The self-check recovers silent intent loss — the one error the
  deterministic repair cannot fix.

**What it means (claim C1 + ablation):** each component does a different job — fine-tuning
gives *understanding* (intent 33% → 96%), repair gives *executability* (33% → 100%),
self-check fixes *the last intent errors* (11/12 → 12/12 on sales). Only the combination is
reliably correct.

**Caveats:** one sample per prompt (repeat ×3 for mean ± std); self-check conditions on 12
prompts; intent scored by regex (checked by hand for the misses).

---

## B2. Resource Agent — with vs without ✅

**What:** what would run if the planner's settings went straight to Azure (no Resource
Agent) vs after the Resource Agent.
**How:** `ablation_offline.py`, 1,000 random planner-format plans from the validated
dataset (seed 20261003); `ResourceAgent.analyze()` on each; compared with the student-tier
hard limits (≤ 4 workers, ≤ 8 DIU).

| Metric | Without Resource Agent | With Resource Agent |
|---|---|---|
| Plans exceeding the subscription's hard limits | **248 / 1,000 (24.8%)** | **0 / 1,000** |
| — by size: small / medium / large / xlarge | 0/295, 0/301, 62/218, **186/186** | 0 everywhere |
| Mean workers per notebook stage | 4.08 | 2.07 (−49%) |
| Max workers | 16 | 4 |
| Mean DIU per copy stage | 6.51 | 3.45 (−47%) |
| Max DIU | 16 | 8 |
| Plans flagged infeasible (memory) | — | 0 |

**Why:** the planner's settings follow a generic size table (xlarge → 8–16 workers, 16 DIU)
that does not know the subscription's limits. The Resource Agent clamps to the tier, sizes
from demand (rows, operations), and splits over-sized parallel groups.
**Meaning:** without the Resource Agent, every xlarge pipeline and 28% of large ones would
request resources the account cannot provide (rejected by Azure or failing at submit),
and the rest would be provisioned ~2× higher on average.
**Caveat:** synthetic plans; the live part (runtime/cost with vs without) is 🧪 (needs
pinning the planner's settings via `custom_settings` on identical runs).

---

## B3. Assurance gate — with vs without (fault injection) ✅

**What:** inject 7 kinds of realistic plan errors and see where each is caught.
**How:** `ablation_offline.py`: 200 clean plans (that pass the gate) × 7 faults = 1,400 faulty
plans. "Without gate" = only what the executor checks before calling the cloud
(Azure-name safety + notebook code generation). "With gate" = structural assurance first.

| Injected fault | Caught by gate | Reaches the cloud **without** gate | Reaches the cloud **with** gate |
|---|---|---|---|
| Filter on a column that doesn't exist | 200/200 | **200/200** | 0 |
| Unsupported aggregation (`median`) | 200/200 | **200/200** | 0 |
| Required key missing (`execution_order`) | 200/200 | **200/200** | 0 |
| Stage order inverted | 200/200 | 0 (executor also refuses) | 0 |
| Unsafe container name | 0/200 | 0 (executor catches) | 0 |
| Code-injection string in filter | 200/200 | 0 (compiler neutralises) | 0 |
| Unknown stage type | 200/200 | 0 (executor refuses) | 0 |
| **Total** | 1,200/1,400 | **600/1,400 (43%)** | **0/1,400** |

Time to catch an error with the gate: **median 0.07 ms** (p95 0.12 ms) per plan, before any
cloud spend. Without it, a bad plan reaches Databricks; real history shows failed runs took
**48–505 s** of cloud time before failing (with 2 automatic retries), e.g. the uncompilable
`predator IS TRUE` filter.

**Why:** the executor only checks what it needs to *generate code* (names, syntax). A filter
on a missing column or a `median` aggregation is valid Python, so it compiles and fails only
inside the Spark job. The gate checks *meaning against the schema*.
**Meaning:** the two layers are complementary (defence in depth): the gate stops 3 fault
classes the executor cannot see; the executor stops name/injection faults the gate does not
check.

**Bug found by this experiment (not fixed — reported):** the gate's column check treats the
type name in `cast(x as double)` / `cast(x as integer)` as a column. It wrongly rejects
**614 / 5,000 (12.3%)** valid plans that use casts. Fix: add the SQL type names
(`double`, `integer`, `int`, `string`, `long`, `float`, `boolean`, `date`, `timestamp`) to
`sql_keywords` in `assurance_agent/config/allowed_operations.json`. Worth a sentence in the
paper's lessons: an evaluation harness found a false-positive class unit tests missed.

---

## B4. With vs without the whole system — manual work replaced ✅ (proxy) / 🧪 (timed)

**What:** what a person would have to author by hand for the same pipelines.
**How:** for each plan, the notebook code the system generates is counted (non-blank,
non-comment lines), plus ADF objects (datasets, pipeline, copy activities, linked service),
containers and sizing decisions. Two sets: 1,000 dataset plans and the 14 plans of real
completed runs.

| Per pipeline | Real completed plans (n = 14) | Varied plans (n = 1,000) |
|---|---|---|
| PySpark notebook lines generated | **89** (88–90) | **195** (87–363) |
| Notebooks | 1.0 | 2.2 |
| Storage containers | 2.1 | 4.2 |
| ADF objects (datasets, pipeline, activities, linked service) | 4.1 | 7.2 |
| Sizing decisions (DIU, workers, shuffle) | 2.5 | 5.4 |

System-side times measured: gate 0.07 ms, notebook generation **0.18 ms** for all stages,
planner repair 0.18 ms, full planning 29–53 s (uncontended), pre-checks 2–7 s.

**Caveat:** generated code includes boilerplate (blob I/O via the SDK, logging, row counts)
that a human might write shorter, so line counts are an upper-bound proxy, not effort.
The timed comparison is in B8.

---

## B5. Live Azure benchmark ✅ (12 runs)

**What:** the real product path on Azure, varying file size and pipeline shape.
**How:** `live_benchmark.py batch`: synthetic sales CSVs (seeded) — **xs** 1,000 rows
(61 KB), **m** 50,000 rows (3.1 MB), **l** 400,000 rows (25.5 MB); two requests —
`filter2` ("keep only rows where region = 'EU' and quantity > 5") and `agg3`
("Stage 1: keep rows where quantity > 5. Stage 2: average unit_price and a row count per
region"). Each (size, shape) planned once through `/api/planner/plan`, then run twice through
`/api/manager/run`, sequentially (no overlap), polling every 5 s. Every field of the final
run state is saved (`live/states/*.json`). Runs: 2026-10-03, ~18:00–19:30 IST.

### B5.1 Planning (6 plans) ✅
All 6 plans **verified on the first attempt**, 0 fallbacks. Latency 41–201 s (median 76 s) —
inflated because the planner experiment shared the local model at the same time.

### B5.2 All 12 runs ✅

| Size | Shape | Rep | Status | Execution (s) | Resource est. (s) | Perf ML pred. (s) | Perf outcome (conf.) | Rows out |
|---|---|---|---|---|---|---|---|---|
| xs | filter2 | 0 | completed | 102.9 | 150 | 172 | slowdown (0.60) | 172 |
| xs | filter2 | 1 | completed | 82.4 | 150 | 143 | slowdown (0.60) | 172 |
| m | filter2 | 0 | completed | 108.5 | 150 | 161 | slowdown (0.60) | 7,623 |
| m | filter2 | 1 | completed | 105.5 | 150 | 139 | slowdown (0.62) | 7,623 |
| l | filter2 | 0 | completed | 127.5 | 150 | 164 | slowdown (0.56) | 59,817 |
| l | filter2 | 1 | completed | 122.6 | 150 | 150 | slowdown (0.57) | 59,817 |
| xs | agg3 | 0 | completed | 144.2 | 270 | 243 | slowdown (0.52) | 5 |
| xs | agg3 | 1 | completed | 139.8 | 270 | 235 | slowdown (0.52) | 5 |
| m | agg3 | 0 | **aborted by perf gate** | — | 270 | 260 | failure (0.49) | — |
| m | agg3 | 1 | **aborted by perf gate** | — | 270 | 212 | failure (0.50) | — |
| l | agg3 | 0 | **aborted by perf gate** | — | 270 | 257 | failure (0.51) | — |
| l | agg3 | 1 | **aborted by perf gate** | — | 270 | 236 | failure (0.52) | — |

Summary: **8/12 completed, 0 execution failures, 4/12 aborted pre-execution by the
Performance gate.** Correct output every time (row-count check below). Run-to-run
repeatability: same pipeline ±2–20 s (xs filter2 102.9 vs 82.4 s; others within 5 s).

Mean execution time by size (filter2): xs 92.7 s, m 107.0 s, l 125.1 s — **a 418× bigger
file costs only +35% time.** agg3 (3 stages) on xs: 142.0 s (+49 s for the extra stage).

**Output correctness:** EU ≈ 1/5 of rows and quantity > 5 ≈ 15/20 → expected ≈ 15% of
input; observed 17.2% (xs), 15.2% (m), 15.0% (l) ✓. agg3 returned 5 rows = 5 regions ✓.
Identical row counts across repeats ✓ (deterministic).

### B5.3 Prediction accuracy on these runs ✅ (out-of-sample)

| Predictor | n | MAPE | median APE |
|---|---|---|---|
| Resource heuristic (corrected) | 8 | 53.6% | 44.0% |
| Performance ML (as applied) | 8 | 51.1% | 57.8% |
| Performance ML raw (3 runs that logged it) | 3 | 63.0% | 73.8% |
| **Cost estimate without learned correction** | 8 | **51.9%** | 42.5% |
| **Cost estimate with learned correction** | 8 | **27.4%** | 17.5% |

**Why runtime barely grows with size:** serverless Databricks spends most of each run on fixed
costs (job submit, cold start, `pip install`, SDK blob I/O); Spark work on 400k rows takes
seconds. The predictors scale with size and stage count more than reality does at this
scale → systematic over-estimation (every prediction above actual).

**Why the ML runtime model is less accurate here (51%) than in Part A (8.8%):** Part A's
6 runs were small 1–2 stage pipelines similar to its ~20 blended real training rows. These
are new shapes/sizes. **This is the honest out-of-sample number** and replaces 8.8% as the
headline for generalisation; 8.8% is "in-distribution".

**Why the retrain barely moved the metrics:** the retrain is evaluated on the synthetic
held-out set (21,008 rows), where 9 more real rows change little. Its effect has to be
judged on new real runs — 🧪 the next live batch.

**Why the cost correction works out-of-sample:** the cost formula's bias is mostly a constant
factor (it over-estimates duration-driven compute), so a single learned multiplier (0.79–0.86)
transfers to new pipelines. Runtime error is shape/size-dependent, so one multiplier helps less.
**Paper value:** replicates Part A §8.2 on fresh data: **with learning 27.4% vs without
51.9%** (n = 8). Together: 16 paired runs, both showing ~45% relative error reduction.

### B5.4 Finding: the Performance gate aborts runnable pipelines ✅

4/12 runs (2 configurations × 2 repeats) stopped before execution with outcome "failure"
at confidence **0.49–0.52**; the other class (slowdown) was 0.44–0.46. The identical
request on the xs file **completed** both times (~142 s), and the filter version completed at
every size. The fan-out plan in B7 was also aborted on the 3 MB file (P = 0.50).

**Why:** the gate aborts on the classifier's top class even when it is a coin-flip. The
classifier, trained on synthetic data, links size × stage count × aggregation to risk, and
3–25 MB with aggregation lands on its failure/slowdown boundary. Its failure-class precision
on its own synthetic test set is only 0.555 (Part A §6.2).

**What we could not measure:** whether these runs would succeed with the gate off — that
needs bypassing a safety gate, which the test environment's policy blocked (B0). Left to the
user: (a) run the two plans once with the gate temporarily disabled, or (b) add a confidence
threshold (abort only if P(failure) ≥ 0.7).
**Paper value:** a real false-abort example of a learned pre-execution gate (33% of runs in
this benchmark), and evidence for calibrated thresholds.

### B5.5 Learning loop observed live ✅

Two learning cycles fired during the benchmark (every 5 runs). From `learning_log.jsonl`:

| Event | Detail |
|---|---|
| duration factor 1.0 → 0.9689 | ML path over-estimates (mean actual/predicted 0.8965 over 10 ML runs) |
| cost factor rolled back 0.7856 → 0.8644 | post-change cost MAPE 195.6% vs 65.6% before |
| **retrain triggered** | ML duration MAPE 27% > 20% threshold over 10 runs |
| cost factor 0.8644 → 0.7902 | mean actual/estimated cost ratio 0.6172 over 26 runs |
| duration factor rolled back 0.9689 → 1.0 | post-change duration MAPE **68,094.7%** vs 26.6% |
| Resource copy factor drifted 0.530 → 0.33 | self-correction on today's copies (floor of its bounds) |
| **automatic retrain ran and deployed** (18:08–18:10) | real rows blended 20 → **29** (37 available, 8 held out); duration MAE 224.05 → 224.02 s; outcome balanced accuracy 0.713 → 0.716; CV 0.719 → 0.711; snapshot `20261003_180820_perf_models` kept for rollback; `models/metrics.json` changed in git because of this |

**Bug found (not fixed — reported):** the 68,094.7% is not real. The rollback review in
`learning_policy_agent/policy_engine.py` (`_review_pending_changes`, duration branch) counts
runs that were **aborted before executing** (perf-gate aborts log `actual_duration_s` ≈ 0.1 s,
so APE ≈ 2,000×). The main analyzer already excludes failed runs (`_run_failed`), the review
step does not. A correct change was rolled back because of this. One-line fix: add
`and r.get("success") is not False` (and/or `r.get("executed") is not False`) to the
`post_change` filter. **Paper value:** a concrete case of a safety mechanism misfiring on
unfiltered evidence — good for the lessons section, and fix before running more experiments.

### B5.6 Cost ✅
Formula cost per completed run (at measured runtime): $0.024–0.036 (2-stage), $0.041–0.043
(3-stage). Whole benchmark ≈ **$0.27** by the formula. Cost agent made 0 recommendations —
expected: no pipeline reached 3 comparable completed runs, so no learned deadline exists and
the agent stays fail-closed.

---

## B6. Streaming — single vs multi stage ✅

**What:** the same two-step streaming request built as one merged stream stage vs a chain
of stream stages; is the output the same, what does each incremental run cost, and is each
new file processed exactly once?
**How:** `live_benchmark.py streaming`: request "Stage 1: keep rows where region = 'EU'.
Stage 2: keep rows where quantity > 5." planned via `/api/planner/plan` with
`mode=streaming`, `stream_layout=single|multi`; stream started via
`/api/manager/stream/start`; then 3 data drops of 2,000 new rows each
(`/stream/{id}/data` uploads the file and runs one incremental tick); after each drop the
sink's total row count was read back.

| Layout | Stream stages | Drop 1 | Drop 2 | Drop 3 | Mean tick | Sink rows after each drop |
|---|---|---|---|---|---|---|
| single | 1 | 41.2 s | 39.4 s | 59.9 s | **46.8 s** | 285 → 567 → 852 |
| multi | 2 | 76.4 s | 75.0 s | 85.1 s | **78.8 s** | 285 → 567 → 852 |

Planning: 58.3 s (single), 55.7 s (multi), both verified.

**Results:**
- **Same output:** both layouts produced identical row counts after every drop ✓.
- **Exactly-once incremental processing:** each drop added 282–285 rows; expected
  2,000 × 1/5 (EU) × 15/20 (quantity > 5) ≈ 300 ✓. No file was re-processed (totals grow
  by one drop's worth, not cumulatively).
- **Single is 40.6% faster per tick** (46.8 vs 78.8 s).
- **Streaming tick vs batch run:** a single-stage tick (≈ 47 s for 2,000 new rows) is about
  half a batch run of the same filter on 1,000 rows (≈ 93 s, B5), because a tick has no ADF
  copy stage and processes only new files.

**Why:** multi-stage runs one Databricks job per stage in sequence, so every tick pays the job
start-up twice; single merges both filters into one job (`(region = 'EU') AND (quantity > 5)`).
Multi exists for cases single cannot express correctly (aggregation in the middle, a filter on a
column a later stage creates) — the planner switches to multi automatically then (Part A stage 11).
**Caveats:** one stream per layout, 3 drops each; drop-3 single (59.9 s) shows start-up variance.

---

## B7. Parallel vs sequential execution groups ✅

**What:** does running independent stages at the same time (execution groups) save time?
**How:** `live_benchmark.py parallel [size]`: one hand-built fan-out plan — ADF copy
raw → bronze, then two independent notebooks that both read bronze (a filter branch and a
summary branch). Run with groups `[[copy],[filter],[summary]]` (sequential) vs
`[[copy],[filter, summary]]` (parallel), alternating, 2 repeats each, through the real
Manager. Done on the 1,000-row file (xs) and the 50,000-row file (m).

| File | Mode | Run 1 | Run 2 | Mean execution |
|---|---|---|---|---|
| xs (61 KB) | sequential | 134.0 s | 153.4 s | **143.7 s** |
| xs (61 KB) | parallel | 98.3 s | 86.5 s | **92.4 s (−35.7%, −51 s)** |
| m (3.1 MB) | sequential | aborted by perf gate (P(failure) 0.50) | aborted (0.50) | — |
| m (3.1 MB) | parallel | 146.4 s | 108.4 s | 127.4 s |

**Why parallel is faster:** each Databricks notebook job pays a large fixed start-up cost
(serverless cold start + library install ≈ 40–60 s). Sequential groups pay it twice back to
back; parallel groups overlap the two start-ups, so wall time ≈ copy + the slower branch.
The saving (~51 s) is about one notebook start-up, as expected.

**Second evidence of false aborts (strengthens B5.4):** on the 3 MB file the *same three
stages* were aborted when grouped sequentially (P(failure) = 0.50) but passed the gate and
**completed successfully** when grouped in parallel (the only input that changed is the
grouping — the model's `n_execution_groups` / `parallel_ratio` features — which moved it
across the 0.5 boundary: P(failure) 0.50 → outcome slowdown at 0.56). Identical work ran fine → the
sequential aborts were false. This is the cleanest false-abort evidence we have, obtained
without bypassing the gate.

**Caveats:** n = 2 per cell; run-to-run spread up to 38 s (serverless start-up variance) —
report means with ranges and repeat ×5 for a significance test. The Performance agent's own
prediction also captured the saving (220 s sequential vs 137 s parallel predicted on xs).

---

## B8. Human vs system time 🧪 (system side ✅ measured live)

**System, measured.** How: each live run's decision log has a timestamp per phase
(`live/states/*.json`, field `decisions`); phase durations = differences between the
phase-start events. 14 completed live runs (B5 + B7), 6 aborted runs.

| Step | What the system does | Median (min–max) |
|---|---|---|
| Describe data + request | upload CSV, one sentence | user time (~1 min) |
| Design the pipeline | planner incl. self-check | 29–53 s uncontended (B1, Part A); 41–201 s while sharing the model (B5.1) |
| Validate + gate the plan | structural rules, name safety | **7 ms** (5–26 ms) |
| Size compute, predict runtime + cost, optimise | Resource, Performance, Cost agents | **0.56 s** (0.19–13.2 s) |
| Write code | notebook generation | 0.18 ms (B4) |
| Create containers, upload, ADF copy | Executor + ADF | **55.6 s** (49.8–92.4 s) |
| Run Databricks notebook stage(s) | Executor + Databricks | **44.7 s** (32.6–99.1 s) |
| Check the result, log feedback | post-run assurance + learning | **8.5 ms** |
| **Request → verified output in storage** | | **116 s** (83–154 s) of run time + planning |
| Reject a run predicted to fail | perf gate | 0.2 s (before any cloud spend) |
| Explain a failure | anomaly event + LLM root cause | automatic, right after the run |

So **request → correct output ≈ 2.5–4.5 minutes**, almost all of it Azure's own copy and
Spark start-up time; the system's own decision-making (gates, sizing, predictions, code
generation) takes **under 1 second** per run.

**Human, without the system — what a person must do** (same pipeline): create 2–4
containers; create ADF linked service, 2–4 datasets, pipeline + copy activity; write
and debug an ~90–200 line PySpark notebook (blob read/write, filter/agg); create a
Databricks job; choose DIU/workers/shuffle; trigger, wait, check output; on error, read
logs, fix, re-run (each failed cloud attempt costs ~1.5–8 min, measured: 48–505 s).

**How to measure the human side properly (protocol — needed before publishing a number):**
1. Participants: the authors + 2–4 classmates who know Azure basics. Record experience.
2. Tasks: T1 = `filter2` on the 3 MB file; T2 = `agg3` on the same file (both from B5).
3. Condition A (manual): Azure portal + Databricks UI, documentation allowed, no AI tools.
   Condition B (system): our UI. Counterbalance order (half do B first).
4. Record with a stopwatch: time to first successful output, number of failed cloud runs,
   time spent fixing errors, and whether the output is correct (row counts from B5).
5. Report median and range per condition; per-step breakdown for manual.
Effort model template for the write-up: `T_manual = T_setup(containers, ADF objects) +
T_code(lines) + T_sizing + Σ_failures (T_diagnose + T_rerun)`; fill each term with the
timed values. **Do not publish estimated human minutes without timing them** — reviewers
will ask for the measurement.

---

## B9. Updated with/without summary (all agents)

| Agent / component | Metric | **Without** | **With** | n | How measured | Status |
|---|---|---|---|---|---|---|
| Whole system | correct pipeline from a request | manual (B8) | 100% of 12 planner prompts executable+correct; live: 8/8 executed runs completed with correct output | 12 / 12 | B1, B5 | ✅/🧪 human time |
| Planner fine-tuning | intent correct (raw) | 33% (base) | 96% | 24 | B1 | ✅ |
| Planner AI at all | correct | 0% (default plan) | 96% (repair) / 100% (full, n=12) | 24 | B1 | ✅ |
| Repair layer | executable | 33% | 100% | 24 | B1 (paired) | ✅ |
| Self-check / re-plan | correct (sales prompts) | 11/12 | 12/12 | 12 | B1 | ✅ |
| Assurance gate | faulty plans reaching cloud | 43% | 0% | 1,400 | B3 | ✅ |
| Assurance gate | valid plans wrongly rejected | — | 12.3% (cast bug) | 5,000 | B3 | ✅ |
| Resource Agent | plans over hard limits | 24.8% | 0% | 1,000 | B2 | ✅ |
| Resource Agent | mean workers / DIU | 4.08 / 6.51 | 2.07 / 3.45 | 1,000 | B2 | ✅ |
| Resource self-correction | per-stage runtime MAPE | 151% / 224% | 60% / 62% | 5 / 6 | Part A §5.4 | ✅ |
| Performance agent (ML vs formula) | runtime MAPE, Part A runs | 81.3% (formula) | 8.8% (ML) | 13 / 6 | Part A §6.5 | ✅ |
| Performance agent on new shapes (out-of-sample) | runtime MAPE | — | 51.1% (resource heuristic 53.6%) | 8 | B5.3 | ✅ |
| Performance gate | runs aborted pre-execution | 0 | 4/12 runs (+ fan-out plan), all at P(failure) 0.49–0.52 | 12 | B5.4 | ✅ (outcome without gate 🧪) |
| Learning agent | cost-estimate MAPE (history) | 50.3% | 29.9% | 8 | Part A §8.2 | ✅ |
| Learning agent | cost-estimate MAPE (new live runs, out-of-sample) | 51.9% | 27.4% | 8 | B5.3 | ✅ |
| Executor retry classification | attempts on deterministic failure | 3 (+40 s backoff) | 1 | — | Part A §10.2 | ✅ |
| Parallel groups | execution time (xs fan-out) | 143.7 s (sequential) | 92.4 s (−35.7%) | 2+2 | B7 | ✅ |
| Streaming layout | tick time (same output) | 78.8 s (multi) | 46.8 s (single, −40.6%) | 3+3 drops | B6 | ✅ |

---

## B10. What else can be done (ranked by value for the paper ÷ effort)

1. **Repeat B1 ×3** (and the self-check conditions on zoo/IoT) → mean ± std.
2. **Timed human study (B8)** — the single most convincing "with vs without system" number.
3. **Performance gate:** run the 2 aborted plans once without the gate (user decision), and/or
   evaluate a confidence threshold offline on the synthetic test set (precision/recall of
   "abort" at thresholds 0.5–0.9) — turns a weakness into a calibrated-gate result.
4. **Fix the cast false-reject** and re-run B3 → report 0% false rejects after the fix.
4b. **Fix the learning rollback filter** (B5.5) before more live runs, otherwise aborted runs keep reverting good corrections.
5. **More live runs** (≥ 20) across sizes/shapes so the learning loop's duration factor and
   the cost agent's learned deadline (needs 3 comparable runs) kick in → a real
   cost-optimisation with/without result.
6. **Groq/cloud LLM planner** on the same 24 prompts — local-vs-cloud quality/latency/cost table.
7. **Anomaly detector P/R** with injected labelled runs (Part A §9.2).
8. **Retrain performance model without `baseline_s`** (circular feature) and compare on B5 runs.

---

## B11. Paper-writing kit

**Abstract-ready sentences (fill bracketed values only if they change):**
- "A 7B model fine-tuned with QLoRA on 5,000 rule-validated synthetic examples (0.53% of
  parameters trained, 4.5 GPU-hours on a T4) raises intent accuracy on free-form requests
  from 33% to 96% over the base model on 24 prompts; a deterministic repair layer raises
  executability from 33% to 100%, and a single self-verification re-plan brings end-to-end
  correctness to 100% on [12] prompts."
- "A structural assurance gate blocks 100% of [1,400] injected faulty plans before any
  cloud spend in under 0.1 ms, while 43% would otherwise reach Databricks and fail at run
  time."
- "Without the Resource Agent, 24.8% of plans request more compute than the subscription
  allows; with it, none do, at half the provisioned workers."
- "On real Azure runs, a closed-loop learning agent reduced cost-estimate error from 50.3%
  to 29.9%, and per-stage duration error from 151–224% to 60–62%."

**Claim → evidence map:**

| Claim | Evidence | Section |
|---|---|---|
| Fine-tuning gives understanding | intent 33% → 96% | B1, A3.3–3.4 |
| Deterministic layers give reliability | executable 33% → 100%; gate 43% → 0% | B1, B3 |
| Agents are complementary, not redundant | each removal hurts a different metric | B9 |
| Learning loop helps on real data | 50.3% → 29.9% cost MAPE | A8.2 |
| Honest limitations | perf-gate false aborts, cast false rejects, small n | B5.3, B3, A17 |

**Figures to make from Part B data:** grouped bars of B1 (7 conditions × executable/intent/
correct); stacked bar of B3 (where each fault is caught, with vs without gate); B2 size-bucket
bar (limit violations without Resource); B5 predicted-vs-actual scatter by size; B8 time
breakdown (system vs measured manual).

**Data files** (all under `unified/data/paper_eval/`, git-ignored — attach as supplementary
material): `planner_ablation_24cases.json` (B1), `offline_results.json` (B2–B4),
`live/batch.jsonl` (B5), `live/parallel.jsonl` + `live_xs/parallel.jsonl` (B7),
`live/streaming.jsonl` (B6), full run states `live*/states/*.json` (B8 phase times),
run logs `*.log`, and `state_backup_before_live/` (feedback logs, monitor DB and learning
state as they were before the live runs). Test CSVs are not kept; `live_benchmark.py`
regenerates them identically from fixed seeds.
