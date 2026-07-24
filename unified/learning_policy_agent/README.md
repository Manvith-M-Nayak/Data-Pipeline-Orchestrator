# Learning & Policy Update Agent

Closes the feedback loop between predicted and actual pipeline outcomes. It reads what the
Central Manager already logs, measures how wrong the Performance Prediction Agent and Cost
Optimization Agent actually were, and gradually nudges two correction factors toward reality —
safely, with evidence gating and automatic rollback if a correction makes things worse.

This document reflects the system as verified against real production runs, including two
real bugs found and fixed after initial deployment (see [Known Issues Found & Fixed](#known-issues-found--fixed-in-production)).
Treat that section as required reading before modifying any file in this agent — both bugs
were subtle, silent, and each one individually invalidated an entire correction factor for a
period of time without raising any error.

---

## What this agent does NOT do

Boundary discipline matters here as much as anything the agent actively does:

- It does **not** touch the Resource Agent's own allocation logic. The Resource Agent already
  has a complete, closed self-correction loop (`record_actual()` → `get_correction_factor()`,
  damped 50% toward observed ratio, per stage *type*). This agent's `resource_headroom_factors`
  is a **diagnostic-only**, per pipeline *signature* signal computed from a coarser source
  (overall duration ratio, not per-stage-type) — it is deliberately never fed back into the
  Resource Agent, to avoid double-correcting the same thing from two different granularities.
- It does **not** touch the Performance Prediction Agent's formula-path predictions. That path
  already self-corrects via its own damped history adjustment inside `performance_agent.py`.
  `duration_correction_factor` applies **only** to `ml_model`-sourced predictions, which get
  zero correction of their own (frozen at training time until the next retrain).
- It does **not** re-run or re-plan anything. It is a passive observer that updates policy
  values other agents read (or the Manager applies on their behalf) — never a live actor in
  the pipeline execution path itself.
- It does **not** have real Azure billing data. All cost numbers — estimated and "actual" —
  come from the same formula (`CostOptimizationAgent._estimate_cost()`), just with predicted
  vs. real elapsed duration substituted in. This is a real, useful signal for calibrating the
  *formula's* accuracy, but it is not a substitute for real invoice reconciliation.

---

## Architecture

```
learning_policy_agent/
├── __init__.py
├── learning_agent.py       Orchestrator — the only file the Manager calls into directly
├── feedback_collector.py   Phase 1 — reads manager_feedback.jsonl, normalizes to a fixed schema
├── error_analyzer.py       Phase 2 — turns normalized records into ratio/error statistics
├── policy_engine.py        Phase 3 + 5 — evidence-gated gradual updates + rollback review
├── retraining_manager.py   Phase 3 — decides whether/when to retrain the ML model
├── safety.py               Phase 5 — snapshot/rollback for every policy change
├── router.py               FastAPI endpoints (status, log, rollback) for the dashboard
├── data/
│   ├── policies.json       Current policy values — the actual state this agent maintains
│   ├── learning_log.jsonl  Append-only audit trail: every policy_update / policy_review /
│   │                       retrain_triggered event, ever
│   ├── cycle_state.json    Just a run counter (runs_since_cycle) — resets every 5 runs
│   └── retrain_state.json  RetrainingManager's own bookkeeping (last retrain, lock state)
└── versions/               Timestamped snapshots of policies.json, taken before every write
```

**Nothing here writes to `manager_feedback.jsonl`.** That file is owned entirely by
`CentralManager.record_feedback()` in `manager.py`. This agent only reads it.

---

## Data flow, end to end

```
1. Pipeline run completes (success or failure)
        │
2. manager.py: CentralManager.record_feedback()
   writes ONE line to unified/data/manager_feedback.jsonl
   then calls get_learning_agent().on_run_recorded()
        │
3. learning_agent.py: LearningPolicyAgent.on_run_recorded()
   bumps a counter (cycle_state.json). Every 5 runs (CYCLE_EVERY_N_RUNS), triggers run_cycle().
        │
4. run_cycle():
   a. FeedbackCollector.load_records()      — reads + normalizes ALL records in the file
   b. ErrorAnalyzer.analyze(records)        — computes ratios/mape/counts over a sliding window
   c. PolicyEngine.evaluate_and_apply(...)  — reviews pending changes, then maybe proposes new ones
   d. RetrainingManager.should_retrain(...) — checks ML-path duration_mape against threshold
   e. PolicyEngine.check_resource_agent_drift() — diagnostic flag only, never corrects anything
        │
5. policies.json updated (if anything changed) — snapshotted first via SafetyManager
   learning_log.jsonl appended — one line per change/review event
```

### Step 2 detail — what actually gets logged per run

`manager.py`'s `record_feedback()` writes (relevant fields only):

| Field | Meaning |
|---|---|
| `final_status` | `"feedback"` for successful runs (yes — `record_feedback()` runs **before** `state.status` is bumped to `"completed"`), `"failed"` for aborted/failed runs |
| `actual_duration_s` / `predicted_duration_s` | Resource Agent's baseline prediction vs. real wall-clock time |
| `perf_predicted_total_s` / `perf_uncorrected_total_s` | Performance Prediction Agent's own forecast — corrected and raw, respectively, if a `duration_correction_factor` was applied |
| `prediction_source` | `"ml_model"` or `"formula"` — which path produced the performance prediction |
| `learning_correction_applied` | The `duration_correction_factor` value applied to this run's prediction, or `null` |
| `estimated_cost_usd` | Cost Optimization Agent's own pre-execution estimate (`state.cost_optimization["estimated_cost"]["total_usd"]`) |
| `cost_uncorrected_estimated_usd` / `cost_correction_applied` | Same idea as duration's pair, for cost |
| `cost_estimate_usd` | **A different, unrelated number** — the Manager's own cheap Phase 2b estimate (ADF + DBX formula, computed before the Cost Optimization Agent even runs). Kept for its own dashboard purposes. **Never use this as the "estimate" half of a cost accuracy comparison** — see [Known Issues](#known-issues-found--fixed-in-production). |
| `actual_cost_usd` | `CostOptimizationAgent.estimate_actual_cost()` — same formula as `estimated_cost_usd`, with `actual_duration_s` substituted in. **Not real Azure billing.** |
| `assurance_passed`, `plan_assurance_passed` | Post/pre-execution assurance check outcomes |

### Step 4a detail — normalization (`feedback_collector.py`)

Raw records get mapped onto a fixed schema before `ErrorAnalyzer` ever sees them. The two
fields every future contributor needs to understand:

- **`success` is derived, not copied.** `final_status` is converted: `"feedback"` /
  `"completed"` / `"success"` / `"succeeded"` / `"ok"` → `True`; `"failed"` / `"failure"` /
  `"error"` / `"aborted"` → `False`. **`final_status` itself is never passed through to
  downstream consumers** — anything in `error_analyzer.py` or `policy_engine.py` that needs to
  know if a run failed must check `success`, not `final_status` (see Known Issues — this exact
  mistake was made and fixed once already).
- **`predicted_duration_s` is a coalesced field**: `perf_predicted_total_s or baseline_predicted_s`.
  The Performance Prediction Agent's own forecast is preferred; the Resource Agent's baseline
  is only a fallback for older records that predate the Performance Agent's forecast being logged.
- **`estimated_cost_usd` must prefer the Cost Optimization Agent's own number**, not the
  Manager's `cost_estimate_usd`. See Known Issues for exactly why this matters and what breaks
  if it's ever reversed.

---

## Policies reference

All values live in `learning_policy_agent/data/policies.json`. Defaults, on a fresh reset:

```json
{
  "duration_correction_factor": 1.0,
  "cost_correction_factor": 1.0,
  "resource_headroom_factors": {},
  "retrain_error_threshold": 0.20,
  "failure_flag_threshold": 0.30,
  "min_runs_for_update": 10,
  "learning_rate": 0.30,
  "factor_bounds": {"min": 0.3, "max": 2.0},
  "cost_learning_rate": 0.30,
  "cost_factor_bounds": {"min": 0.3, "max": 2.0},
  "rollback_review_min_runs": 5,
  "rollback_tolerance": 0.05,
  "flagged_signatures": {},
  "_pending_reviews": {}
}
```

| Policy | Consumed by | Learned from | Notes |
|---|---|---|---|
| `duration_correction_factor` | `manager.py`'s `predict_performance()` — applied **only** when `prediction_source == "ml_model"` | `by_source["ml_model"]` ratios only (never the blended window average) | Formula path is excluded on purpose — it already self-corrects; applying this on top would double-correct it |
| `cost_correction_factor` | `manager.py`'s `optimize_cost()` — applied to the Cost Optimization Agent's `estimated_cost.total_usd`, unconditionally | All records with both `estimated_cost_usd` and `actual_cost_usd` present | No source split needed — `_estimate_cost()` is one formula regardless of whether ML or heuristic recommendations were generated |
| `resource_headroom_factors` | Nothing (diagnostic only) | Per-signature duration ratio, when > 1.3 | Deliberately never wired into the Resource Agent — see [What this agent does NOT do](#what-this-agent-does-not-do) |
| `retrain_error_threshold` | `RetrainingManager.should_retrain()` | N/A (a threshold, not learned) | Triggers when `ml_model` `duration_mape` exceeds this over the window |
| `failure_flag_threshold` | Per-signature flagging logic | N/A (a threshold, not learned) | 30% failure/assurance-failure rate → added to `flagged_signatures` for human review |
| `min_runs_for_update` | Both correction factors' evidence gates | N/A | Gates on the **relevant bucket's** count (`ml_runs` for duration, `cost_runs` for cost) — not the total window size |
| `learning_rate` / `cost_learning_rate` | The gradual-move formula for each factor | N/A | Deliberately separate constants — duration's bounds were tuned against real observed ratios; cost had none at the time this was built, so it gets its own independently-tunable knob rather than reusing an untested one |
| `factor_bounds` / `cost_factor_bounds` | Clamp on gradual movement | N/A | `0.3` floor (not `0.5`) because real `ml_model` duration ratios have been observed as low as `0.39` |
| `rollback_review_min_runs` | `_review_pending_changes()` | N/A | How many post-change runs are needed before judging a change |
| `rollback_tolerance` | `_review_pending_changes()` | N/A | Only rolls back if post-change error is worse by more than this margin (not noise) |
| `flagged_signatures` | Human review (surfaced in dashboard/API) | Per-signature failure rates | Once flagged, stays flagged until manually cleared |
| `_pending_reviews` | Internal bookkeeping | N/A | Not meant to be hand-edited. One entry per correction factor currently awaiting its post-change review |

---

## How a correction factor actually moves

Both `duration_correction_factor` and `cost_correction_factor` follow the identical mechanism:

1. **Evidence gate.** Nothing happens until the relevant run count (`ml_runs` for duration,
   `cost_runs` for cost) reaches `min_runs_for_update` (default 10). This is FR5: a pattern,
   not one bad run.
2. **Gradual move, not a snap.** The new value is `current + (target − current) × learning_rate`
   (default 30%), clamped to `factor_bounds`. A single noisy batch can't send the factor
   swinging to an extreme in one step.
3. **Change recorded as pending.** `_pending_reviews[policy_name]` stores the old value, new
   value, and the pre-change error rate (mape) — this is what "worse" gets measured against later.
4. **Rest period after a rollback.** If a factor was just rolled back this cycle, it is **not**
   immediately eligible to propose a new change — it rests for one full cycle at the reverted
   value first. Without this, the rollback safety net would never actually rest anywhere.
5. **Review, next cycle.** Once `rollback_review_min_runs` (default 5) new records have landed
   *after* the change took effect, the next cycle computes the post-change error rate and
   compares it to the pre-change one:
   - Worse by more than `rollback_tolerance` (default 5%) → **rolled back** to the old value.
   - Otherwise → **confirmed**, value kept.
6. **Every change is snapshotted first.** `SafetyManager.snapshot()` copies `policies.json`
   into `versions/` before any write, so any change is manually reversible via
   `SafetyManager.rollback(version_id)` regardless of whether the automatic review catches it.

This full cycle — evidence gate → gradual move → pending → automatic rollback — has been
**confirmed on real production data**, for both directions (a change that held, and a change
that got correctly reverted). See [Verifying it's working](#verifying-its-working).

---

## Known issues found & fixed in production

These were each silent — no exception, no error log, just a policy quietly not doing what it
was supposed to. Documented here so nobody reintroduces them.

### 1. `estimated_cost_usd` vs `cost_estimate_usd` — field name/priority mismatch

Two completely different numbers exist in every feedback record:
- `cost_estimate_usd` — the Manager's own cheap Phase 2b formula (~$0.004 range in testing)
- `estimated_cost_usd` — the Cost Optimization Agent's own estimate (~$0.05-0.11 range in
  testing), computed with the *same* formula used for `actual_cost_usd`

**Bug 1 (in `error_analyzer.py`, initial build):** it read `record.get("estimated_cost_usd")`,
but `manager.py` was only logging `cost_estimate_usd` at the time — so `cost_ape`/`cost_ratio`
silently computed as `None` forever, no matter how many runs accumulated. Fixed by adding
`estimated_cost_usd` to what `manager.py` logs.

**Bug 2 (in `feedback_collector.py`, introduced by a well-intentioned but backwards
backward-compatibility shim):**
```python
est_cost = _to_float(raw.get("cost_estimate_usd") or raw.get("estimated_cost_usd"))
```
Python's `or` picks the first truthy value. `cost_estimate_usd` is always present and
non-zero, so it **always won** — `estimated_cost_usd` never got used, silently, for every
record. This produced ratios around **11-14** (real: `actual_cost_usd ÷ cost_estimate_usd`)
instead of the correct ~0.5-1.15 range, and drove `cost_correction_factor` straight to its
`2.0` ceiling from corrupted data. Fixed by reversing the priority:
```python
est_cost = _to_float(raw.get("estimated_cost_usd") or raw.get("cost_estimate_usd"))
```
`cost_estimate_usd` is now only a fallback for legacy records that predate
`estimated_cost_usd` existing at all.

**Lesson:** whenever two fields could plausibly answer "what was the cost estimate," verify
which one is actually populated in real logged data before assuming an `or` fallback is
harmless — it isn't, if one side is always truthy.

### 2. `final_status` vs `success` — checked the wrong field after normalization

A failed run's `actual_duration_s` includes retry backoff sleep (10s + 30s) and repeated
failed connection attempts — not real execution time. Including it in `duration_ratio` or
`cost_ratio` teaches both correction factors from noise.

The first fix attempt added a filter checking `record.get("final_status") == "failed"` in
`error_analyzer.py`. This looked correct in isolation and passed a standalone unit test — but
`ErrorAnalyzer.analyze()` is only ever called with **already-normalized** records from
`FeedbackCollector.load_records()`, and `normalize()` converts `final_status` into a `success`
boolean and **never passes `final_status` through**. The filter was checking a field that
doesn't exist on the data it actually receives — silently always `False`, doing nothing.

Fixed to check `record.get("success") is False` instead — confirmed correct via a full
`normalize()` → `analyze()` round-trip test using real failed-run data, not just a standalone
dict.

**Lesson:** a fix tested against synthetic data matching your mental model of the record shape
is not the same as a fix tested against what the real call chain actually produces. Test
through the real pipeline, not a shortcut.

### 3. `cost_optimizer.py` — missing zero-worker guard (regression)

`_suggest_shuffle_tuning()` originally divided `current_shuffle // max_workers` without
guarding against `max_workers == 0` (a driver-only/0-worker notebook stage — common on small
test files). This was found and fixed once, then reintroduced in a later teammate patch that
branched from an older copy of the file. Restored:
```python
max_workers = max((a.get("workers", 1) for a in notebook_allocs), default=1)
max_workers = max(max_workers, 1)  # guard against 0-worker (driver-only) stages
```

### 4. `cost_correction_factor` was a stub, not a real policy, in the initial build

The very first version only had an informational flag (`cost_estimate_accuracy_flag`) —
logged when `cost_mape > 0.15`, but never actually moved `cost_correction_factor`, never
evidence-gated it, never reviewed it. It was built from scratch to mirror
`duration_correction_factor`'s full mechanism (gate → gradual move → pending → rollback
review), using its own separate `cost_learning_rate`/`cost_factor_bounds` rather than reusing
duration's untested-for-cost values.

---

## A currently-unconfirmed hypothesis worth investigating

On real production data, `duration_correction_factor` was learned, applied, and then rolled
back once already (post-change `duration_mape` of 95.6% vs. a pre-change 56.2%) — a real,
correct rollback. One possible contributing mechanism, **not yet confirmed**:

`FeedbackCollector.normalize()` sets `predicted_duration_s = perf_predicted_total_s or
baseline_predicted_s`, and `perf_predicted_total_s` is the **already-corrected** value once a
`duration_correction_factor` is live. This means once a correction is active, the next cycle's
ratio calculation is comparing actual duration against an already-corrected prediction, not
the raw model output the factor is meant to be calibrated against — which could produce
exactly this kind of measure-correct-remeasure oscillation.

This has not been confirmed against `performance_agent.py`'s `predict()` method (not yet
reviewed). If `duration_correction_factor` continues oscillating (proposed → rolled back →
proposed → rolled back) over further cycles rather than settling, this is the first place to
investigate. If it stabilizes on its own, the earlier instability was more likely just an
early, noisy sample settling down as more data accumulated.

---

## Verifying it's working

No claim in this document should be taken on faith — here's exactly how each part was
actually confirmed against real data, not just unit tests.

**1. Evidence gate clears correctly.** After enough successful runs, check:
```bash
cat learning_policy_agent/data/policies.json
```
`duration_correction_factor` / `cost_correction_factor` should move off `1.0` only once the
relevant run count reaches `min_runs_for_update` (10 by default) — not before.

**2. The gradual-move math is exact.** Confirm `new = round(min(max(old + (target - old) *
learning_rate, lo), hi), 4)` — e.g. a target ratio of `0.7077` with `old=1.0`, `lr=0.30` gives
`1.0 + (0.7077-1.0)*0.30 = 0.9123` (this exact case was verified on real data).

**3. A change actually applies live**, not just sits in `policies.json`:
```bash
tail -5 data/manager_feedback.jsonl
```
Look for `learning_correction_applied` (duration) or `cost_correction_applied` (cost)
populated with the current factor, and `perf_uncorrected_total_s` /
`cost_uncorrected_estimated_usd` showing the pre-correction raw value alongside it.

**4. The rollback safety net actually fires**, in both directions:
```bash
cat learning_policy_agent/data/learning_log.jsonl
```
Look for `"type": "policy_review"` entries with `"action": "confirmed"` or `"action":
"rolled_back"`. Both have been observed on real production data — a `duration_correction_factor`
change that got rolled back after a real regression, confirming the whole loop (propose →
apply → measure → revert) works end-to-end without any manual intervention.

**5. Failed runs don't corrupt the correction factors.** Deliberately include a run that fails
mid-pipeline (any real failure works) and confirm `cost_runs`/`ml_runs` in the next cycle's
metrics don't count it, and the ratio isn't skewed by its (mostly retry-sleep, not real work)
`actual_duration_s`.

---

## Resetting learning state

To start over with a clean slate (e.g. before a fresh batch of test runs):

```bash
# Back up first (optional but recommended)
mkdir -p backup_before_reset
cp data/manager_feedback.jsonl backup_before_reset/ 2>/dev/null
cp learning_policy_agent/data/policies.json backup_before_reset/ 2>/dev/null
cp learning_policy_agent/data/learning_log.jsonl backup_before_reset/ 2>/dev/null
cp learning_policy_agent/data/cycle_state.json backup_before_reset/ 2>/dev/null
cp learning_policy_agent/data/retrain_state.json backup_before_reset/ 2>/dev/null
cp -r learning_policy_agent/versions backup_before_reset/versions 2>/dev/null

# Then clear everything this agent owns
rm -f data/manager_feedback.jsonl
rm -f learning_policy_agent/data/policies.json
rm -f learning_policy_agent/data/learning_log.jsonl
rm -f learning_policy_agent/data/cycle_state.json
rm -f learning_policy_agent/data/retrain_state.json
rm -rf learning_policy_agent/versions
mkdir -p learning_policy_agent/versions   # SafetyManager expects this dir to exist
```

`data/resource_feedback.jsonl` is **not** part of this agent — it belongs to the Resource
Agent's own separate self-correction loop, which this agent only reads diagnostically
(`check_resource_agent_drift()`). Clear it separately if you specifically want to reset the
Resource Agent's own state too.

---

