# Agent Responsibilities & Decision Boundaries

This document is the authoritative map of **which agent owns which decision** in the
pipeline-orchestration platform. It exists to prevent two agents from making — or
contradicting — the same decision. If you add a decision, add it here first.

The guiding rule:

> **Each decision has exactly one owner.** Every other agent may *read* that decision but
> must not recompute or override it. The one exception is a documented *fallback* (an owner
> that is temporarily unavailable), which is always explicitly labelled.

The Central Manager is the orchestrator: it sequences the agents and carries their outputs
on a single `RunState`. It makes **no domain decisions itself** — it only gates on the
booleans the owners return (feasible / assured / outcome).

---

## Ownership matrix

| Decision | Owner | Consumers (read-only) |
|---|---|---|
| Pipeline design: stages, containers, execution order, transformations | **Planner Agent** | everyone |
| `recommended_settings` (workers/DIU/node/shuffle) as an *initial hint* | **Planner Agent** | Resource Agent (may override) |
| Plan is structurally & semantically valid | **Assurance Agent** | Manager (hard gate) |
| Dependency graph → parallel execution groups | **Central Manager** (`analyze_parallelism`) | Resource, Performance |
| **Compute settings per stage** (workers, DIU, peak memory, shuffle partitions, node type) | **Resource Agent** | Performance, Executor, Manager, UI |
| Plan fits the student-tier hard limits (feasibility) | **Resource Agent** | Manager (hard gate) |
| Mid-run compute re-allocation from live telemetry | **Resource Agent** (`dynamic_reallocate`) | Manager, Monitor UI |
| **Total runtime / bottleneck / SLA / success-or-fail outcome** | **Performance Prediction Agent** | Manager (hard gate on `outcome == "failure"`) |
| $ cost estimate | **Central Manager** (`estimate_cost`) | UI |
| Live run status, anomalies, per-pipeline runtime from *history* | **Monitor Agent** | UI, Resource (`dynamic_reallocate`) |
| Retry / backoff on execution failure | **Central Manager** (`execute_with_retry`) | — |
| Post-run assurance (did it actually succeed) | **Central Manager** (`run_assurance`) | UI, feedback log |

---

## The overlap that was removed (duration prediction)

Before this change, **three** places predicted how long a pipeline would take, and they
fed on each other:

1. **Resource Agent** produced a per-stage `estimated_duration_s` from a hand-tuned formula.
2. **Performance Prediction Agent**'s ML model took the Resource Agent's estimate as its
   single strongest feature (`baseline_s`, importance ≈ 0.91) and re-predicted the total.
   → The "ML" prediction was largely a re-scaling of the Resource Agent's formula: a
   **circular dependency**, not an independent signal.
3. **Monitor Agent** predicted per-pipeline runtime from history via an LLM.

### New, non-overlapping split

- **Resource Agent → resource management only.** It now recommends *settings* (workers,
  DIU, peak memory, shuffle partitions, node type) via a supervised model
  (`resource_agent/models/resource_models.pkl`, heuristic fallback). Its internal
  `duration_s` fields are retained **only** as a sizing/reallocation aid and are explicitly
  **not** the plan's runtime. They are never surfaced as the authoritative runtime.

- **Performance Prediction Agent → the single pre-execution runtime/outcome authority.**
  It *consumes* the Resource Agent's chosen settings and forecasts total runtime,
  bottleneck stage, SLA breach risk, and success/slowdown/failure. It no longer double-owns
  sizing.

- **Monitor Agent → post-hoc / live only.** It reports what is happening or has happened to
  real runs (status, anomalies, historical runtime). It does not make pre-execution
  decisions; it *feeds* the Resource Agent's `dynamic_reallocate` during a run.

This breaks the circular `baseline_s` dependency: the Resource Agent decides *what to
provision*; the Performance Agent decides *how it will perform*. Two questions, two owners.

> **Follow-up (not required for this change):** the Performance Agent's model still lists
> `resource_estimate_s` / `baseline_s` among its features. Now that the Resource Agent owns
> *settings* rather than *duration*, a future retrain should drop those two columns and feed
> the Performance model the settings directly (workers, DIU, memory, node) so the two models
> share no target. Tracked here so the boundary stays clean.

---

## Manager Phase 2 ordering (unchanged, annotated)

```
Phase 2 (pre_checks):
  analyze_parallelism()   # Manager owns the execution-group graph
  predict_resources()     # Resource Agent: settings + feasibility  (HARD GATE: feasible)
  estimate_cost()         # Manager: $ estimate from settings
  predict_performance()   # Performance Agent: runtime/outcome/SLA  (HARD GATE: outcome!=failure)
```

The order matters: settings must exist before cost and performance can be derived from them.
Neither cost nor performance recomputes settings — they read `state.resource_plan`.

---

## Where each lives

| Agent | Path |
|---|---|
| Planner | `planner_agent/` |
| Assurance | `assurance_agent/` |
| Central Manager | `central_manager_agent/` |
| Resource | `resource_agent/` (model: `models/`, training: `training/`, contract: `ml/`) |
| Performance Prediction | `performance_prediction_agent/` |
| Monitor | `monitor_agent/` |
| Executor | `executor_agent/` |
