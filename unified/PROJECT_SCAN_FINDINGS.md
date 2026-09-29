# `unified/` — Full Project Scan Findings

**Scanned:** 2026-09-29 · **Scope:** everything under `unified/` (excl. `venv/`, `planner_agent/model/llama.cpp/`, `frontend/node_modules|dist`)
**Baseline commit:** `ff9c2df` + uncommitted working tree (`central_manager_agent/router.py`, `executor_agent/executor.py`, `monitor_agent/routers/anomalies.py`, `monitor_agent/services/db_service.py`, new `monitor_agent/services/anomaly_detector.py`)

**How this was produced** — so another session can re-verify rather than trust:

| Check | Command |
|---|---|
| Syntax | `ast.parse` over all non-vendor `.py` → 0 errors |
| Lint | `python3 -m ruff check --select F,E9,B,S102,S105,S106,S107,S301,S307,S324,S501,S506,S602,S605,S608 --exclude venv,llama.cpp,__pycache__,node_modules .` → 80 findings |
| Imports | every module imported under `venv/bin/python` (3.9.6) → 0 failures |
| Frontend | `npx vite build` → clean, 2324 modules |
| Runtime | `uvicorn main:app --port 8899`, smoke-tested all 22 GET endpoints → all 200 |
| Secrets | `git log --all -- config.py`, `git grep` for `dapi*`/`gsk_*`/`AccountKey=` |

Nothing in the codebase was modified by the scan.

**Environment facts discovered (matter for several findings):**
- Project venv is **Python 3.9.6**, sklearn **1.6.1**, numpy 2.0.2, pandas 2.3.3, fastapi 0.111.0, pydantic 2.13.4
- `unified/.env` exists but is **empty** — all secrets currently come from `config.py`
- `data/adf_monitor.db` has 65 pipeline runs, 12 manager runs, 27 feedback records — enough history that the live evidence below is real, not synthetic

---

## Severity index

| # | Severity | Area | One-liner |
|---|---|---|---|
| 1 | Critical | notebook_builder | Code injection into generated Databricks notebooks |
| 2 | Critical | config | Live secrets in plaintext `config.py` |
| 3 | Critical | executor | Storage key passed as Databricks job widget param |
| 4 | Critical | auth | Destructive endpoints unauthenticated by default |
| 5 | High | auth | Timing-unsafe API key compare |
| 6 | High | config | `.env` silently ignored for storage/Databricks keys |
| 7 | High | combined | `monitor_analysis` always `None` |
| 8 | High | manager | Every success logs `final_status: "feedback"` |
| 9 | High | manager | `executor_result` set too late, never on failure |
| 10 | High | anomaly_detector | Baselines pooled across unrelated pipelines |
| 11 | Medium | anomaly_detector | Cold-start gap reads the wrong row |
| 12 | High | executor router | Databricks stages unrecorded when plan has a copy stage |
| 13 | Medium | executor | `rows_written` picks the wrong stage |
| 14 | High | cost agent | `cost_models.pkl` does not exist |
| 15 | High | ML | sklearn version mismatch; `requirements.txt` uninstallable |
| 16 | Medium | monitor | WebSocket broadcast can crash the poll loop |
| 17 | Medium | manager | `all_stages_completed` always true |
| 18 | Medium | manager | Stages missing from `execution_order` silently dropped |
| 19 | Medium | stream_manager | Poll loop bypasses `background.spawn`; state leak |
| 20 | Low | stream_manager | Naive CSV parse in sink preview |
| 21 | Medium | main | `download_output` hard-requires `config.py` |
| 22 | High | db_service | Connection-per-call, no WAL, no busy timeout |
| 23 | Low | db_service | No indexes on new tables; nondeterministic sort |
| 24 | — | db_service | Ruff S608 ×3 are false positives — no action |
| 25 | High | frontend | New anomaly-events feature unreachable from UI |
| 26 | Medium | frontend | Download breaks whenever auth is on |
| 27 | Medium | frontend | 7 unhandled promise rejections |
| 28 | Low | frontend | ExecutorTab polls permanently-dead endpoints |
| 29 | Low | frontend | `AppContext` value rebuilt every render |
| 30 | Medium | lifecycle | No shutdown handling |
| 31 | Medium | perf | Feedback logs never rotate, re-read constantly |
| 32 | Low | security | `read_upload_capped` overshoots cap by 1 MB |
| 33 | — | lint | 80 ruff findings, 4 real dead assignments |
| 34 | Low | repo | `data/zv.csv` truncated in working tree |
| 35 | Low | docs | `unified/README.md` documents the dataset generator, not the backend |

---

## Critical

### 1. Code injection into generated Databricks notebooks
`executor_agent/notebook_builder.py:186` (`_convert_filter`)

The `_invalid_pyspark_reason` guard runs **only** on the final fallthrough (line 263). Every early-return branch — BETWEEN (line 202), IN (line 208), LIKE (line 218), and all 18 regex `patterns` (lines 232-258) — returns unguarded, and none escape quotes in captured values.

Reproduced live:

```python
from executor_agent.notebook_builder import _convert_filter
_convert_filter("""name like '%a"); import os; os.system("id")#%'""")
# -> col("name").contains("a"); import os; os.system("id")#")

_convert_filter("""region in ('EU"), lit(1)) | col("x").isNull(), ("')""")
# -> col("region").isin("EU")", "lit(1)) | col("x").isNull()", "(")
```

That string is written into the notebook source uploaded to Databricks and executed with `storage_key` in scope. `filter_condition` originates from planner LLM output, which is steered by the user prompt.

**Fix:** escape `"` and `\` in every captured literal, and run `_invalid_pyspark_reason(result)` on **all** return paths of `_convert_filter`, not just the fallthrough. The transform path (`notebook_builder.py:385-388`, `570-571`) is already guarded correctly — mirror it.

### 2. Live secrets in plaintext — `unified/config.py`
Azure client secret, storage account key, Databricks PAT, Groq key, all literal string values.

**Git history is clean** — `git log --all -- config.py` shows only `3c13d34`, whose version used `os.environ.get(...)`. The current file is untracked and gitignored. But the four credentials have sat unencrypted in the working tree and are read at import.

**Fix:** rotate all four, move to `.env`, keep `config.example.py` as the template. See #6 — moving them to `.env` does not currently work.

### 3. Storage account key as a Databricks job parameter
`executor_agent/executor.py:786`

```python
parameters = {
    "storage_key": AZURE_STORAGE_KEY,
    "run_id":      f"{run_tag}-{stage['name']}",
    "stage_name":  stage["name"],
}
```

Widget params are visible in Databricks run history and the jobs UI. **Fix:** Databricks secret scope, referenced by name from the generated notebook.

### 4. Destructive endpoints unauthenticated by default
`API_KEY` is unset (`.env` is empty), so `APIKeyMiddleware` is never added (`main.py:104-113`). With auth off:

- `POST /api/learning/retrain` — spawns `run_training.py`, overwrites `performance_prediction_agent/models/*.pkl`
- `POST /api/learning/rollback` — `safety.rollback()` does `shutil.rmtree(dst)` on `original_path` read from a JSON manifest on disk (`learning_policy_agent/safety.py:81`)
- `GET /api/executor/download/{container}` — reads any container when `DOWNLOAD_CONTAINER_ALLOWLIST` is empty
- `/ws/live` bypasses the middleware **by design** (documented in `app_security.py:57`), leaking run metadata

**Fix:** require `API_KEY` in non-dev; at minimum gate the learning write endpoints unconditionally.

---

## High

### 5. Timing-unsafe API key compare
`app_security.py:69` — `request.headers.get("x-api-key") != self._api_key`. Use `hmac.compare_digest`.

### 6. `.env` silently ignored for storage and Databricks keys
`main.py:42` `_BRIDGE` bridges tenant/client/subscription/RG/factory/Groq/planner keys but **omits** `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`, `DATABRICKS_SPARK_VERSION`, `DATABRICKS_NODE_TYPE`, `DATABRICKS_NOTEBOOK_BASE`.

Meanwhile these read `config.py` directly:
- `executor_agent/executor.py:24` — `from config import (...)` at **module level**
- `executor_agent/executor.py:203` — `from config import DATABRICKS_CLUSTER_ID`
- `planner_agent/groq_planner.py:13` — `from config import GROQ_API_KEY`
- `main.py:362` — `import config as _cfg` inside `download_output`

And these read env only: `monitor_agent/services/adf_service.py:8-12`.

Import chain `main → central_manager_agent.router → stream_manager → executor_agent.executor` makes `config.py` **mandatory**, contradicting `main.py:39` (`except ImportError: pass`) and `.env.example`'s claim that `.env` values "take precedence over the legacy plaintext config.py". Setting `AZURE_STORAGE_KEY` in `.env` alone does nothing.

**Fix:** one `settings.py` resolving env → `config.py` → default; drop all module-level `from config import`.

### 7. `monitor_analysis` is always `None`
`central_manager_agent/combined.py:69`

```python
rows = await db.get_pipeline_runs(run_id=run_id, limit=1)
```

`run_id` is the manager's `uuid4`. `pipeline_runs.run_id` holds the ADF/dbx run id. Verified against the live DB:

```
manager run:            dbc3a8d3-6a38-4f4e-b56c-9a9b82901892
executor_result.run_id: 90e8f66e-45b9-47bf-8896-6f00cc177cbe   <- present in pipeline_runs
monitor_analysis:       None
```

The entire Monitor cross-reference in Run Insights is dead. **Fix:** look up by `state["executor_result"]["run_id"]`.

### 8. Every successful run logs `final_status: "feedback"`
`central_manager_agent/manager.py:1050` writes `"final_status": state.status`; `manager.py:1268` sets `state.status = "completed"` **after** `record_feedback()` at line 1265.

Live count in `data/manager_feedback.jsonl`: **19 × `"feedback"`, 8 × `"failed"`, 0 × `"completed"`**.

Leaks through `/api/performance-prediction/history` and `/api/manager/feedback` to the UI. `learning_policy_agent` documents and compensates for it (`learning_policy_agent/README.md` §Known Issues #2, `feedback_collector.py:126`) — which is why nothing has broken yet, but it is a landmine for any new consumer.

Related: `phase` never advances past `"feedback"`. `/api/manager/runs` returns `status:"completed", phase:"feedback"`.

**Fix:** set `state.status = "completed"` (and `_enter(state, "completed", ...)`) before calling `record_feedback`. Then simplify `feedback_collector.normalize()`'s workaround.

### 9. `state.executor_result` set too late and never on failure
`central_manager_agent/manager.py:1265` assigns it after `run_assurance` and `record_feedback`, and on no failure path.

So `detect_and_store(state, result=None, ...)` runs for every failed run, forcing `_pipeline_name(None)` → `"Databricks_Notebook_Pipeline"` even for streaming failures. **Fix:** assign immediately after `execute_with_retry` returns, and in the `except` block too.

### 10. Anomaly baselines pooled across unrelated pipelines
`monitor_agent/services/anomaly_detector.py:60` (`_pipeline_name`) returns one of exactly two constants.

- `get_last_schema()` keys on that constant, so **`schema_drift` fires on nearly every run** with a different input file — false positive by construction
- `slow_runtime`, `cost_spike`, `cold_start` compare against a p95/average mixing every batch pipeline ever run

**Fix:** derive a real per-pipeline identity (plan hash, container-set key, or a user-supplied pipeline name carried on the plan) and key `pipeline_schemas`, `run_metrics`, and `get_historical_stats` on it.

### 12. Databricks stages unrecorded when the plan has a copy stage
`executor_agent/router.py:60` — `if db and run_id.startswith("dbx-")`.

`execute_pipeline` returns `adf_run_id or f"dbx-{run_tag}"` (`executor.py:828`). With any copy stage, `adf_run_id` wins, so no `Databricks_Notebook_Pipeline` record is ever written. That starves every history-based check in #10 and #11.

**Fix:** always upsert the Databricks record under its own synthetic id, independent of whether ADF also ran.

### 14. Cost ML model does not exist
`cost_optimization_agent/ml_predictor.py:29` points at `models/cost_models.pkl`. `ls cost_optimization_agent/models/` → only `cost_metrics.json`.

`CostMLPredictor.is_available()` is permanently false → silent heuristic fallback, while the shipped `cost_metrics.json` implies a trained model exists and is served through the UI.

**Fix:** train and commit the bundle, or delete `cost_metrics.json` and remove the ML path.

### 15. sklearn version mismatch; `requirements.txt` uninstallable
`requirements.txt:38` pins `scikit-learn==1.7.0`. The project venv is **Python 3.9 with sklearn 1.6.1** — 1.7.0 does not support 3.9, so the pin cannot be installed there.

Every model load emits `InconsistentVersionWarning` (bundles pickled at 1.7.0). `/api/resource/model-info` confirms: `"sklearn_version": "1.7.0"` while loading under 1.6.1. Affects `resource_agent/models/resource_models.pkl` and all three `performance_prediction_agent/models/*.pkl`.

Predictions are running on a version combination sklearn explicitly flags as possibly invalid.

**Fix:** either move the project to Python 3.11+ and keep the 1.7.0 pin, or pin `scikit-learn==1.6.1` and retrain. The pin and the runtime must match — `requirements.txt:33-37` already documents why.

### 22. Connection-per-call sqlite, no WAL, no busy timeout
`monitor_agent/services/db_service.py` — every method opens a fresh `aiosqlite.connect(DB_PATH)`.

With the 20s monitor poll, `_persist` on every manager phase, and `log_anomaly_event`/`save_run_metrics` writes running concurrently, `database is locked` is a matter of load.

**Fix:** `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, and a single held connection.

### 25. New anomaly-events feature unreachable from the UI
`GET /api/monitor/anomalies/events` (added in the working diff, `monitor_agent/routers/anomalies.py:12`) has no function in `frontend/src/api.js` and no UI. `frontend/src/pages/AnomaliesPage.jsx:22` still calls the legacy `getAnomalies()` → `/monitor/anomalies/`.

The detector writes rows nobody can see — confirmed, the endpoint returns real classified events.

**Fix:** add `getAnomalyEvents` to `api.js` and render kind/severity/metrics in `AnomaliesPage`.

---

## Medium

### 11. Cold-start gap reads the wrong row
`monitor_agent/services/anomaly_detector.py:139` assumes `get_pipeline_runs(limit=2)[0]` is the current run. Only true when `_notify_monitor` already inserted it, which requires `run_id.startswith("dbx-")` (see #12). Otherwise `prior[1]` is two runs back and `gap_s` is wrong.

Also `prev_end` parses with strict `"%Y-%m-%dT%H:%M:%SZ"`; any other stored format raises into a bare `except: pass`.

### 13. `rows_written` picks the wrong stage
`executor_agent/executor.py:836`

```python
for s in compute_stages:
    if s["name"] in stage_rows:
        rows_written = stage_rows[s["name"]]
```

Takes the last match in `compute_stages` **declaration** order, not execution order. For a fan-out plan (several stages in the final `execution_group`) this reports a sibling branch's count, not the sink's — which then feeds `zero_rows` detection.

**Fix:** resolve from the last executed group.

### 16. WebSocket broadcast can crash the poll loop
`monitor_agent/services/monitor_service.py:32`

```python
for ws in self.ws_clients:
    await ws.send_text(json.dumps(payload))
```

`websocket_live` (`main.py:449`) adds to that same set. A client connecting during the await → `RuntimeError: Set changed size during iteration`. **Fix:** iterate `list(self.ws_clients)`.

### 17. `all_stages_completed` always true
`central_manager_agent/manager.py:958`. `result["stages"]` is `[s["name"] for s in stages]` (`executor.py:827`), built unconditionally from the plan, not from what actually ran. `stages_ran >= stages_expected` can never fail. The assurance check is decorative.

### 18. Stages missing from `execution_order` silently dropped
`central_manager_agent/manager.py:604` — `remaining = list(plan.get("execution_order", [...]))`. A stage present in `stages` but absent from `execution_order` never lands in a group. The executor recovers this (`executor.py:776-778`); the manager's parallelism, cost, and resource math does not.

Related: the cycle-break at `manager.py:609` (`group = [remaining[0]]`) proceeds silently on a cyclic plan.

### 19. Stream poll loop bypasses `background.spawn`; state leak
`central_manager_agent/stream_manager.py:72` — `asyncio.ensure_future(self._poll_loop(sid))` with no strong reference and no done-callback, which is exactly the failure mode `background.py` was written to prevent.

`stop()` (line 122) cancels the task but leaves the `_streams` and `_locks` entries forever.

### 21. `download_output` hard-requires `config.py`
`main.py:362-374` — `import config as _cfg` / `_cfg.AZURE_STORAGE_ACCOUNT` inside the handler → 500 when the rest of `main.py` was written to tolerate its absence. Subsumed by the #6 fix.

### 26. Download breaks whenever auth is on
`frontend/src/api.js:67` — `downloadUrl` returns a bare URL used as an `<a href>`; an anchor cannot carry `x-api-key`. Set `API_KEY` → every download 401s.

**Fix:** fetch + blob download through `req()`, or a signed short-lived token in the URL.

### 27. Seven unhandled promise rejections
`.then()` with no `.catch()` — backend errors render as empty state with no message:

- `frontend/src/pages/AnomaliesPage.jsx:22`
- `frontend/src/pages/RunInsights.jsx:143`
- `frontend/src/pages/PredictionsPage.jsx:46`
- `frontend/src/pages/ManagerTab.jsx:605`
- `frontend/src/pages/LiveDashboard.jsx:57`
- `frontend/src/pages/HomePage.jsx:158`
- `frontend/src/App.jsx` `handleSync`

### 30. No shutdown handling
`main.py:85` — `lifespan` has nothing after `yield`. `monitor_service.start_polling()` is a bare `while True` (`monitor_service.py:40`); on shutdown it keeps polling into a closing loop, and WebSocket clients are never closed.

**Fix:** keep the `spawn()` handles and cancel them after `yield`.

### 31. Feedback logs never rotate and are re-read constantly
- `resource_agent/resource_agent.py:856` — `analyze()` reads `resource_feedback.jsonl` four times per call (`corr_copy`, `corr_notebook`, `copy_n`, `notebook_n`)
- `central_manager_agent/combined.py:24` — `_feedback_records()` re-reads `manager_feedback.jsonl` on every `/analytics` and `/run/{id}` request

Both files grow unbounded. **Fix:** cache with an mtime check; add rotation.

---

## Low

### 20. Naive CSV parse in sink preview
`central_manager_agent/stream_manager.py:203,206` — `l.split(",")`. Quoted fields containing commas produce misaligned rows. Use `csv.reader`.

### 23. No indexes on new tables; nondeterministic sort
`db_service.py:77-107` creates `anomaly_events`, `run_metrics`, `pipeline_schemas` with no indexes. `get_anomaly_events` (line 380) sorts by `detected_at`, which is `datetime('now')` at **second** resolution — ties order nondeterministically. Sort by `id DESC`; index `anomaly_events(kind)` and `run_metrics(pipeline_name, status)`.

### 24. Ruff S608 ×3 are false positives
`db_service.py:186,317,380` — all three interpolate only internally-built placeholder strings; params are bound. **No action.**

### 28. ExecutorTab polls permanently-dead endpoints
`executor_agent/router.py:14` — `_jobs` is never written to (the comment says so). `/executor/jobs` always `[]`, `/executor/status/{id}` always 410. Remove both, or remove the UI that calls them (`api.js:66-67`, `ExecutorTab.jsx`).

### 29. `AppContext` value rebuilt every render
`frontend/src/AppContext.jsx:57` — the provider value object is a fresh literal each render, so every consumer re-renders on any state change. Wrap in `useMemo`.

### 32. `read_upload_capped` overshoots the cap
`app_security.py:47` — size is checked **after** `buf.extend(chunk)`, so up to `max_bytes + 1MB` is buffered. Check before extending.

### 34. `data/zv.csv` truncated in the working tree
Repo-root `data/zv.csv` (tracked, predates the `data/` ignore rule): `git diff --stat` shows **1 insertion, 1011 deletions** — file is now header + one row. Looks accidental. `git checkout data/zv.csv` unless intentional.

### 35. `unified/README.md` documents the dataset generator, not the backend
For a project whose entire code lives in `unified/`, that folder's README covers only `generate_dataset.py` / `validate_dataset.py` / `report.py` and says nothing about running the app. (The **repo-root** `README.md` is the real project README.)

It also records a live drift: `build_finetune_notebook.py` points at `synthetic_planner_dataset.jsonl` while the generator writes `planner_config_dataset.jsonl`.

---

## Lint — 80 ruff findings

**Real dead assignments** (each may be a dropped feature, not a stray):

| Location | Variable |
|---|---|
| `central_manager_agent/manager.py:376` | `stages` — computed, never used in `estimate_cost` |
| `central_manager_agent/combined.py:209` | `total_predictions` — computed, never returned |
| `cost_optimization_agent/cost_optimizer.py:398` | `total_saving` — accumulated, never read |
| `cost_optimization_agent/ml/feature_spec.py:191` | `rate` — fetched, never used in the cost loop |

**Mechanical:** 36 unused imports (`asyncio` in four routers, `math`, `dataclasses.field`, `io`, assorted `typing`), 14 `B904` (`raise ... from exc`), 3 `B008` (`File()` / `CycleRequest()` in argument defaults), 8 `B905` (`zip(strict=)` — **not available on 3.9, skip these**).

`python3 -m ruff check --fix ...` clears 36 safely.

**Pydantic warning ×3:** a field named `schema` shadows `BaseModel.schema` — `resource_agent/router.py:23` and the two manager-run form bodies. Rename to `schema_` with `alias="schema"`.

---

## Suggested order

1. **#1** injection guard, **#2** rotate secrets — before anything else
2. **#6** config unification (unblocks deploying without `config.py`; subsumes #21)
3. **#7, #8, #9, #12** — four small fixes that make the run-history data trustworthy
4. **#15** sklearn pin + **#14** cost model — the ML claims currently outrun reality
5. **#10, #11** — the anomaly detector needs a real pipeline identity before the feature is usable
6. **#22** WAL, **#16** broadcast copy, **#30** shutdown
7. **#25, #26, #27** frontend
8. ruff `--fix`, **#34** restore csv

---

## Verified clean — do not re-investigate

- No syntax errors; every module imports under Python 3.9
- `vite build` succeeds
- All 22 GET endpoints return 200 against the live DB
- No secrets in git history (`config.py`'s only committed version used `os.environ.get`)
- `venv/`, `.env`, `__pycache__`, `*.pyc`, `.DS_Store`, `frontend/dist/`, `llama.cpp/` all untracked
- Every `api.js` call maps to a real backend route (checked route-by-route)
- All frontend `setInterval`/`setTimeout` have matching cleanup
- `db_service` S608 SQL-injection flags are false positives
- `assurance_agent` (structural + semantic) — no findings
- `background.py` `spawn()` is correct; the problem is the two call sites that skip it (#19)
