# Session changelog — scan findings fix-up (2026-09-29 → 2026-09-30)

Everything changed in this session, why it was changed, how it was verified,
and the mistakes and dead ends along the way. Finding numbers (`#1`, `#22`, …)
refer to [PROJECT_SCAN_FINDINGS.md](PROJECT_SCAN_FINDINGS.md).

Ground rules for the session: the assistant made file changes only. It ran
no git commands that change state (no add/commit/checkout). The user wrote
every commit, using the suggested one-line messages.

## Commits at a glance

| Commit | Scope |
|---|---|
| `e997e85` | Security: notebook-filter injection, storage key via secret scope, auth, upload cap, `.env` support |
| `136ab6d` | Ollama auto-starts with the backend |
| `2ebecd1` | scikit-learn pin + retrained Resource and Performance models |
| `d4f4f93` | Run-history correctness (#7, #8, #9, #12, #13, #17, #18) |
| `3d035c0` | Anomaly detector: per-pipeline baselines, cold start, events UI (#10, #11, #23, #25) |
| `8720d44` | Reliability: SQLite WAL, safe WebSocket broadcast, shutdown, stream tasks (#16, #19, #22, #30) |
| `ae954ec` | *(teammate)* Cost Optimization model shipped — resolved #14 |
| `6f8904c` | Monitor predictions via Performance model, frontend errors, log rotation, lint, docs (#20, #27–#29, #31, #33, #35) |
| `60d59b0` | Folder reorganisation of `unified/` |

Not done / still on the user: **rotate the four leaked credentials** (Azure client
secret, storage key, Databricks PAT, Groq key). See [Outstanding](#outstanding).

---

## 1. Security — `e997e85`

| # | Change | Why |
|---|---|---|
| 1 | `executor_agent/notebook_builder.py`: new `_pystr()` escapes `\` and `"` in every captured filter value; `_convert_filter` became a wrapper that runs the validator on **every** return path; `_invalid_pyspark_reason` now also AST-checks the expression (bare names must be known PySpark helpers, no `_private`/dunder attributes, no lambdas). | Filter text from the planner LLM was written unescaped into notebook source that runs on Databricks with the storage key in scope. Reproduced: `name like '%a"); import os; os.system("id")#%'` became executable code. |
| 3 | Notebooks read the storage key with `dbutils.secrets.get(scope, key)`. The executor creates the scope `unified-orchestrator` on first use and stores the key there (`dbx_ensure_storage_secret`, runs once per process). It falls back to `initial_manage_principal: users` for standard-tier workspaces. | Job parameters are visible in Databricks run history and the Jobs UI. |
| 4 | `APIKeyMiddleware` is always installed. With no `API_KEY`, only loopback clients (127.0.0.1 / ::1) are served. `/ws/live` checks the same policy (key via `?api_key=`). Download validates container names. Rollback rejects `../` version ids and manifest paths outside `unified/`. | Retrain, rollback (`shutil.rmtree`) and download were open to anyone who could reach the port. |
| 5 | `hmac.compare_digest` for the API key. | Plain `!=` leaks timing. |
| 32 | Upload size checked **before** appending each chunk. | Previously up to 1 MB over the cap was buffered. |
| 6 / 21 | New `settings.py` (env → `.env` → legacy `config.py` → default). Executor, Groq planner and download endpoint use it instead of `from config import …`. | Storage/Databricks keys in `.env` were silently ignored; `config.py` was effectively mandatory. |
| 26 | Frontend download = fetch + blob with `x-api-key`; WebSocket sends the key as a query param. | A plain `<a href>` can't send headers, so downloads 401'd once auth was on. |

Also: the assistant generated `unified/.env` from `config.py` values (never
printing them) and made it `chmod 600`. It checked that `.env` is gitignored.

**Verified:** 3 injection payloads became inert string literals. All normal filters
and transforms still converted. Auth tests: 403 remote / 401 wrong key / 200
right key; WebSocket rejected without the key; 413 over the upload cap; rollback
traversal rejected. The first real run after the change logged "Storage key stored
in secret scope 'unified-orchestrator'".

## 2. Ollama auto-start — `136ab6d`

`planner_agent/ollama_launcher.py`, spawned from `main.py`'s lifespan:
- If `PLANNER_BACKEND=ollama` and the host is local, it starts `ollama serve` (log: `data/ollama.log`).
- It waits up to 30 s, checks that `planner-agent` exists, and preloads it.
- It runs in its own session, so it survives `--reload` and Ctrl-C.
- Opt out with `OLLAMA_AUTOSTART=0`.

The Ollama planner's `_cfg` also switched to env-first (it used to prefer `config.py`).

**Why:** the planner silently fell back to a generic plan whenever Ollama wasn't running.

**Keep-alive:** the user asked whether the model unloads. It does: 30 min after
the startup preload, then Ollama's 5-min default after each plan request. The user
chose to keep the default.

## 3. ML models — `2ebecd1`

- **Problem found in a live run log (not in the scan):** `X has 19 features, but
  GradientBoostingRegressor is expecting 15`. The saved Performance Prediction
  models came from an older training run, while the code and training script use
  19 features. Every prediction silently used the formula fallback.
- `requirements.txt` → `scikit-learn==1.6.1` (#15). 1.7.0 can't install on the
  project's Python 3.9. Also updated the Kaggle notebook, the training README and
  `train_resource_model.py`.
- Retrained Performance models under 1.6.1: MAE 224.0 s, R² 0.871, balanced
  accuracy 0.713. 26 real runs were blended in. Metrics are about the same as before.
- Retrained the Resource model under 1.6.1: metrics matched the old ones to 2–3 decimals.
- `ml_predictor.py`: passes named columns (a DataFrame) and refuses a bundle whose
  feature count doesn't match, with a "retrain" message.

## 4. Run-history correctness — `d4f4f93`

| # | Change |
|---|---|
| 7 | Run Insights finds the monitor record via the executor's `dbx_run_id` / `run_id` / `adf_run_id`, not the manager's uuid. |
| 8 | `record_feedback(..., final_status="completed")`; the phase ends at `completed`. (The status isn't flipped earlier because the UI stops polling on `completed`.) |
| 9 | `state.executor_result` is set right after every executor attempt, including failures. |
| 12 | The executor returns both `adf_run_id` and `dbx_run_id`; the monitor always records the Databricks run under its own id. |
| 13 | `rows_written` comes from the last executed group, preferring the stage that writes the final sink. |
| 17 | Assurance counts `stages_completed` (what actually ran), not the plan's stage list. |
| 18 | Every plan stage lands in exactly one parallel group; cycles log a warning. |

The anomaly detector also switched to the Databricks id so its events match the run record.

**Verified:** a mocked executor run gave `rows_written=42` (the old code gave 7,
the sibling branch). A failed run listed only the stages that finished. The Run
Insights lookup found data for a real old run.

## 5. Anomaly detector — `3d035c0`

- **#10 — baselines per pipeline:** `pipeline_key(plan)`.
  - It's the engine name plus a hash of each stage's type, source, sink,
    transforms, filter and aggregations. A plan can instead set `pipeline_name`,
    which gives a readable key.
  - The history for slow-runtime, cold-start and cost-spike checks now comes from
    the detector's own `run_metrics` table, filtered to that pipeline.
  - **Why:** there were only two pipeline names in total, so `schema_drift` fired
    on almost every run and p95 mixed unrelated pipelines.
- **#11 — cold start:** the idle gap is measured from the same pipeline's previous
  run (failed runs included), and timestamps are parsed in any common format. The
  silent `except: pass` became a logged message.
- **#23:** indexes on `anomaly_events(kind)` and `run_metrics(pipeline_name, status, created_at)`; events are sorted by `id`.
- **#25:** `getAnomalyEvents` added to `api.js`. The Anomalies page shows events
  with severity, details, metrics and filter chips for the 9 kinds, and the old AI
  verdicts below them.
- **Side effect:** slow-runtime and cost-spike baselines restart from zero (each
  pipeline needs 3 runs), because old rows used the shared names.

## 6. Reliability — `8720d44`

| # | Change | Note |
|---|---|---|
| 22 | `PRAGMA journal_mode=WAL` (stored in the DB file) and a 10 s busy timeout via one `_connect()` helper at all 21 sites. | **Deviation from the scan:** kept one connection per call instead of a single shared one. A shared aiosqlite connection is tied to one event loop and breaks scripts and tests; WAL plus the timeout is what fixes the lock errors. Test: 300 concurrent write+read tasks, 0 errors. |
| 16 | Broadcast iterates a snapshot of the client set, with a 5 s send timeout per client. | A stuck client is dropped instead of blocking the 20 s poll. |
| 30 | Lifespan shutdown cancels the poll, backfill, Ollama-check and stream tasks and closes WebSockets. | Ollama is deliberately left running. |
| 19 | The stream poll loop uses `spawn()`; stopped streams are pruned after 1 h. | |

## 7. Monitor, frontend, logs, lint, docs — `6f8904c`

- **Groq model:** the key could no longer use `llama-3.3-70b-versatile`. Listed the
  models available to the key; the user asked for "any free model", so
  `GROQ_MODEL=openai/gpt-oss-120b` was set. The Groq planner's hardcoded model
  now reads `GROQ_MODEL` too.
- **Monitor runtime prediction** (the user chose this; not in the scan):
  - New `monitor_agent/services/runtime_predictor.py` finds the latest orchestrator
    plan for the pipeline and runs the Performance Prediction Agent on it. For the
    ADF copy pipeline it uses only the copy-stage forecasts.
  - If no plan is found, it falls back to history statistics. The Groq
    `predict_runtime` was removed.
- **#27:** added error handling on the Predictions and Logs pages, the header Sync
  button and the Home page sync link. (Several call sites listed in the scan
  already had `.catch` by then.)
- **#28:** removed the dead `/executor/jobs` and `/executor/status` endpoints and their `api.js` wrappers.
- **#29:** the `AppContext` value is memoized and its setters are created once.
- **#31:** new `jsonl_log.py`:
  - The feedback logs are cached and re-read only when their mtime or size changes.
  - They rotate at 20 MB and keep 5 archives. Readers include the archives, so
    the learning agents never lose history.
  - All 8 read/write sites were converted.
- **#20:** the stream sink preview uses `csv.reader`.
- **#33:**
  - Removed 36 unused imports and 4 dead variables, and fixed 2 f-strings with no placeholders.
  - Added `from exc` to 17 raises.
  - Renamed the Pydantic `schema` fields with aliases (the API still takes `schema`).
  - B008 left alone: it flags FastAPI's normal `File()` default.
- **#35:** `unified/README.md` became the backend guide; the generator docs moved out.
- **Found along the way:** `build_finetune_notebook.py` pointed at the old dataset,
  and its `--verify` check had always crashed (it used `json.load` on a JSONL file).
  Both fixed.

## 8. Folder reorganisation — `60d59b0`

- Top level of `unified/` went from 44 entries to 22.
- New homes: `docs/` (notes), `scripts/` (integration test, seed data, sample
  input), `planner_agent/training/` (dataset tools, notebook, README, `datasets/`).
- Deleted `config.py.bak` (after checking all 18 values were identical in `.env`),
  `config.example.py` and `.ruff_cache/`.
- **Path fixes:**
  - Dataset tools resolve `datasets/` relative to their own location.
  - The scripts point at the project root.
  - `.gitignore` entries were moved to the new paths so local-only notes stay ignored.
- **Bug found:** the monitor read only `ADF_FACTORY_NAME`, which only the old
  `config.py` bridge set. It now falls back to `AZURE_DATA_FACTORY` via `settings`.
- `SETUP_GUIDE.md` sections 9–10 were rewritten for `.env`; all 5 checks were run and pass.
- Chosen over also creating a `core/` package, which would have touched 23 imports
  in 17 files while a teammate was committing to the same agents.

---

## Mistakes and dead ends (honest list)

| What happened | Impact | Resolution |
|---|---|---|
| A multi-file patch script asserted partway through: `executor.py` and `groq_planner.py` were patched, but `main.py` wasn't (the first anchor was wrong). | Briefly left `main.py` inconsistent. | Re-read the block and patched it separately. |
| Mocked `get_access_token`, but the executor calls `get_azure_token`. | One **real** Azure access-token request during a "mocked" test. Nothing was created or changed. | Disclosed at the time. |
| A manager unit test called the real `record_feedback`. | Wrote a test row to `data/manager_feedback.jsonl` and bumped `learning_policy_agent/data/cycle_state.json` (2 → 3). | Restored the log from a backup and reset the counter to 2. |
| First cold-start test: the earlier 400 s run had raised p95, so 400 s wasn't "slow". | False "not detected". | Test design error, not a code bug; reran with a clearly slow run. |
| Renamed the form field to `schema_json`. | Still shadowed `BaseModel.schema_json`. | Renamed to `input_schema`. |
| Removed `import os` from `adf_service.py` when switching it to `settings`, though later code still used `os`. | Would have raised NameError on the first token call. The import-only test didn't catch it. | Caught when re-reading the file; restored before commit. |
| Assumed `priority = …` in `cost_optimizer.py` appeared once. | The edit assertion failed (3 occurrences). | Removed only the unused one, by line number. |
| Changed the notebook builder's dataset path and assumed `--verify` would pass. | It crashed. | Found `--verify` was broken before this session too (`json.load` on JSONL); fixed it. |
| The model-status table listed the Cost Optimization model as "❌ Missing" based on the scan doc, without re-checking. | Reported stale information; a teammate had shipped the model hours earlier (`ae954ec`). | Re-checked when asked, then verified it loads and predicts. |
| Tooling slips: `python3 -m ruff` via the venv (not installed there); a zsh `=====` separator; `head -0`; unmatched `--include=*.py` globs; the `ruff --exclude` argument quoted wrongly twice; `importlib` couldn't load a `.bak` file; `compile()` on a notebook with `%pip`. | Wasted tool calls only. | Worked around each one. |
| `[ollama]` status lines never showed in the captured log (stdout buffering). | Couldn't confirm startup from logs. | Confirmed via Ollama's `/api/ps` instead. |
| The WebSocket test client printed nothing during the shutdown test. | Client-side confirmation missing. | Server log showed `connection open` → `connection closed`. |
| Asked some questions several times with the question tool while the user still wanted to clarify. | Extra back-and-forth. | Switched to plain-text explanations, then asked again. |

**Unverified at the time of each change:**
- The frontend was only build-checked, never clicked through in a browser.
- The secret-scope change was confirmed from one real run's log, not by inspecting the Databricks UI.

## Deviations from the scan document

- **#22:** WAL + busy timeout instead of a single held connection (reason above).
- **#33 B008:** not changed (FastAPI idiom). B905 skipped as the scan advised (`zip(strict=)` doesn't exist on Python 3.9).
- **#34 `data/zv.csv`:** kept as the 101-row zoo dataset at the user's request. It
  had gone into commit `e997e85` together with the security fixes.
- **#24:** S608 flags confirmed as false positives; no action.

## Outstanding

1. **Rotate** the Azure client secret, storage key, Databricks PAT and Groq key.
   These sat in plaintext `config.py`. Put the new values in `unified/.env` and
   restart the backend so the new storage key is pushed into the Databricks
   secret scope.
2. Before exposing the app beyond localhost: set `API_KEY` in `.env` and the same value as `VITE_API_KEY` for the frontend.
3. Known weak spot: the Resource model's node-type classifier (balanced accuracy 0.46).
4. Optional later: move the 4 shared helpers into a `core/` package once no one has open work on the agents.
