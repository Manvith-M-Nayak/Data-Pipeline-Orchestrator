# Unified Orchestrator — backend + dashboard

Everything that runs lives in this folder: a FastAPI backend (`main.py`) that hosts
every agent, and a React dashboard (`frontend/`). The repo-root
[README](../README.md) explains the architecture; this file is how to run and work on it.

## Run it

```bash
# 1. Configuration — secrets live in .env (gitignored)
cp .env.example .env            # fill in Azure, Databricks, Groq values

# 2. Backend (Python 3.9 venv; scikit-learn is pinned to 1.6.1 to match the models)
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 127.0.0.1 --port 8000

# 3. Dashboard (separate terminal)
cd frontend && npm install && npm run dev      # http://localhost:5173
```

Run **one** server process (no `--workers N`): run state, the executor's
resource locks, live streams and the monitor poll loop live in that process,
and startup marks any unfinished run as failed.

On startup the backend also starts Ollama (`ollama serve`) if it isn't running and
loads the fine-tuned `planner-agent` model — see [Planner model](#models).
Health check: `GET /api/health`. Interactive API docs: `/docs`.

## Configuration

Settings resolve **environment / `.env` → legacy `config.py` → default** (`settings.py`).
`config.py` is optional; `.env.example` lists every key. Notable ones:

| Key | Purpose |
|---|---|
| `API_KEY` | When set, every request needs `x-api-key`. **When empty, only localhost clients are served.** Set the same value as `VITE_API_KEY` for the frontend. Set it whenever the app is behind a reverse proxy. |
| `AZURE_*`, `DATABRICKS_*` | Cloud credentials. The storage key reaches Databricks jobs through the secret scope `DATABRICKS_SECRET_SCOPE`, never as a job parameter. |
| `GROQ_API_KEY`, `GROQ_MODEL` | Monitor AI analysis and the Groq planner fallback (default model `openai/gpt-oss-120b`). |
| `PLANNER_BACKEND`, `OLLAMA_HOST`, `PLANNER_MODEL`, `OLLAMA_AUTOSTART` | Planner LLM. `OLLAMA_AUTOSTART=0` stops the backend from launching Ollama. |
| `DOWNLOAD_CONTAINER_ALLOWLIST`, `MAX_UPLOAD_BYTES`, `ALLOWED_ORIGINS` | Download / upload / CORS limits. |

## Layout

| Path | API prefix | What it does |
|---|---|---|
| `planner_agent/` | `/api/planner` | Turns a prompt + CSV schema into a pipeline plan (fine-tuned Qwen via Ollama; Groq fallback). |
| `assurance_agent/` | `/api/assurance` | Plan-checking **library**: structural rules + intent (LLM) check. The Planner runs both on its own plan before returning it (`planner_agent/self_check.py`, one re-plan on problems); the Manager runs the structural rules as its run-time gate. `/api/assurance/validate` backs the Planner page's "Re-check Plan" button. |
| `resource_agent/` | `/api/resource` | Per-stage compute sizing (ML model, heuristic fallback). |
| `performance_prediction_agent/` | `/api/performance-prediction` | Run duration / outcome forecast (ML model, formula fallback). |
| `cost_optimization_agent/` | `/api/cost-optimization` | Cost estimate + cheaper-config recommendations (ML model). |
| `central_manager_agent/` | `/api/manager` | Runs a plan end to end: validate → structural gate → pre-checks → execute (with retries) → post-assurance → feedback. Also streaming runs and the combined Run Insights API (`/api/manager/combined`). |
| `executor_agent/` | `/api/executor` | Builds Databricks notebooks, deploys ADF copy pipelines, runs jobs, downloads output. Runs only start via the manager. |
| `monitor_agent/` | `/api/monitor/*`, `/ws/live` | Polls ADF, stores run history (SQLite), AI analysis, runtime predictions, anomaly events. |
| `learning_policy_agent/` | `/api/learning` | Learns correction factors from feedback; retrains / rolls back models. |
| `frontend/` | — | React + Vite dashboard (dev proxy → `127.0.0.1:8000`). |
| `settings.py`, `app_security.py`, `background.py`, `jsonl_log.py` | — | Config resolution, auth + upload limits, safe background tasks, rotating feedback logs. |
| `planner_agent/training/` | — | Planner fine-tuning dataset generator, validator, notebook builder, live eval (see its README). |
| `scripts/` | — | `integration_test.py` (local pre-execution flow test), `seed_anomalies.py` (demo data), `test_input.json` (sample request). |
| `docs/` | — | Design notes: agent responsibilities, scan findings, agent deep-dives. |

## Models

| Agent | Model | Rebuild |
|---|---|---|
| Planner | LoRA on Qwen2.5-7B-Instruct, served by Ollama as `planner-agent` | `planner_agent/model/build_ollama_model.sh` (see `README_OLLAMA.md`) |
| Resource | `resource_agent/models/resource_models.pkl` | `resource_agent/training/README.md` |
| Performance | `performance_prediction_agent/models/*.pkl` (gitignored) | `cd performance_prediction_agent && python run_training.py` |
| Cost optimization | `cost_optimization_agent/models/cost_models.pkl` | `cost_optimization_agent/README.md` |
| Monitor | Groq-hosted LLM (no local model) | — |

`.pkl` files must be trained with the same scikit-learn as `requirements.txt` (1.6.1);
a mismatch makes the agent fall back to its heuristic.

## Data

Runtime state is in `data/` (gitignored):

- `adf_monitor.db` — SQLite (WAL mode): pipeline runs, AI analyses, manager runs, anomaly events, per-pipeline metrics.
- `manager_feedback.jsonl`, `resource_feedback.jsonl` — per-run feedback the learning loop trains on; rotated at 20 MB (5 archives kept, archives are still read).
- `ollama.log` — output of the auto-started Ollama server.

## Planner training data

The planner's fine-tuning dataset generator and validator live in
`planner_agent/training/` and are documented in its [README](planner_agent/training/README.md).
Run them from anywhere, e.g. `python planner_agent/training/validate_dataset.py`.

## Tests

```bash
python scripts/integration_test.py   # Resource → Performance → Cost pre-execution flow, no cloud calls
```
