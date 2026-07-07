# Data Pipeline Orchestrator

## Overview

This is an **AI-powered, multi-agent Data Pipeline Orchestrator** that automatically
designs, deploys, runs, and monitors cloud data pipelines from a user-provided CSV file
and a natural-language prompt. The pipeline is built on **Azure Data Factory (ADF)** for
ingest and **Azure Databricks (PySpark notebooks)** for transformation, with data flowing
through medallion-style stages (`raw → bronze → silver → gold`).

The "brain" that decides the pipeline shape is the **Planner Agent**, powered by a
**fine-tuned Qwen2.5-7B-Instruct model (LoRA adapter) served locally via Ollama**. Given
the CSV schema plus the user's request, it emits a complete JSON pipeline configuration —
containers, datasets, stages, transformations, and compute settings. (The legacy Groq
LLaMA backend is still selectable as a fallback; see [Planner backends](#planner-backends).)

> The original single-file Groq prototype lives in `py_files/` for reference. The current
> system is the multi-agent backend under `unified/`.

## Architecture

### Multi-agent backend (`unified/`)

A single **FastAPI** application (`unified/main.py`) hosts a team of cooperating agents,
each mounted under its own `/api/*` prefix:

| Agent | Role |
|-------|------|
| **Planner** | Reasons over schema + prompt to design the pipeline (stages, containers, transforms, compute). Powered by the fine-tuned Qwen model. |
| **Central Manager** | Orchestrates each run through 5 phases: validate → pre-checks → execute → assurance → feedback. |
| **Resource Agent** | Estimates the hardware/time each stage needs, checks it fits cloud limits, right-sizes it, and learns from real runs. |
| **Performance Prediction** | Forecasts whole-plan runtime/throughput and likely outcome (success/slowdown/failure) before a run. |
| **Executor** | Builds the pipeline on Azure (ADF copy + Databricks notebooks), runs it, and returns the result. |
| **Monitor** | Watches live runs, flags anomalies, and writes a plain-language explanation of each finished run (uses Groq for analysis). |
| **Assurance** | Validates planner output (structural + semantic layers) and checks a completed run is correct. |

### Technology stack

- **Language**: Python 3.9+
- **Planner model**: fine-tuned **Qwen2.5-7B-Instruct + LoRA**, served by **Ollama** (model `planner-agent`)
  - Legacy fallback: Groq LLaMA 3.3 70B (`PLANNER_BACKEND=groq`)
- **Monitor/analysis model**: Groq (run analysis, anomaly explanation)
- **Cloud platform**: Microsoft Azure
  - Azure Data Factory (ingest / copy)
  - Azure Databricks (PySpark notebook transforms)
  - Azure Blob Storage (medallion zones)
- **Backend**: FastAPI + Uvicorn, async SQLite (`aiosqlite`)
- **Frontend**: React + Vite dashboard (`unified/frontend/`)
- **Fine-tuning**: LoRA — Apple MLX (`planner_finetune/`) and the Ollama build (`unified/planner_agent/model/`)

## How It Works

```
User Input (CSV + Prompt)
         │
         ▼
   ┌─────────────┐
   │ Schema      │  /api/schema/detect — columns, types, preview
   │ Detector    │
   └─────────────┘
         │
         ▼
   ┌─────────────┐
   │  Planner    │ ◄── Fine-tuned Qwen2.5-7B decides:
   │  Agent      │    • Containers (raw/bronze/silver/gold)
   │ (Qwen/Ollama)    • Stages (copy vs notebook)
   └─────────────┘    • Transformations / filters / aggregations
         │            • Compute settings (workers, shuffle, node type, DIU)
         ▼
   ┌─────────────────────┐
   │  Central Manager    │  validate → pre-checks (resource + perf +
   │  (5-phase run)      │  cost) → execute → assurance → feedback
   └─────────────────────┘
         │
         ▼
   ┌─────────────────────┐
   │   Executor          │  • Create containers + upload data
   │  (ADF + Databricks) │  • ADF copy: raw → bronze
   │                     │  • Databricks notebooks: bronze → silver → gold
   └─────────────────────┘
         │
         ▼
   ┌─────────────────────┐
   │   Monitor           │  live status, anomaly flags, run explanation
   └─────────────────────┘
```

### Step-by-step

1. **Schema detection** — the backend reads the uploaded CSV and infers column names and
   types (`integer` / `double` / `string`), a preview, and a row-count sample.
2. **Planning (fine-tuned Qwen)** — schema + prompt are sent to the local Planner model
   (served by Ollama). It returns a JSON config with containers, datasets, stages
   (stage 0 `copy`, the rest `notebook`), execution order, and recommended compute
   settings. The user can review and edit the plan before running.
3. **Pre-checks (Central Manager)** — parallelism analysis, resource prediction (with hard
   feasibility limits), and a cost estimate. Infeasible plans are aborted before execution.
4. **Execution (Executor)** — authenticates to Azure, creates blob containers, uploads the
   CSV, runs the ADF copy stage, generates and runs Databricks PySpark notebooks for the
   transform stages, then returns the result.
5. **Monitoring & feedback** — the Monitor tracks the live run, flags anomalies, and (via
   Groq) writes a short explanation; real timings feed back to the Resource Agent.

## Pipeline configuration structure

The Planner emits a JSON config with these keys:
`containers`, `containers_to_create`, `datasets`, `stages`, `execution_order`,
`num_containers`, `recommended_settings`, `editable_settings`, `reasoning`.

```json
{
  "containers": ["raw", "bronze", "silver"],
  "containers_to_create": ["raw", "bronze", "silver"],
  "num_containers": 3,
  "datasets": [
    { "name": "DS_Raw",    "container": "raw",    "role": "source" },
    { "name": "DS_Bronze", "container": "bronze", "role": "intermediate" },
    { "name": "DS_Silver", "container": "silver", "role": "sink" }
  ],
  "stages": [
    { "name": "Ingest_Raw_to_Bronze", "type": "copy",
      "source": "raw", "sink": "bronze", "diu": 4 },
    { "name": "Transform_Bronze_to_Silver", "type": "notebook",
      "source": "bronze", "sink": "silver",
      "transformations": ["processed_time = currentTimestamp()", "name = upper(name)"],
      "num_workers": 2, "shuffle_partitions": 16 }
  ],
  "execution_order": ["Ingest_Raw_to_Bronze", "Transform_Bronze_to_Silver"],
  "recommended_settings": {
    "diu": 4, "num_workers": 2, "shuffle_partitions": 16, "node_type": "Standard_D4s_v3"
  },
  "editable_settings": { },
  "reasoning": "Explanation of pipeline decisions"
}
```

## Planner backends

The planner backend is chosen at startup via `PLANNER_BACKEND` in `unified/config.py`:

| Value | Backend |
|-------|---------|
| `ollama` (default) | Local fine-tuned **Qwen2.5-7B + LoRA**, served by Ollama at `OLLAMA_HOST`, model `PLANNER_MODEL` (default `planner-agent`). |
| `groq` | Legacy Groq cloud LLaMA 3.3 70B (`GROQ_API_KEY`). |

Build the local model with `unified/planner_agent/model/build_ollama_model.sh`
(`FROM qwen2.5:7b-instruct` + the fine-tuned LoRA adapter). See
`unified/planner_agent/model/README_OLLAMA.md` for details.

## Setup & usage

### 1. Configure credentials

```bash
cp unified/config.example.py unified/config.py
# fill in Azure, Databricks, Ollama / Groq values
```

See `SETUP_GUIDE.md` for step-by-step Azure resource creation.

### 2. Start the planner model (Ollama backend)

```bash
ollama serve
# build the fine-tuned planner model once:
cd unified/planner_agent/model && ./build_ollama_model.sh
```

### 3. Run the backend

```bash
cd unified
pip install -r requirements.txt
uvicorn main:app --reload
```

Health check: `GET /api/health`.

### 4. Run the dashboard

```bash
cd unified/frontend
npm install
npm run dev
```

## Fine-tuning the planner

Two LoRA fine-tuning paths are included:

- **`planner_finetune/`** — Apple **MLX** LoRA pipeline (setup → prepare → train → evaluate),
  16 GB-safe on Apple Silicon. See `planner_finetune/README.md`.
- **`unified/planner_agent/model/`** — converts the trained adapter into an Ollama model
  (`planner-agent-lora.gguf` + `Modelfile`).

The dataset (`planner_config_dataset.jsonl`) is generated and validated by the tooling in
`unified/` (`generate_dataset.py`, `validate_dataset.py`) — one JSON object per line of
`{schema, user_prompt, config}`, with a validator enforcing structural, quality, and
semantic correctness on every row.

## Supported transformations

The Planner generates Databricks PySpark transform stages including:

- **String**: `upper()`, `lower()`, `trim()`, `substring()`, `concat()`, `regexReplace()`
- **Type conversion**: cast to integer / double / string / timestamp
- **Date/Time**: `currentTimestamp()`, `year()`, `month()`, `dayOfMonth()`
- **Filters**: SQL-style predicates (`=`, `!=`, `<`, `<=`, `>`, `>=`, `between`, `in`)
- **Aggregation**: `sum()`, `avg()`, `min()`, `max()`, `count()` with `group_by`

## Security notes

- Credentials live in `unified/config.py`, which is gitignored — never commit real secrets.
  Copy from `config.example.py` and fill in locally (move to a secret store for production).
- Resources are deployed directly to live Azure (no Git integration on the ADF side).
- Databricks job clusters can drain credit if left running — watch the cost warning in `SETUP_GUIDE.md`.

## Dependencies

See `unified/requirements.txt` for the full list. Key dependencies:

- `fastapi`, `uvicorn` — backend web framework
- `requests`, `httpx` — HTTP clients (Ollama planner + Azure APIs)
- `aiosqlite` — async run/feedback store
- `groq` — Groq SDK (monitor analysis; legacy planner backend)
- `azure-identity`, `azure-mgmt-datafactory`, `azure-storage-blob` — Azure SDKs
- Ollama (external) — serves the fine-tuned Qwen planner model
```
