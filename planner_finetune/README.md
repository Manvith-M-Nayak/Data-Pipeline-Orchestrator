# Planner Agent — MLX LoRA fine-tuning

Fine-tunes a small LLM with LoRA on **Apple MLX** to act as the **Planner Agent** of
the data-pipeline orchestrator: given CSV schema metadata + a transformation request,
it emits a JSON pipeline config for an Azure Data Factory (ADF) ingest + Databricks
notebook pipeline.

Everything runs on the **Metal GPU within 16 GB** — no CUDA, Unsloth, bitsandbytes,
Triton, or PyTorch-GPU.

- **Base model:** `mlx-community/Qwen2.5-3B-Instruct-4bit`
- **Target machine:** MacBook Air M5, 16 GB unified memory, macOS

## Pipeline

| Stage | Script | What it does |
|-------|--------|--------------|
| 1 Setup | `setup.sh` | venv + `mlx-lm[train]`, confirm Metal GPU |
| 2 Data prep | `prepare_data.py` | dataset → MLX chat format, 90/10 split, validate |
| 3 Train | `train.sh` + `lora_config.yaml` | `mlx_lm.lora`, 16 GB-safe, OOM auto-retry |
| 4 Compare | `evaluate.py` | base vs fine-tuned, side-by-side + metrics table |

## Quick start

```bash
cd planner_finetune
./run_all.sh          # setup → prepare → train → evaluate
```

Or step by step:

```bash
./setup.sh
source .venv/bin/activate
python prepare_data.py
./train.sh
python evaluate.py
```

## Dataset

`planner_config_dataset.jsonl` — one JSON object per line. Each record is
`{schema, user_prompt, config}`; the assistant target is the wrapped config
`{"config": <9-key config>, "used_fallback": false}`.

If the file is missing, `prepare_data.py` writes a tiny 3-example sample so the
pipeline still runs end to end.

> Stages 2–4 are added in subsequent commits.
