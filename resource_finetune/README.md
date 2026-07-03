# Resource Agent — Fine-tuning

Teach an LLM to emulate the **Resource Agent**: given a pipeline *plan* (ADF copy +
Databricks notebook stages), the input **data size**, and the intended **parallel
execution groups**, predict per-stage compute, right-size, resolve contention,
enforce the student-tier hard limits, and return the full **resource plan** JSON.

Mirrors the planner's fine-tuning setup (Qwen2.5-7B, QLoRA / Unsloth,
`train_on_responses_only`), but the notebook is Kaggle-oriented.

## Files

| File | Purpose |
|------|---------|
| `generate_resource_dataset.py` | Procedurally builds diverse pipeline plans, then labels each by running the **real** `ResourceAgent.analyze()` as an oracle. Value pools (table schemas, workspaces, source-table names, filter/aggregation frequencies) are grounded in the original dataset via its cleaned CSV mirror. |
| `build_resource_notebook.py` | Emits `finetune_qwen_resource.ipynb` cell-by-cell. |
| `finetune_qwen_resource.ipynb` | The Kaggle QLoRA/Unsloth fine-tuning notebook. |

Output dataset: `Datasets/Datasets/Synthetic/synthetic_resource_dataset.json`.

## 1. Generate the dataset

```bash
python resource_finetune/generate_resource_dataset.py --rows 1500
```

Each record:

```json
{
  "schema":           {"columns": [...], "inferred_types": {...}, "row_count": N, "size_hint": "..."},
  "csv_size_bytes":   12345678,
  "input_plan":       {"num_containers": N, "containers_to_create": [...],
                       "recommended_settings": {...}, "execution_order": [...], "stages": [...]},
  "execution_groups": [["ingest"], ["clean", "enrich"], ...],
  "resource_plan":    { ...ResourceAgent.analyze() output — the label... }
}
```

Because every label comes from the deterministic agent, the training targets are
exactly self-consistent with the hard limits (`MAX_WORKERS=4`, `MAX_DIU=8`,
`MAX_CONCURRENT=3`, `MAX_TOTAL_MEM_GB=64`) and the cost/throughput model. The
generator deliberately over-requests workers/DIU and builds oversized parallel
groups on some examples so the labels cover clamping, contention resolution, and
group-splitting.

## 2. Fine-tune on Kaggle

1. Upload `synthetic_resource_dataset.json` as a Kaggle **Dataset** (or straight to
   the working dir).
2. New Notebook → **Settings → Accelerator → GPU T4 x2** (or P100).
3. **Add Input** → attach the dataset.
4. Upload / paste `finetune_qwen_resource.ipynb` and **Run all**
   (the install cell may ask for one kernel restart).

Adapters are written to `/kaggle/working/qwen_resource_lora` — download them from the
notebook **Output** tab. To regenerate the notebook after editing the builder:

```bash
python resource_finetune/build_resource_notebook.py
```

## Serving

Keep `SYSTEM_PROMPT` and `build_user_content` identical at inference. The
deterministic `ResourceAgent.analyze()` stays the ground-truth oracle and safety net;
the fine-tuned model reproduces its judgement in a single forward pass and should fall
back to the deterministic agent whenever the emitted JSON fails to parse.
