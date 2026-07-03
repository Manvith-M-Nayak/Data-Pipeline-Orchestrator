#!/usr/bin/env python3
"""
Emit `finetune_qwen_resource.ipynb` — a Kaggle QLoRA/Unsloth fine-tuning notebook
for the RESOURCE AGENT. Mirrors the planner's finetune notebook structure
(Qwen2.5-7B, LoRA r=16, train_on_responses_only) but is Kaggle-oriented
(GPU T4 x2 / P100, /kaggle/input, /kaggle/working) and consumes
`synthetic_resource_dataset.json`.

    python resource_finetune/build_resource_notebook.py
"""

import json
import os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "finetune_qwen_resource.ipynb")

cells = []


def md(text):
    cells.append({"cell_type": "markdown", "metadata": {},
                  "source": text.splitlines(keepends=True)})


def code(text):
    cells.append({"cell_type": "code", "execution_count": None, "metadata": {},
                  "outputs": [], "source": text.strip("\n").splitlines(keepends=True)})


# ── 1. Title ─────────────────────────────────────────────────────────────────
md(r"""# Fine-tuning Qwen2.5-7B-Instruct (QLoRA / Unsloth) — **Resource Agent** (Kaggle)

Teaches the model to emulate the **Resource Agent**: given a pipeline *plan*
(ADF copy + Databricks notebook stages), the input **data size**, and the intended
**parallel execution groups**, it predicts per-stage compute requirements, right-sizes
and resolves contention, enforces the student-tier hard limits, and returns the full
**resource plan** JSON.

**Dataset:** `synthetic_resource_dataset.json` — each record is
`{schema, csv_size_bytes, input_plan, execution_groups, resource_plan}`, where the
label `resource_plan` is produced by the real `ResourceAgent.analyze()` oracle.

**Kaggle setup**
1. Sidebar → **Settings → Accelerator → GPU T4 x2** (or P100).
2. Sidebar → **Add Input** → attach the dataset you uploaded
   (`synthetic_resource_dataset.json`), *or* upload it to the working directory.
3. **Run all**. The install cell may ask you to restart the kernel once.
""")

# ── 2. Environment check ─────────────────────────────────────────────────────
md(r"""## 1. Environment check
Confirm a GPU is attached before installing anything heavy.""")

code(r"""
import subprocess, sys
print("Python:", sys.version.split()[0])
try:
    print(subprocess.check_output(["nvidia-smi"]).decode())
except Exception as e:
    print("nvidia-smi not found — enable a GPU accelerator (T4 x2 or P100) in the sidebar:", e)
""")

# ── 3. Install ───────────────────────────────────────────────────────────────
md(r"""## 2. Install dependencies
Kaggle already ships a CUDA-enabled PyTorch. **Unsloth** pulls a coherent
`transformers / trl / peft / accelerate / bitsandbytes / datasets` stack, so we do
**not** pin those ourselves.""")

code(r"""
%pip install -q --upgrade --no-cache-dir unsloth
# If a later cell reports a version clash, uncomment and re-run this one:
# %pip install -q --upgrade --no-cache-dir "unsloth[kaggle-new]"
print("Install done. If imports fail below: Run > Restart & Clear Cell Outputs, then re-run from here.")
""")

# ── 4. Config ────────────────────────────────────────────────────────────────
md(r"""## 3. Configuration""")

code(r"""
import os, glob

# Locate the dataset (attached Kaggle Dataset, or uploaded to the working dir).
DATASET_NAME = "synthetic_resource_dataset.json"
_cands = [DATASET_NAME, os.path.join("/kaggle/input", DATASET_NAME)]
_cands += glob.glob("/kaggle/input/**/" + DATASET_NAME, recursive=True)
_cands += glob.glob("/kaggle/working/**/" + DATASET_NAME, recursive=True)
DATASET_PATH = next((p for p in _cands if os.path.exists(p)), None)
assert DATASET_PATH, (
    "Could not find " + DATASET_NAME + " — attach it via 'Add Input' or upload it "
    "to /kaggle/working."
)
print("Dataset:", DATASET_PATH)

BASE_MODEL     = "unsloth/Qwen2.5-7B-Instruct-bnb-4bit"   # pre-quantized 4-bit
MAX_SEQ_LENGTH = 4096      # resource plans are verbose; the token-stats cell verifies
LORA_RANK      = 16
LORA_ALPHA     = 16
OUTPUT_LORA    = "/kaggle/working/qwen_resource_lora"
OUTPUT_MERGED  = "/kaggle/working/qwen_resource_merged"
SEED           = 3407
""")

# ── 5. Build chat dataset ────────────────────────────────────────────────────
md(r"""## 4. Build the chat dataset
Each record becomes a 3-turn chat: **system** (the Resource Agent contract: hard
limits + cost model) → **user** (plan + data size + intended groups) → **assistant**
(the `resource_plan` JSON label).""")

code(r"""
import json
from datasets import Dataset

SYSTEM_PROMPT = (
    "You are a compute resource-planning agent for a hybrid Azure Data Factory + "
    "Databricks pipeline running on a student / free tier. Given a pipeline plan "
    "(stages + recommended settings), the input data size, and the intended parallel "
    "execution groups, output exactly ONE JSON object: the resource plan.\n\n"
    "Stage types: 'copy' = ADF Copy Activity sized in DIU; 'notebook' = Databricks "
    "PySpark sized in workers (0 = driver-only).\n\n"
    "HARD LIMITS — never exceed in allocations or groups:\n"
    "  MAX_WORKERS = 4 per notebook stage; MAX_DIU = 8 per copy stage;\n"
    "  MAX_CONCURRENT = 3 stages per parallel group; MAX_TOTAL_MEM_GB = 64 combined per group.\n\n"
    "COST / THROUGHPUT MODEL:\n"
    "  ADF copy   : 30s startup + bytes / (DIU * 5 MB/s); DIU ~ 1 vCPU, ~1.5 GB each.\n"
    "  Databricks : 90s cold start + 30s pip + rows/50000 read + write + 3s per "
    "transformation + 10s per aggregation; memory = 4 GB driver + workers * node_memory.\n"
    "  Node catalogue (vCPU / GB): Standard_D4s_v3 4/16, Standard_DS3_v2 4/14, "
    "Standard_DS2_v2 2/7, Standard_DS4_v2 8/28, Standard_D8s_v3 8/32.\n\n"
    "PROCEDURE: for each stage predict requirements, then right-size (a sub-2-minute "
    "notebook collapses to driver-only), resolve parallel-group contention (reduce "
    "workers first, else serialize the heaviest stage into a later group), and hard-cap "
    "everything. Over-requests are clamped and surfaced as warnings; a single stage whose "
    "memory exceeds 64 GB makes the plan infeasible.\n\n"
    "Output JSON keys (exactly these): feasible, constraint_violations, warnings, "
    "stage_requirements, allocations, execution_groups, total_workers, total_memory_gb, "
    "peak_concurrent_workers, estimated_total_s, correction_factors. "
    "Output ONLY the JSON object — no markdown, no commentary."
)

def build_user_content(rec):
    s, p = rec["schema"], rec["input_plan"]
    return (
        f"Input data: row_count={s['row_count']}, size_hint=\"{s['size_hint']}\", "
        f"csv_size_bytes={rec['csv_size_bytes']}\n"
        f"CSV columns: {s['columns']}\n\n"
        f"Pipeline plan:\n{json.dumps(p, indent=2)}\n\n"
        f"Intended execution groups (parallelism): {rec['execution_groups']}\n\n"
        "Produce the resource plan JSON (feasibility, per-stage requirements and "
        "allocations, contention-resolved execution groups, and totals):"
    )

def record_to_messages(rec):
    return [
        {"role": "system",    "content": SYSTEM_PROMPT},
        {"role": "user",      "content": build_user_content(rec)},
        {"role": "assistant", "content": json.dumps(rec["resource_plan"], indent=2)},
    ]

with open(DATASET_PATH, encoding="utf-8") as f:
    raw = json.load(f)
print("Loaded", len(raw), "records")

examples = [{"messages": record_to_messages(r)} for r in raw]
ds = Dataset.from_list(examples)
if len(ds) >= 20:
    split = ds.train_test_split(test_size=0.1, seed=SEED)
    train_ds, eval_ds = split["train"], split["test"]
else:
    train_ds, eval_ds = ds, None
print("train:", len(train_ds), "| eval:", len(eval_ds) if eval_ds else 0)
""")

# ── 6. Load model + LoRA ─────────────────────────────────────────────────────
md(r"""## 5. Load model + tokenizer (4-bit) and attach LoRA""")

code(r"""
from unsloth import FastLanguageModel
import torch

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name     = BASE_MODEL,
    max_seq_length = MAX_SEQ_LENGTH,
    dtype          = None,        # auto (bf16 where supported)
    load_in_4bit   = True,
)

model = FastLanguageModel.get_peft_model(
    model,
    r              = LORA_RANK,
    lora_alpha     = LORA_ALPHA,
    lora_dropout   = 0,
    bias           = "none",
    target_modules = ["q_proj", "k_proj", "v_proj", "o_proj",
                      "gate_proj", "up_proj", "down_proj"],
    use_gradient_checkpointing = "unsloth",
    random_state   = SEED,
)
""")

# ── 7. Token stats + fit filter ──────────────────────────────────────────────
md(r"""## 6. Token-length check
Verify `MAX_SEQ_LENGTH` is large enough, and drop any example that would be
silently truncated (truncating the target JSON would teach the model to emit
invalid output).""")

code(r"""
lens = [len(tokenizer.apply_chat_template(e["messages"], tokenize=True)) for e in examples]
over = sum(1 for l in lens if l > MAX_SEQ_LENGTH)
print(f"tokens/example  max={max(lens)}  mean={sum(lens)//len(lens)}  "
      f"over {MAX_SEQ_LENGTH}: {over}/{len(lens)}")

def _fits(e):
    return len(tokenizer.apply_chat_template(e["messages"], tokenize=True)) <= MAX_SEQ_LENGTH

if over:
    b_tr = len(train_ds)
    train_ds = train_ds.filter(_fits)
    if eval_ds is not None:
        eval_ds = eval_ds.filter(_fits)
    print(f"Dropped long examples — train {b_tr} -> {len(train_ds)} "
          f"(raise MAX_SEQ_LENGTH + reload the model to keep them).")
else:
    print("OK: every example fits in MAX_SEQ_LENGTH =", MAX_SEQ_LENGTH)
""")

# ── 8. Apply chat template ───────────────────────────────────────────────────
md(r"""## 7. Apply the Qwen chat template → training text""")

code(r"""
def formatting_func(batch):
    return {"text": [
        tokenizer.apply_chat_template(m, tokenize=False, add_generation_prompt=False)
        for m in batch["messages"]
    ]}

train_fmt = train_ds.map(formatting_func, batched=True, remove_columns=train_ds.column_names)
eval_fmt  = (eval_ds.map(formatting_func, batched=True, remove_columns=eval_ds.column_names)
             if eval_ds is not None else None)
print(train_fmt[0]["text"][:700])
""")

# ── 9. Trainer ───────────────────────────────────────────────────────────────
md(r"""## 8. Trainer
Loss is computed on the **assistant response only** (`train_on_responses_only`), so
the model is not trained to reproduce the (long) prompt.""")

code(r"""
import torch
from trl import SFTTrainer, SFTConfig
from unsloth.chat_templates import train_on_responses_only

sft_args = SFTConfig(
    dataset_text_field          = "text",
    max_seq_length              = MAX_SEQ_LENGTH,
    packing                     = False,
    per_device_train_batch_size = 2,
    gradient_accumulation_steps = 4,        # effective batch = 8
    warmup_ratio                = 0.05,
    num_train_epochs            = 3,
    learning_rate               = 2e-4,
    fp16 = not torch.cuda.is_bf16_supported(),
    bf16 = torch.cuda.is_bf16_supported(),
    logging_steps               = 5,
    optim                       = "adamw_8bit",
    weight_decay                = 0.01,
    lr_scheduler_type           = "cosine",
    seed                        = SEED,
    output_dir                  = "/kaggle/working/outputs",
    report_to                   = "none",   # no W&B
)

trainer = SFTTrainer(
    model         = model,
    tokenizer     = tokenizer,
    train_dataset = train_fmt,
    eval_dataset  = eval_fmt,
    args          = sft_args,
)

# Mask everything except the assistant turn (Qwen ChatML markers) so loss is on
# the resource-plan JSON only.
trainer = train_on_responses_only(
    trainer,
    instruction_part = "<|im_start|>user\n",
    response_part    = "<|im_start|>assistant\n",
)
""")

# ── 10. Train ────────────────────────────────────────────────────────────────
md(r"""## 9. Train""")

code(r"""
stats = trainer.train()
print(stats)
""")

# ── 11. Inference test ───────────────────────────────────────────────────────
md(r"""## 10. Quick inference test
Generate a resource plan for a held-out-style input and confirm it parses as JSON
with the expected keys.""")

code(r"""
FastLanguageModel.for_inference(model)

test_rec = {
    "schema": {
        "columns": ["order_id", "region", "amount", "quantity", "ts"],
        "row_count": 5_000_000, "size_hint": "large (50–200MB)",
    },
    "csv_size_bytes": 80 * 1024 * 1024,
    "input_plan": {
        "num_containers": 4,
        "containers_to_create": ["raw", "bronze", "silver", "gold"],
        "recommended_settings": {"num_workers": 3, "node_type": "Standard_D4s_v3",
                                 "diu": 8, "shuffle_partitions": 16},
        "execution_order": ["ingest", "clean", "aggregate"],
        "stages": [
            {"name": "ingest", "type": "copy", "source_dataset": "DS_Raw",
             "sink_dataset": "DS_Bronze", "diu": 12},
            {"name": "clean", "type": "notebook", "source_container": "bronze",
             "sink_container": "silver", "num_workers": 6,
             "transformations": ["processed_time = currentTimestamp()",
                                 "amount_scaled = amount * 2"],
             "filter_condition": "amount > 0"},
            {"name": "aggregate", "type": "notebook", "source_container": "silver",
             "sink_container": "gold", "num_workers": 3,
             "aggregations": {"agg_exprs": ["sum(amount)", "avg(amount)", "count(*)"]}},
        ],
    },
    "execution_groups": [["ingest"], ["clean", "aggregate"]],
}

msgs = [{"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": build_user_content(test_rec)}]
inputs = tokenizer.apply_chat_template(msgs, add_generation_prompt=True,
                                       return_tensors="pt").to("cuda")
out = model.generate(input_ids=inputs, max_new_tokens=1536,
                     temperature=0.1, top_p=0.9, do_sample=True)
text = tokenizer.decode(out[0][inputs.shape[1]:], skip_special_tokens=True)
print(text[:2000])
try:
    obj = json.loads(text)
    print("\nValid JSON. feasible =", obj.get("feasible"),
          "| keys:", list(obj.keys()))
except Exception as e:
    print("\nNot valid JSON yet (more data / epochs may help):", e)
""")

# ── 12. Save ─────────────────────────────────────────────────────────────────
md(r"""## 11. Save
LoRA adapters are tiny (~80 MB) and land in `/kaggle/working` — download them from
the notebook **Output** tab, or save the notebook to create a versioned artifact.""")

code(r"""
model.save_pretrained(OUTPUT_LORA)
tokenizer.save_pretrained(OUTPUT_LORA)
print("Saved LoRA adapters ->", OUTPUT_LORA)

# Optional: merged fp16 (standalone ~15 GB; needs disk + RAM). Uncomment to use.
# model.save_pretrained_merged(OUTPUT_MERGED, tokenizer, save_method="merged_16bit")

# Optional: GGUF for llama.cpp / Ollama (builds llama.cpp; slow). Uncomment to use.
# model.save_pretrained_gguf("/kaggle/working/qwen_resource_gguf", tokenizer,
#                            quantization_method="q4_k_m")
""")

# ── 13. Integration note ─────────────────────────────────────────────────────
md(r"""## 12. Using the fine-tuned model
Keep `SYSTEM_PROMPT` and `build_user_content` **identical** at inference time (same
as training). Two options to serve it as an LLM-backed Resource Agent:

1. **Local serve** — merge to 16-bit or GGUF, run with vLLM / Ollama (OpenAI-compatible
   endpoint), and have the agent call it, falling back to the deterministic
   `ResourceAgent.analyze()` when the JSON fails to parse.
2. **In-process** — load the base model + these LoRA adapters with `peft` /
   `FastLanguageModel.from_pretrained(..., adapter=OUTPUT_LORA)`.

The deterministic `ResourceAgent` remains the ground-truth oracle and the safety net;
the fine-tuned model reproduces its judgement in one forward pass.""")

# ── Write notebook ───────────────────────────────────────────────────────────
nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10"},
        "accelerator": "GPU",
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print(f"Wrote {OUT}  ({len(cells)} cells)")
