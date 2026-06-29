"""
Emits `finetune_qwen_planner.ipynb` — a QLoRA (Unsloth) fine-tuning notebook for
Qwen2.5-7B-Instruct on the synthetic planner dataset.

Run:  python build_finetune_notebook.py
Then verify the data-prep block with:  python build_finetune_notebook.py --verify
"""
import json
import sys

NB_PATH = "finetune_qwen_planner.ipynb"
DATASET = "synthetic_planner_dataset.jsonl"


# ── The system prompt the fine-tuned model is trained against. Kept consistent
#    with the real groq_planner contract so the model is a drop-in replacement. ─
SYSTEM_PROMPT = (
    "You are a hybrid Azure Data Factory + Databricks pipeline architect. "
    "ADF orchestrates (control plane); Databricks computes (execution plane). "
    "Given a CSV schema and a user request, output exactly ONE JSON object "
    "describing a multi-stage medallion pipeline (raw -> bronze -> silver -> ...).\n\n"
    "Stage types:\n"
    "  - 'copy'    : ADF Copy Activity, ingestion ONLY (stage0 -> stage1), no transforms.\n"
    "  - 'notebook': Databricks PySpark; does transformations, filter, aggregation.\n\n"
    "Transformation DSL ('output_col = expr'): upper/lower/trim/initCap/length, "
    "toInteger/toDouble/toString/toTimestamp, concat/substring/regexReplace, "
    "year/month/dayOfMonth, currentTimestamp(), and arithmetic (+ - * /). "
    "ALWAYS add 'processed_time = currentTimestamp()' to every notebook stage.\n\n"
    "Filter ('filter_condition'): equals/notEquals/greater/less(toInteger(col), n), "
    "equals(col, 'value'), isNull(col), or 'col > n'.\n\n"
    "Aggregation (optional, notebook stages only): "
    "{\"group_by\": [...], \"aggregations\": [{\"op\": avg|sum|min|max|count, "
    "\"column\": <col or '*'>, \"alias\": <name>}]}. "
    "avg/sum require numeric columns; group_by and aggregated columns MUST exist "
    "in the schema.\n\n"
    "Rules: first stage is 'copy', the rest are 'notebook'; reference ONLY columns "
    "that exist in the schema; preserve column names exactly. "
    "Output ONLY the JSON object — no markdown, no commentary."
)


def build_user_content(rec):
    """Mirror groq_planner.decide_pipeline_config's user message (self-contained)."""
    s, cfg = rec["schema"], rec["config"]
    return (
        f"CSV Columns: {s['columns']}\n"
        f"Column Types: {s['inferred_types']}\n"
        f"Row Count: {s['row_count']}\n"
        f"File Size: {s['size_hint']}\n\n"
        f"Sample Data:\n{json.dumps(s['samples'][:3], indent=2)}\n\n"
        f'User Prompt: "{rec["user_prompt"]}"\n\n'
        f"Number of stages: {cfg['num_containers']}\n"
        f"Container names: {cfg['containers_to_create']}\n\n"
        "Design the complete unified ADF+Databricks pipeline configuration JSON:"
    )


def record_to_messages(rec):
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_content(rec)},
        {"role": "assistant", "content": json.dumps(rec["config"], indent=2)},
    ]


# ════════════════════════════════════════════════════════════════════════════
# Verification path (runs locally, no GPU needed)
# ════════════════════════════════════════════════════════════════════════════
def verify():
    with open(DATASET, encoding="utf-8") as f:
        data = json.load(f)
    print(f"Loaded {len(data)} records from {DATASET}")
    max_chars = 0
    for rec in data:
        msgs = record_to_messages(rec)
        assert [m["role"] for m in msgs] == ["system", "user", "assistant"]
        # assistant content must be parseable JSON identical to the config
        assert json.loads(msgs[2]["content"]) == rec["config"]
        total = sum(len(m["content"]) for m in msgs)
        max_chars = max(max_chars, total)
    # rough token estimate for code/JSON ~ chars / 3.3
    est_tokens = int(max_chars / 3.3)
    print(f"Longest example: {max_chars} chars  (~{est_tokens} tokens est.)")
    print("Recommended max_seq_length: 2048" if est_tokens < 1900 else
          "Recommended max_seq_length: 4096")
    print("\nSAMPLE (first record) ----------------------------------------")
    sample = record_to_messages(data[0])
    print("[system] ", sample[0]["content"][:200], "...")
    print("\n[user]\n", sample[1]["content"][:400], "...")
    print("\n[assistant]\n", sample[2]["content"][:300], "...")
    print("\nData-prep verification PASSED.")


# ════════════════════════════════════════════════════════════════════════════
# Notebook assembly
# ════════════════════════════════════════════════════════════════════════════
_CELL_N = [0]


def _cid(prefix):
    _CELL_N[0] += 1
    return f"{prefix}{_CELL_N[0]:02d}"


def md(*lines):
    return {"cell_type": "markdown", "id": _cid("md"), "metadata": {}, "source": _src(lines)}


def code(*lines):
    return {"cell_type": "code", "id": _cid("code"), "metadata": {},
            "execution_count": None, "outputs": [], "source": _src(lines)}


def _src(lines):
    text = "\n".join(lines)
    parts = text.split("\n")
    return [p + "\n" for p in parts[:-1]] + [parts[-1]]


def build_notebook():
    sys_prompt_py = json.dumps(SYSTEM_PROMPT)

    cells = []

    cells.append(md(
        "# Fine-tuning Qwen2.5-7B-Instruct (QLoRA / Unsloth) — Pipeline Planner",
        "",
        "Fine-tunes the planner agent to emit the unified **ADF + Databricks** pipeline-config",
        "JSON from a CSV schema + a natural-language request.",
        "",
        "**Target hardware:** Intel CPU · **RTX 3080 Ti (12 GB VRAM)** · 64 GB RAM.",
        "Qwen2.5-7B in 4-bit QLoRA uses ~7-9 GB VRAM at `max_seq_length=2048`, so it fits.",
        "",
        "---",
        "## 0. Prerequisites (READ FIRST)",
        "",
        "**Operating system — strongly recommended: Linux or Windows + WSL2 (Ubuntu).**",
        "Unsloth + Triton + bitsandbytes are most reliable there. Native Windows works but is",
        "fragile; if you must, see the *Native Windows* note in the install cell.",
        "",
        "1. **NVIDIA driver + CUDA**: driver supporting CUDA 12.1+ (`nvidia-smi` should work).",
        "2. **Python 3.11** (3.10 also fine; avoid 3.12+ for Unsloth).",
        "   Use a fresh conda/venv environment:",
        "   ```bash",
        "   conda create -n qwenft python=3.11 -y && conda activate qwenft",
        "   ```",
        "3. **~30 GB free disk** (base model download ~5 GB + checkpoints).",
        "4. Copy **`synthetic_planner_dataset.jsonl`** next to this notebook.",
        "5. (Optional) a Hugging Face account/token if the model needs auth (Qwen is public).",
        "",
        "> **Dataset size warning:** 20 records is enough to smoke-test the pipeline but NOT to",
        "> produce a good model. Regenerate a larger set first (e.g. change `range(20)` ->",
        "> `range(2000)` in `generate_dataset.py`) and aim for **a few thousand** examples.",
    ))

    cells.append(md(
        "## 1. Environment check",
        "Confirm the GPU is visible before installing anything heavy.",
    ))
    cells.append(code(
        "import subprocess, sys",
        "print('Python:', sys.version)",
        "try:",
        "    print(subprocess.check_output(['nvidia-smi']).decode())",
        "except Exception as e:",
        "    print('nvidia-smi not found — check your NVIDIA driver install:', e)",
    ))

    cells.append(md(
        "## 2. Install dependencies",
        "",
        "Run this cell **once** per environment. It installs a CUDA-12.1 PyTorch build,",
        "Unsloth, and the training stack. Unsloth pins compatible `transformers`/`trl`/`peft`.",
        "",
        "**Native Windows note:** if you are NOT in WSL2/Linux, also run:",
        "`pip install triton-windows` and ensure `bitsandbytes>=0.43` (has Windows wheels).",
        "If `xformers`/`flash-attn` fail to build, ignore them — Unsloth ships its own kernels.",
    ))
    cells.append(code(
        "# 1) PyTorch (CUDA 12.1). Skip if a CUDA torch is already installed.",
        "#    Check first:  python -c \"import torch;print(torch.__version__,torch.cuda.is_available())\"",
        "%pip install --quiet 'torch==2.4.1' --index-url https://download.pytorch.org/whl/cu121",
        "",
        "# 2) Unsloth (official one-liner). It pulls a COHERENT, mutually-compatible",
        "#    transformers / trl / peft / accelerate / bitsandbytes / datasets set,",
        "#    so do NOT pin those yourself.",
        "%pip install --quiet --upgrade --no-cache-dir unsloth",
        "",
        "# Native Windows only (NOT WSL2/Linux), uncomment:",
        "# %pip install --quiet triton-windows",
        "print('Install step done. RESTART THE KERNEL before continuing.')",
    ))

    cells.append(md(
        "## 3. Configuration",
    ))
    cells.append(code(
        "DATASET_PATH    = 'synthetic_planner_dataset.jsonl'",
        "BASE_MODEL      = 'unsloth/Qwen2.5-7B-Instruct-bnb-4bit'  # pre-quantized 4-bit",
        "MAX_SEQ_LENGTH  = 2048      # raise to 4096 if the token-stats cell says so",
        "LORA_RANK       = 16",
        "LORA_ALPHA      = 16",
        "OUTPUT_LORA     = 'qwen_planner_lora'",
        "OUTPUT_MERGED   = 'qwen_planner_merged'",
        "SEED            = 3407",
    ))

    cells.append(md(
        "## 4. Build the chat dataset",
        "Each record `{schema, user_prompt, config}` becomes a 3-turn chat:",
        "system (task + DSL grammar) -> user (schema + request) -> assistant (config JSON).",
        "The system prompt mirrors the real `groq_planner` contract so this model can be",
        "swapped in directly.",
    ))
    cells.append(code(
        "import json",
        "from datasets import Dataset",
        "",
        f"SYSTEM_PROMPT = {sys_prompt_py}",
        "",
        "def build_user_content(rec):",
        "    s, cfg = rec['schema'], rec['config']",
        "    return (",
        "        f\"CSV Columns: {s['columns']}\\n\"",
        "        f\"Column Types: {s['inferred_types']}\\n\"",
        "        f\"Row Count: {s['row_count']}\\n\"",
        "        f\"File Size: {s['size_hint']}\\n\\n\"",
        "        f\"Sample Data:\\n{json.dumps(s['samples'][:3], indent=2)}\\n\\n\"",
        "        f'User Prompt: \"{rec[\"user_prompt\"]}\"\\n\\n'",
        "        f\"Number of stages: {cfg['num_containers']}\\n\"",
        "        f\"Container names: {cfg['containers_to_create']}\\n\\n\"",
        "        'Design the complete unified ADF+Databricks pipeline configuration JSON:'",
        "    )",
        "",
        "def record_to_messages(rec):",
        "    return [",
        "        {'role': 'system', 'content': SYSTEM_PROMPT},",
        "        {'role': 'user', 'content': build_user_content(rec)},",
        "        {'role': 'assistant', 'content': json.dumps(rec['config'], indent=2)},",
        "    ]",
        "",
        "with open(DATASET_PATH, encoding='utf-8') as f:",
        "    raw = [json.loads(line) for line in f if line.strip()]",
        "print(f'Loaded {len(raw)} records')",
        "",
        "examples = [{'messages': record_to_messages(r)} for r in raw]",
        "ds = Dataset.from_list(examples)",
        "",
        "# Train/val split (guard tiny datasets).",
        "if len(ds) >= 20:",
        "    split = ds.train_test_split(test_size=0.1, seed=SEED)",
        "    train_ds, eval_ds = split['train'], split['test']",
        "else:",
        "    train_ds, eval_ds = ds, None",
        "print('train:', len(train_ds), '| eval:', len(eval_ds) if eval_ds else 0)",
    ))

    cells.append(md(
        "## 5. Load model + tokenizer (4-bit) and attach LoRA",
    ))
    cells.append(code(
        "from unsloth import FastLanguageModel",
        "import torch",
        "",
        "model, tokenizer = FastLanguageModel.from_pretrained(",
        "    model_name     = BASE_MODEL,",
        "    max_seq_length = MAX_SEQ_LENGTH,",
        "    dtype          = None,        # auto (bf16 on Ampere)",
        "    load_in_4bit   = True,",
        ")",
        "",
        "model = FastLanguageModel.get_peft_model(",
        "    model,",
        "    r              = LORA_RANK,",
        "    lora_alpha     = LORA_ALPHA,",
        "    lora_dropout   = 0,",
        "    bias           = 'none',",
        "    target_modules = ['q_proj','k_proj','v_proj','o_proj',",
        "                      'gate_proj','up_proj','down_proj'],",
        "    use_gradient_checkpointing = 'unsloth',",
        "    random_state   = SEED,",
        ")",
    ))

    cells.append(md(
        "## 6. Token-length stats",
        "Confirms `MAX_SEQ_LENGTH` is large enough (no silent truncation of targets).",
    ))
    cells.append(code(
        "lens = [len(tokenizer.apply_chat_template(e['messages'], tokenize=True))",
        "        for e in examples]",
        "print('max tokens:', max(lens), '| mean:', sum(lens)//len(lens))",
        "assert max(lens) <= MAX_SEQ_LENGTH, (",
        "    f'Increase MAX_SEQ_LENGTH to >= {max(lens)} and reload the model')",
        "print('OK: all examples fit in MAX_SEQ_LENGTH =', MAX_SEQ_LENGTH)",
    ))

    cells.append(md(
        "## 7. Apply Qwen chat template -> training text",
    ))
    cells.append(code(
        "def formatting_func(batch):",
        "    return {'text': [",
        "        tokenizer.apply_chat_template(m, tokenize=False, add_generation_prompt=False)",
        "        for m in batch['messages']",
        "    ]}",
        "",
        "train_fmt = train_ds.map(formatting_func, batched=True, remove_columns=train_ds.column_names)",
        "eval_fmt  = (eval_ds.map(formatting_func, batched=True, remove_columns=eval_ds.column_names)",
        "             if eval_ds else None)",
        "print(train_fmt[0]['text'][:600])",
    ))

    cells.append(md(
        "## 8. Trainer",
        "Loss is computed on the **assistant response only** (`train_on_responses_only`),",
        "so the model is not trained to reproduce the prompt.",
    ))
    cells.append(code(
        "import torch",
        "from trl import SFTTrainer, SFTConfig",
        "from unsloth.chat_templates import train_on_responses_only",
        "",
        "# Current TRL puts all training + dataset args inside SFTConfig.",
        "sft_args = SFTConfig(",
        "    dataset_text_field          = 'text',",
        "    max_seq_length              = MAX_SEQ_LENGTH,",
        "    packing                     = False,",
        "    per_device_train_batch_size = 2,",
        "    gradient_accumulation_steps = 4,    # effective batch 8",
        "    warmup_ratio                = 0.05,",
        "    num_train_epochs            = 3,    # small data -> a few epochs",
        "    learning_rate               = 2e-4,",
        "    fp16 = not torch.cuda.is_bf16_supported(),",
        "    bf16 = torch.cuda.is_bf16_supported(),",
        "    logging_steps               = 1,",
        "    optim                       = 'adamw_8bit',",
        "    weight_decay                = 0.01,",
        "    lr_scheduler_type           = 'cosine',",
        "    seed                        = SEED,",
        "    output_dir                  = 'outputs',",
        "    report_to                   = 'none',",
        ")",
        "",
        "trainer = SFTTrainer(",
        "    model         = model,",
        "    tokenizer     = tokenizer,    # Unsloth accepts this alias for processing_class",
        "    train_dataset = train_fmt,",
        "    eval_dataset  = eval_fmt,",
        "    args          = sft_args,",
        ")",
        "",
        "# Mask everything except the assistant turn (Qwen ChatML markers),",
        "# so loss is computed on the config JSON only.",
        "trainer = train_on_responses_only(",
        "    trainer,",
        "    instruction_part = '<|im_start|>user\\n',",
        "    response_part    = '<|im_start|>assistant\\n',",
        ")",
    ))

    cells.append(md("## 9. Train"))
    cells.append(code(
        "stats = trainer.train()",
        "print(stats)",
    ))

    cells.append(md(
        "## 10. Quick inference test",
        "Generate a config for a held-out-style prompt and check it parses as JSON.",
    ))
    cells.append(code(
        "FastLanguageModel.for_inference(model)",
        "",
        "test_user = build_user_content({",
        "    'schema': {",
        "        'columns': ['order_id','region','product','quantity','price','customer_email'],",
        "        'inferred_types': {'order_id':'integer','region':'string','product':'string',",
        "            'quantity':'integer','price':'double','customer_email':'string'},",
        "        'row_count': 5000, 'size_hint': 'small (< 5MB)',",
        "        'samples': [{'order_id':'101','region':'US','product':'Mouse',",
        "            'quantity':'3','price':'19.99','customer_email':'a@b.com'}],",
        "    },",
        "    'user_prompt': 'keep orders over 100 and average price per region',",
        "    'config': {'num_containers': 3, 'containers_to_create': ['raw','bronze','silver']},",
        "})",
        "",
        "msgs = [{'role':'system','content':SYSTEM_PROMPT},",
        "        {'role':'user','content':test_user}]",
        "inputs = tokenizer.apply_chat_template(msgs, add_generation_prompt=True,",
        "                                       return_tensors='pt').to('cuda')",
        "out = model.generate(input_ids=inputs, max_new_tokens=1024, temperature=0.2,",
        "                     top_p=0.8, do_sample=True)",
        "text = tokenizer.decode(out[0][inputs.shape[1]:], skip_special_tokens=True)",
        "print(text)",
        "import json as _j",
        "try:",
        "    _j.loads(text); print('\\nValid JSON ✔')",
        "except Exception as e:",
        "    print('\\nNOT valid JSON yet (more data/epochs needed):', e)",
    ))

    cells.append(md(
        "## 11. Save",
        "LoRA adapters are tiny (~80 MB). The merged 16-bit model is a standalone ~15 GB",
        "checkpoint you can serve with vLLM / transformers.",
    ))
    cells.append(code(
        "# LoRA adapters only",
        "model.save_pretrained(OUTPUT_LORA)",
        "tokenizer.save_pretrained(OUTPUT_LORA)",
        "print('Saved adapters ->', OUTPUT_LORA)",
        "",
        "# Optional: merged fp16 (uncomment; needs ~15 GB disk + RAM)",
        "# model.save_pretrained_merged(OUTPUT_MERGED, tokenizer, save_method='merged_16bit')",
        "",
        "# Optional: GGUF for llama.cpp / Ollama (uncomment; builds llama.cpp, slow)",
        "# model.save_pretrained_gguf('qwen_planner_gguf', tokenizer, quantization_method='q4_k_m')",
    ))

    cells.append(md(
        "## 12. Plug back into the planner",
        "",
        "Two options to use the fine-tuned model in `groq_planner`-style code:",
        "",
        "1. **Local serve (vLLM, OpenAI-compatible):**",
        "   ```bash",
        "   pip install vllm",
        "   vllm serve qwen_planner_merged --max-model-len 4096",
        "   ```",
        "   Then point the planner at `http://localhost:8000/v1/chat/completions`",
        "   (same request shape as the Groq call; set `model` to the served name).",
        "",
        "2. **Adapters + transformers** in-process: load `BASE_MODEL` + `PeftModel.from_pretrained`",
        "   on `OUTPUT_LORA` and call `apply_chat_template` with the SAME `SYSTEM_PROMPT`.",
        "",
        "Keep the **SYSTEM_PROMPT identical** at inference to how it was trained.",
    ))

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main():
    if "--verify" in sys.argv:
        verify()
        return
    nb = build_notebook()
    with open(NB_PATH, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1)
    # round-trip validate the emitted notebook is valid JSON
    with open(NB_PATH, encoding="utf-8") as f:
        json.load(f)
    print(f"Wrote {NB_PATH}  ({len(nb['cells'])} cells, valid JSON)")


if __name__ == "__main__":
    main()
