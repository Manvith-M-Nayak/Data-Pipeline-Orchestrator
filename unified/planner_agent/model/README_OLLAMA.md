# Planner Agent — Ollama build

Turn the fine-tuned LoRA adapter (`planner_agent_lora.zip`) into a local Ollama
model named `planner-agent`, used by the unified backend in place of the Groq
planner.

This uses the **adapter path**: the base model is pulled from the Ollama
registry (resumable) and the LoRA is converted to a small GGUF and applied on
load — no 15 GB HuggingFace download, no full merge.

## Contents
| File | Purpose |
|------|---------|
| `planner_agent_lora.zip` | LoRA adapter (Qwen2.5-7B-Instruct, from fine-tune) |
| `build_ollama_model.sh` | One-shot: unzip → LoRA→GGUF → pull base → `ollama create` |
| `Modelfile` | Ollama recipe (`FROM qwen2.5:7b-instruct` + `ADAPTER` + system prompt) |
| `planner finetune.ipynb` | Original training notebook (reference) |

## Prerequisites
- [Ollama](https://ollama.com) installed and running (`ollama serve` or `brew services start ollama`)
- Python with `torch`, `transformers`, `peft` (a throwaway venv is fine — these
  are only needed for the one-time LoRA→GGUF conversion, not to run the model)
- `git`

## Build
```bash
cd unified/planner_agent/model
bash build_ollama_model.sh
```
Pulls base `qwen2.5:7b-instruct` (~4.7 GB, resumable) and converts the adapter
to `planner-agent-lora.gguf` (~80 MB), then registers `planner-agent`. Re-runs
skip completed stages.

## Test
```bash
ollama run planner-agent '{"schema":{"columns":["emp_name","department","salary"],"inferred_types":{"emp_name":"string","department":"string","salary":"integer"},"row_count":980,"size_hint":"small (< 5MB)","samples":[{"emp_name":"Tom Becker","department":"HR","salary":"5809"}]},"user_prompt":"ingest data from raw into bronze; then in silver, keep rows where salary > 5000."}'
```
Expect a single compact JSON pipeline config (no prose).

The system prompt is baked into the `Modelfile`, so it does not need to be sent
at call time. The user message is the JSON `{"schema": {...}, "user_prompt": "..."}`.

## Notes
- Build artifacts (`adapter/`, `llama.cpp/`, `*.gguf`) are generated locally and
  should not be committed (see `.gitignore`).
- The base model lives in Ollama's store (`~/.ollama/models`), not in this repo.
- The `convert_lora_to_gguf.py` step may print a `torch==2.11.0` pip resolution
  error from llama.cpp's `requirements.txt`; it is harmless — conversion runs on
  whatever recent torch is already installed.
