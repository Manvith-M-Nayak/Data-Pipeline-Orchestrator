# Planner Agent — Ollama build

Turn the fine-tuned LoRA adapter (`planner_agent_lora.zip`) into a local Ollama
model named `planner-agent`, used by the unified backend in place of the Groq
planner.

## Contents
| File | Purpose |
|------|---------|
| `planner_agent_lora.zip` | LoRA adapter (Qwen2.5-7B-Instruct, from fine-tune) |
| `merge_adapter.py` | Merge adapter into FP16 base → standalone HF model |
| `build_ollama_model.sh` | One-shot: unzip → merge → GGUF → quantize → `ollama create` |
| `Modelfile` | Ollama recipe (base GGUF + training system prompt + params) |
| `planner finetune.ipynb` | Original training notebook (reference) |

## Prerequisites
- [Ollama](https://ollama.com) installed and running (`ollama serve`)
- Python with `torch`, `transformers`, `peft`
- `git`, `cmake` (to build llama.cpp's quantizer)
- ~30 GB free disk (base download + merged + GGUF artifacts)

## Build
```bash
cd unified/planner_agent/model
bash build_ollama_model.sh
```
First run downloads the base `Qwen/Qwen2.5-7B-Instruct` (~15 GB) and builds
`planner-agent.gguf` (Q4_K_M). Re-runs skip completed stages.

## Test
```bash
ollama run planner-agent '{"schema":{"columns":["emp_name","department","salary"],"inferred_types":{"emp_name":"string","department":"string","salary":"integer"},"row_count":980,"size_hint":"small (< 5MB)","samples":[{"emp_name":"Tom Becker","department":"HR","salary":"5809"}]},"user_prompt":"ingest data from raw into bronze; then in silver, keep rows where salary > 5000."}'
```
Expect a single compact JSON pipeline config (no prose).

The system prompt is baked into the `Modelfile`, so it does not need to be sent
at call time. The user message is the JSON `{"schema": {...}, "user_prompt": "..."}`.

## Notes
- Build artifacts (`adapter/`, `merged/`, `llama.cpp/`, `*.gguf`) are generated
  locally and should not be committed.
- Quantization is Q4_K_M for Mac/CPU. For higher fidelity on a GPU box, skip
  step 5 and point the `Modelfile` `FROM` at `planner-agent-f16.gguf`.
