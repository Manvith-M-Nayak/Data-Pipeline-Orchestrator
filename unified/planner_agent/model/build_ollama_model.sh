#!/usr/bin/env bash
#
# Build the `planner-agent` Ollama model from the fine-tuned LoRA adapter.
#
# Adapter path (no 15GB base download, no merge):
#   1. unzip the LoRA adapter             -> ./adapter
#   2. clone llama.cpp                     -> ./llama.cpp
#   3. convert LoRA adapter to GGUF        -> ./planner-agent-lora.gguf
#   4. pull base model via Ollama          (qwen2.5:7b-instruct, resumable)
#   5. register base + adapter with Ollama -> model name `planner-agent`
#
# Run from this directory (any venv with `torch`, `transformers`, `peft`):
#   bash build_ollama_model.sh
#
# Requirements: python (torch, transformers, peft), git, ollama (running).
set -euo pipefail

cd "$(dirname "$0")"

ADAPTER_ZIP="planner_agent_lora.zip"
ADAPTER_DIR="./adapter"
LLAMA_CPP_DIR="./llama.cpp"
LORA_GGUF="./planner-agent-lora.gguf"
BASE_MODEL="qwen2.5:7b-instruct"
BASE_MODEL_ID="Qwen/Qwen2.5-7B-Instruct"   # config-only, for tensor metadata
OLLAMA_MODEL="planner-agent"

# 1. unzip adapter -----------------------------------------------------------
if [ ! -d "$ADAPTER_DIR" ]; then
    echo "==> Unzipping adapter"
    unzip -o "$ADAPTER_ZIP" -d "$ADAPTER_DIR"
fi

# 2. clone llama.cpp (for the LoRA->GGUF converter) --------------------------
if [ ! -d "$LLAMA_CPP_DIR" ]; then
    echo "==> Cloning llama.cpp"
    git clone --depth 1 https://github.com/ggml-org/llama.cpp "$LLAMA_CPP_DIR"
fi
# converter deps; the torch pin in this file may not resolve — ignore, an
# already-installed torch/transformers/peft is sufficient for conversion.
pip install -q -r "$LLAMA_CPP_DIR/requirements.txt" || true

# 3. convert LoRA adapter -> GGUF (downloads only the base config, ~KB) ------
if [ ! -f "$LORA_GGUF" ]; then
    echo "==> Converting LoRA adapter to GGUF"
    python "$LLAMA_CPP_DIR/convert_lora_to_gguf.py" "$ADAPTER_DIR" \
        --base-model-id "$BASE_MODEL_ID" --outtype f16 --outfile "$LORA_GGUF"
fi

# 4. pull the base model via Ollama (resumable) ------------------------------
echo "==> Pulling base model: $BASE_MODEL"
ollama pull "$BASE_MODEL"

# 5. register base + adapter with Ollama -------------------------------------
echo "==> Creating Ollama model: $OLLAMA_MODEL"
ollama create "$OLLAMA_MODEL" -f Modelfile

echo "Done. Test with:  ollama run $OLLAMA_MODEL"
