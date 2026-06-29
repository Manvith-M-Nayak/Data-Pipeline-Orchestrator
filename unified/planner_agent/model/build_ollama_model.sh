#!/usr/bin/env bash
#
# Build the `planner-agent` Ollama model from the fine-tuned LoRA adapter.
#
# Pipeline:
#   1. unzip the LoRA adapter            -> ./adapter
#   2. merge adapter into FP16 base      -> ./merged   (merge_adapter.py)
#   3. convert merged HF model to GGUF   -> ./planner-agent-f16.gguf  (llama.cpp)
#   4. quantize to Q4_K_M                -> ./planner-agent.gguf
#   5. register with Ollama              -> model name `planner-agent`
#
# Run from this directory:
#   bash build_ollama_model.sh
#
# Requirements: python (peft, transformers, torch), git, cmake, ollama.
set -euo pipefail

cd "$(dirname "$0")"

ADAPTER_ZIP="planner_agent_lora.zip"
ADAPTER_DIR="./adapter"
MERGED_DIR="./merged"
LLAMA_CPP_DIR="./llama.cpp"
GGUF_F16="./planner-agent-f16.gguf"
GGUF_Q4="./planner-agent.gguf"
OLLAMA_MODEL="planner-agent"

# 1. unzip adapter -----------------------------------------------------------
if [ ! -d "$ADAPTER_DIR" ]; then
    echo "==> Unzipping adapter"
    unzip -o "$ADAPTER_ZIP" -d "$ADAPTER_DIR"
fi

# 2. merge adapter into full-precision base ----------------------------------
if [ ! -d "$MERGED_DIR" ]; then
    echo "==> Merging LoRA adapter into base (this downloads Qwen2.5-7B-Instruct ~15GB)"
    python merge_adapter.py --adapter "$ADAPTER_DIR" --out "$MERGED_DIR"
fi

# 3. clone + build llama.cpp converter ---------------------------------------
if [ ! -d "$LLAMA_CPP_DIR" ]; then
    echo "==> Cloning llama.cpp"
    git clone --depth 1 https://github.com/ggml-org/llama.cpp "$LLAMA_CPP_DIR"
fi
pip install -q -r "$LLAMA_CPP_DIR/requirements.txt"

# 4. convert HF -> GGUF (f16) ------------------------------------------------
if [ ! -f "$GGUF_F16" ]; then
    echo "==> Converting merged model to GGUF (f16)"
    python "$LLAMA_CPP_DIR/convert_hf_to_gguf.py" "$MERGED_DIR" \
        --outfile "$GGUF_F16" --outtype f16
fi

# 5. quantize f16 -> Q4_K_M --------------------------------------------------
if [ ! -f "$GGUF_Q4" ]; then
    echo "==> Building llama-quantize"
    cmake -S "$LLAMA_CPP_DIR" -B "$LLAMA_CPP_DIR/build" -DLLAMA_CURL=OFF >/dev/null
    cmake --build "$LLAMA_CPP_DIR/build" --target llama-quantize -j >/dev/null
    echo "==> Quantizing to Q4_K_M"
    "$LLAMA_CPP_DIR/build/bin/llama-quantize" "$GGUF_F16" "$GGUF_Q4" Q4_K_M
fi

# 6. register with Ollama ----------------------------------------------------
echo "==> Creating Ollama model: $OLLAMA_MODEL"
ollama create "$OLLAMA_MODEL" -f Modelfile

echo "Done. Test with:  ollama run $OLLAMA_MODEL"
