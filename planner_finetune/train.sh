#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Train (LoRA via Apple MLX).
# Runs mlx_lm.lora with the 16 GB-safe config. If it OOMs, automatically retries
# with fewer LoRA layers and a shorter max sequence length. Loss is tee'd to a log.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

VENV="${VENV:-.venv}"
CONFIG="${CONFIG:-lora_config.yaml}"
DATA_DIR="${DATA_DIR:-mlx_data}"
ADAPTER="${ADAPTER:-planner_adapter}"
LOG="${LOG:-training.log}"

# Activate venv if present (run_all.sh / setup.sh create it).
if [[ -d "$VENV" ]]; then
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
fi

# Preconditions.
if [[ ! -f "$DATA_DIR/train.jsonl" ]]; then
  echo "ERROR: $DATA_DIR/train.jsonl missing. Run: python prepare_data.py" >&2
  exit 1
fi
if ! python -c "import mlx_lm" 2>/dev/null; then
  echo "ERROR: mlx_lm not importable. Run ./setup.sh first." >&2
  exit 1
fi

# run_lora <num_layers> <max_seq_length> : run training, tee to log, return rc.
run_lora() {
  local layers="$1" seq="$2"
  echo "==> Training: num_layers=$layers max_seq_length=$seq iters from $CONFIG"
  set +e
  python -m mlx_lm.lora \
    --config "$CONFIG" \
    --num-layers "$layers" \
    --max-seq-length "$seq" \
    --adapter-path "$ADAPTER" \
    2>&1 | tee "$LOG"
  local rc=${PIPESTATUS[0]}
  set -e
  return "$rc"
}

# OOM = nonzero exit AND a memory error in the log.
is_oom() {
  [[ -f "$LOG" ]] && grep -qiE "out of memory|insufficient memory|metal.*memory|failed to allocate" "$LOG"
}

echo "==> Stage 3: train (adapter -> $ADAPTER, log -> $LOG)"

if run_lora 8 2048; then
  echo "==> Training complete. Adapter: $ADAPTER  Log: $LOG"
  exit 0
fi

if is_oom; then
  echo "!! OOM detected — retrying with num_layers=4, max_seq_length=1024" >&2
  if run_lora 4 1024; then
    echo "==> Training complete on retry. Adapter: $ADAPTER  Log: $LOG"
    exit 0
  fi
fi

echo "ERROR: training failed. See $LOG" >&2
exit 1
