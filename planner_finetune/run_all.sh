#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Single entry point: setup -> prepare -> train -> evaluate.
# Re-runnable / idempotent. Apple MLX only.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

VENV="${VENV:-.venv}"

echo "######################################################################"
echo "# Planner Agent — MLX LoRA fine-tune  (setup -> prepare -> train -> eval)"
echo "######################################################################"

echo; echo ">>> [1/4] setup"
./setup.sh

# shellcheck disable=SC1091
source "$VENV/bin/activate"

echo; echo ">>> [2/4] prepare data"
python prepare_data.py

echo; echo ">>> [3/4] train"
./train.sh

echo; echo ">>> [4/4] evaluate (base vs fine-tuned)"
python evaluate.py

echo; echo ">>> Done."
