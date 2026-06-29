#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Setup
# Creates a Python venv, installs the MLX training stack, and HARD-FAILS unless
# the Metal GPU is usable. Apple Silicon only. Idempotent: re-running is safe.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

VENV="${VENV:-.venv}"
PYTHON="${PYTHON:-python3}"

echo "==> Stage 1: setup (cwd=$HERE)"

# 0. Platform guard — MLX needs macOS on Apple Silicon.
OS="$(uname -s)"
ARCH="$(uname -m)"
if [[ "$OS" != "Darwin" || "$ARCH" != "arm64" ]]; then
  echo "ERROR: MLX requires macOS on Apple Silicon (got $OS/$ARCH)." >&2
  exit 1
fi

# 1. venv (reuse if present).
if [[ ! -d "$VENV" ]]; then
  echo "==> Creating venv at $VENV"
  "$PYTHON" -m venv "$VENV"
else
  echo "==> Reusing existing venv at $VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# 2. Install pinned deps.
echo "==> Installing requirements"
python -m pip install --upgrade pip >/dev/null
python -m pip install -r requirements.txt

# 3. Report MLX version + confirm Metal GPU. Fail loudly if not usable.
echo "==> Verifying MLX + Metal GPU"
python - <<'PY'
import sys
try:
    import mlx.core as mx
    import mlx_lm
except Exception as e:
    print(f"ERROR: MLX import failed: {e}", file=sys.stderr)
    sys.exit(1)

print(f"mlx     : {mx.__version__}")
print(f"mlx_lm  : {getattr(mlx_lm, '__version__', 'unknown')}")

# A real Metal device must be the default, and a tiny GPU op must succeed.
dev = mx.default_device()
print(f"device  : {dev}")
if dev.type != mx.DeviceType.gpu:
    print("ERROR: Metal GPU is not the default MLX device.", file=sys.stderr)
    sys.exit(1)

a = mx.ones((1024, 1024))
b = (a @ a)
mx.eval(b)              # force the GPU to actually run
assert float(b[0, 0]) == 1024.0, "GPU matmul produced wrong result"
print("gpu op  : OK (1024x1024 matmul on Metal)")
PY

echo "==> Setup complete. Activate with:  source $VENV/bin/activate"
