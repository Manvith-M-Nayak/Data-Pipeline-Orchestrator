# Paper evaluation scripts

Reproduce the numbers in [`docs/PAPER_RESULTS.md`](../../docs/PAPER_RESULTS.md).
Run from `unified/` with the project venv. None of them touch Azure or Databricks,
and none write to `data/` or the agents' state files.

| Script | What it measures | Needs | Time |
|---|---|---|---|
| `real_run_metrics.py` | Every number computed from real runs: success rate, runtime-prediction error per predictor, cost error with vs without the learned correction, per-stage error with vs without the Resource correction, monitor history (demo rows excluded), local LLM latency | the logs in `data/` | seconds |
| `ablation_offline.py` | With vs without the **Resource Agent** (hard-limit violations, provisioned workers/DIU), the **Assurance gate** (fault injection: where each fault is caught), and the **whole system** (what a person would write by hand) | nothing | ~1–2 min |
| `ablation_planner.py` | Planner with vs without **fine-tuning**, the **repair layer**, **self-check**, and the **LLM** itself (deterministic default) on 24 prompts × 3 schemas | Ollama running with `planner-agent` and `qwen2.5:7b-instruct` | ~65 min on an Apple M5 |

```bash
python scripts/paper_eval/real_run_metrics.py
python scripts/paper_eval/ablation_offline.py  /tmp/offline_results.json
ollama serve &   # if not already running
python scripts/paper_eval/ablation_planner.py  /tmp/planner_results.json
```

Notes:
- `ablation_planner.py` samples at temperature 0.2, so repeated runs differ slightly.
  For the paper, run it 3 times and report mean ± std.
- It appends to `data/ollama.log` (Ollama's own log), which changes the latency number from
  `real_run_metrics.py`. The doc's latency figure was taken before these runs (280 calls).
- Seeds: `ablation_offline.py` uses `random.seed(20261003)`.
