# Paper evaluation scripts

Reproduce the numbers in [`docs/PAPER_RESULTS.md`](../../docs/PAPER_RESULTS.md).
Run from `unified/` with the project venv. `real_run_metrics.py`, `ablation_offline.py` and
`ablation_planner.py` don't touch Azure and don't write to `data/` or the agents' state files.
`live_benchmark.py` runs real pipelines: each run is recorded by the system like any other run
(feedback logs, monitor DB) and can trigger the learning agent (correction factors, retrain).

| Script | What it measures | Needs | Time |
|---|---|---|---|
| `real_run_metrics.py` | Every number computed from real runs: success rate, runtime-prediction error per predictor, cost error with vs without the learned correction, per-stage error with vs without the Resource correction, monitor history (demo rows excluded), local LLM latency | the logs in `data/` | seconds |
| `ablation_offline.py` | With vs without the **Resource Agent** (tier-limit violations and settings: model output vs planner after repair vs Resource Agent), the **Assurance gate** (fault injection through the real Manager validation → gate → executor pre-cloud checks; where each fault stops or what it would do in Databricks), and the **whole system** (what a person would write by hand) | nothing | ~2–4 min |
| `live_benchmark.py` | Live Azure runs through the running backend: `batch` (3 sizes × 2 shapes × 2 repeats), `parallel [size]` (sequential vs parallel execution groups), `streaming` (single vs multi-stage, 3 data drops). Saves every run's full state | backend running (`uvicorn main:app`), Azure credentials in `.env`; **costs real Azure money** (≈ $0.02–0.08 per run by the cost formula) | batch ~45 min, parallel ~15 min, streaming ~10 min |
| `ablation_planner.py` | Planner with vs without **fine-tuning**, the **repair layer**, **self-check**, and the **LLM** itself (deterministic default) on 24 prompts × 3 schemas | Ollama running with `planner-agent` and `qwen2.5:7b-instruct` | Apple M5: ~75–90 s per case without the self-check conditions (~15 min for 12 cases); 2–4 min per case with them. Heavy local load — don't run it alongside live planning |

```bash
python scripts/paper_eval/real_run_metrics.py
python scripts/paper_eval/ablation_offline.py  /tmp/offline_results.json
ollama serve &   # if not already running
python scripts/paper_eval/ablation_planner.py  /tmp/planner_results.json
python scripts/paper_eval/ablation_planner.py --summary /tmp/planner_results.json
# low-heat variant: resume at case 12 and skip the self-check conditions
PAPER_EVAL_START=12 PAPER_EVAL_SKIP_FULL=1 python scripts/paper_eval/ablation_planner.py /tmp/planner_results.json
# live (uses Azure):
python scripts/paper_eval/live_benchmark.py batch     /tmp/live
python scripts/paper_eval/live_benchmark.py parallel  /tmp/live_xs xs
python scripts/paper_eval/live_benchmark.py streaming /tmp/live
```

The results used in `docs/PAPER_RESULTS.md` Part B are kept in `data/paper_eval/` (git-ignored).

Notes:
- `ablation_planner.py` samples at temperature 0.2, so repeated runs differ slightly.
  For the paper, run it 3 times and report mean ± std.
- When Ollama is started by the backend, its log is `data/ollama.log`; new calls there change
  the latency number from `real_run_metrics.py`. The doc's figure (280 calls) was taken before
  the Part B runs (during Part B, Ollama was started by hand and logged elsewhere).
- Seeds: `ablation_offline.py` uses `random.seed(20261003)`.
