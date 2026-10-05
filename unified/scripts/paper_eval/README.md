# Paper evaluation scripts

Reproduce the numbers in [`docs/PAPER_RESULTS.md`](../../docs/PAPER_RESULTS.md).
Run from `unified/` with the project venv. `real_run_metrics.py`, `ablation_offline.py`,
`ablation_planner.py` and `offline_remainder.py` don't touch Azure and don't write to `data/`
or the agents' state files. `offline_remainder.py` points the monitor at a temporary database
before importing the detector, and deletes that directory when it finishes.
`live_benchmark.py` runs real pipelines: each run is recorded by the system like any other run
(feedback logs, monitor DB) and can trigger the learning agent (correction factors, retrain).

| Script | What it measures | Needs | Time |
|---|---|---|---|
| `real_run_metrics.py` | Every number computed from real runs: success rate, runtime-prediction error per predictor, cost error with vs without the learned correction, per-stage error with vs without the Resource correction, monitor history (demo rows excluded), local LLM latency | the logs in `data/` | seconds |
| `ablation_offline.py` | With vs without the **Resource Agent** (tier-limit violations and settings: model output vs planner after repair vs Resource Agent), the **Assurance gate** (fault injection through the real Manager validation → gate → executor pre-cloud checks; where each fault stops or what it would do in Databricks), and the **whole system** (what a person would write by hand) | nothing | ~2–4 min |
| `live_benchmark.py` | Live Azure runs through the running backend: `batch` (3 sizes × 2 shapes × 2 repeats), `parallel [size]` (sequential vs parallel execution groups), `streaming` (single vs multi-stage, 3 data drops). Saves every run's full state | backend running (`uvicorn main:app`), Azure credentials in `.env`; **costs real Azure money** (≈ $0.02–0.08 per run by the cost formula) | batch ~45 min, parallel ~15 min, streaming ~10 min |
| `offline_more.py` | Retry replay, fixed limit vs learned usual duration, live re-sizing replay, Learning-agent simulation (learning rates, regime change, aborted runs) — real agent code, temp files only | nothing | ~2 min |
| `perf_no_baseline.py` | Runtime model with vs without the `baseline_s` features: retrains in a temp folder (production models untouched) and scores both on saved live run states | saved states in `data/paper_eval/live*/states/` | ~4 min CPU |
| `groq_bare_eval.py` | Groq planner raw reply vs shipped (repaired), paired, N repeats; strict and fair (function-style aware) scoring; saves replies | `GROQ_API_KEY`; ~70,000 tokens per repeat (free tier: 200,000/day) | ~8–10 min per repeat |
| `ablation_planner.py` | Planner with vs without **fine-tuning**, the **repair layer**, **self-check**, and the **LLM** itself (deterministic default) on 24 prompts × 3 schemas | Ollama running with `planner-agent` and `qwen2.5:7b-instruct` | Apple M5: ~75–90 s per case without the self-check conditions (~15 min for 12 cases); 2–4 min per case with them. Heavy local load — don't run it alongside live planning |
| `offline_remainder.py` | Performance-gate thresholds on the synthetic held-out fold (needs the saved classifier pickle; records `not_run` and does not retrain when it is missing), anomaly precision/recall on a temporary database, filter-compiler fuzz, Resource ML vs heuristic on 1,000 dataset plans | the validated planner dataset on disk (`planner_agent/training/datasets/planner_config_dataset.jsonl`); the outcome-classifier pickle only for the threshold section. No Azure | ~15 min. The resource comparison is the slow part (~13 min). Set `PAPER_EVAL_ONLY` to `filter_fuzz`, `resource_ml`, `anomalies`, or `perf_gate` to run one section |
| `groq_planner_eval.py` | Shipped Groq planner on the same 24 prompts and intent regexes as `ablation_planner.py` | `GROQ_API_KEY` in `.env`. No Azure, no Ollama. Retries a free-tier 429 | ~8 min on the free tier |

```bash
python scripts/paper_eval/real_run_metrics.py
python scripts/paper_eval/ablation_offline.py  /tmp/offline_results.json
python scripts/paper_eval/offline_remainder.py /tmp/offline_remainder.json
python scripts/paper_eval/groq_planner_eval.py  /tmp/groq_planner_24.json
# one section only:
# $env:PAPER_EVAL_ONLY = "filter_fuzz"; python scripts/paper_eval/offline_remainder.py $env:TEMP\paper_fuzz.json
ollama serve &   # if not already running
python scripts/paper_eval/ablation_planner.py  /tmp/planner_results.json
python scripts/paper_eval/ablation_planner.py --summary /tmp/planner_results.json
# low-heat variant: resume at case 12 and skip the self-check conditions
PAPER_EVAL_START=12 PAPER_EVAL_SKIP_FULL=1 python scripts/paper_eval/ablation_planner.py /tmp/planner_results.json
# live (uses Azure):
python scripts/paper_eval/live_benchmark.py batch     /tmp/live
python scripts/paper_eval/live_benchmark.py parallel  /tmp/live_xs xs
python scripts/paper_eval/live_benchmark.py streaming /tmp/live
PAPER_EVAL_REPEATS=4 python scripts/paper_eval/live_benchmark.py batch /tmp/live   # with output checks
python scripts/paper_eval/live_benchmark.py pinned  /tmp/live    # Resource Agent settings vs planner's pinned
python scripts/paper_eval/live_benchmark.py recheck /tmp/live    # one (size, shape) again, output saved
python scripts/paper_eval/offline_more.py      /tmp/offline_more.json
python scripts/paper_eval/perf_no_baseline.py  /tmp/perf_no_baseline.json
python scripts/paper_eval/groq_bare_eval.py    /tmp/groq.json 1
```

The results used in `docs/PAPER_RESULTS.md` Part B are kept in `data/paper_eval/` (git-ignored).

Notes:
- `ablation_planner.py` samples at temperature 0.2, so repeated runs differ slightly.
  For the paper, run it 3 times and report mean ± std.
- When Ollama is started by the backend, its log is `data/ollama.log`; new calls there change
  the latency number from `real_run_metrics.py`. The doc's figure (280 calls) was taken before
  the Part B runs (during Part B, Ollama was started by hand and logged elsewhere).
- Seeds: `ablation_offline.py` and the resource section of `offline_remainder.py` sample
  1,000 dataset plans with `random.seed(20261003)`. The dataset generator's own seed for the
  5,000-row file is 20260628. Regenerating that file on a later commit is not guaranteed to
  match the 2026-10-03 file byte for byte; §B2 of the results doc says which numbers to cite.
