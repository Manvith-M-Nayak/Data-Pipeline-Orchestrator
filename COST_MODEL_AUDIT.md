# Cost model audit — 2026-09-29

**Historical findings below describe the pre-fix model.** A subsequent safety rebuild added deadline features, corrected copy labels/pricing, excluded infeasible training cases, retrained on 30,000 rows, and made public optimization fail closed. All 12 tests in test_cost_model_safety.py pass. cost_model_audit_results.json now contains the post-fix audit on 5,000 independent synthetic examples (seed 20260930). Worker R² is 0.9616, DIU R² 0.9925, memory R² 0.9713, shuffle R² 0.9784 and node balanced accuracy 0.9939. The former deadline violation is rejected, empty plans return no recommendations, node prices affect cost, and copy-only plans no longer incur notebook charges. Real billing/production SLA validation remains outstanding.

Final safety verification: **13 regression tests passed** after adding a missing-workload guard. The broader pipeline integration test stopped at missing FastAPI in the isolated environment. No cloud execution was performed.

Original audit of the existing cost_models.pkl, before retraining or changing agent logic. Reproduce the current-version audit with `venv\Scripts\python.exe audit_cost_model.py`. Full current outputs are in cost_model_audit_results.json.

## Execution and accuracy

- Default Python (NumPy 1.26.4, sklearn 1.7.0) could not deserialize the bundle: PCG64 BitGenerator error. An isolated environment with NumPy 2.2.6 and sklearn 1.6.1 loaded it successfully. The requirements pin sklearn but allow NumPy 1.26, which was insufficient for this artifact in the tested environment.
- Bundle metadata: sklearn 1.6.1, 200,001 training rows, expected 16 features.
- Evaluated 5,002 fresh synthetic stages from the repository generator, seed 20260929. These are not real production workloads; accuracy measures agreement with the synthetic generator, not actual cost savings or SLA compliance.

| Target | Fresh MAE | Fresh R² |
|---|---:|---:|
| Workers | 0.395 | 0.239 |
| DIU | 0.220 | 0.341 |
| Memory GB | 4.216 | 0.749 |
| Shuffle partitions | 2.983 | 0.982 |

Node balanced accuracy: 98.96%. Worker accuracy after rounding both predictions and noisy labels: 74.81%; DIU: 85.97%. These are diagnostic metrics, not proof of feasible configurations. Simulated unavailable-model fallback returned heuristic mode; application preserved the original input object.

## Confirmed red flags

1. **High: deadline enforcement can be bypassed.** With one million rows, four workers, predicted runtime 150 seconds and a 151-second critical deadline, optimize recommended and apply_optimization applied one worker. The training generator's own duration estimator increased from 141.09 to 204.38 seconds. The recommendation claimed 75% savings and negligible runtime impact; stored duration remained 150 seconds. ML application does not receive constraints. Suggestion filtering relies on words such as `cluster` or `node` in display text, which the tested ML description did not contain.
2. **High: rejected suggestions are restored.** `_enforce_constraints` appends the first original suggestion when all suggestions fail its checks. A direct probe confirmed a deadline-rejected cluster reduction was returned anyway.
3. **High: deadlines are missing from model inputs.** The generator randomly selects a deadline but writes only the 16 workload features. For identical workload features, the labeler selected three workers at 151 seconds and one at 300 seconds. The predictor cannot distinguish these cases. The generator also labels infeasible deadlines with a cheapest configuration instead of recording infeasibility; its 120-second startup makes sampled 60/120-second notebook deadlines infeasible.
4. **High: savings are not recalculated against changed runtime and node allocation.** Reducing workers leaves duration unchanged; `_estimate_cost` reads node type from plan recommended settings instead of allocation node types. Changing only the allocation from D8s_v3 to DS2_v2 produced exactly the same estimated cost (0.230833). Copy-only plans were also charged notebook compute and DBUs (0.092083 for 300 seconds, besides the ADF fee).
5. **Medium: empty allocations crash when ML is available.** `optimize({}, {}, {})` raised `ValueError: max() iterable argument is empty` in the ML suggestion path.
6. **Medium: copy training cannot optimize DIU meaningfully.** Its labeler loops over DIU but passes zero to the duration function, uses constant copy cost, and never checks the copy deadline. It selects DIU 1 before adding noise.
7. **Medium: reported validation is misleading.** README claims perfect worker/DIU metrics, unlike both saved metrics and this audit. The trainer compares rounded worker/DIU predictions with unrounded Gaussian-noisy labels for its exact-match metric, explaining misleadingly low saved exact-match scores. Small train/test gaps do not demonstrate SLA safety.

## Recommended next steps

Fix constraint enforcement and fail closed when no feasible recommendation exists; recalculate candidate runtime/cost and validate resources before applying changes. Correct copy pricing/DIU labels and model feasibility labels, include deadline in the feature contract, and retrain. Pin a verified compatible runtime and update documentation/metrics. Add regression checks for the reproduced failures and evaluate with real telemetry before relying on automatic application.

No cloud jobs or billing experiments were run. Production logic and the model binary were left unchanged.
