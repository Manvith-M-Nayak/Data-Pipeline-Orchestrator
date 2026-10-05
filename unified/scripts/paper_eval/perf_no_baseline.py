"""Runtime model with vs without the Resource estimate features (§6.4).

`baseline_s` / `resource_estimate_s` are the Resource Agent's own duration
estimate, called a circular dependency in docs/RESPONSIBILITIES.md. This
retrains the Performance model without those two columns IN A TEMPORARY
FOLDER (the production models in performance_prediction_agent/models/ are not
touched) and scores both models on the saved live run states with the exact
inputs the Manager used (plan + schema + csv_size_bytes, see
CentralManager._agent_plan).

    python scripts/paper_eval/perf_no_baseline.py /tmp/perf_no_baseline.json [states_glob] [after_iso]

states_glob defaults to data/paper_eval/live*/states/*.json. Runs that started
after `after_iso` (default 2026-10-03T12:41, i.e. after the 18:10 IST
automatic retrain whose real-row export both models share) are out-of-sample
for both models.
"""
import glob
import json
import os
import shutil
import statistics as st
import subprocess
import sys
import tempfile
import warnings

warnings.filterwarnings("ignore")
_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

import joblib  # noqa: E402
import numpy as np  # noqa: E402

DROP = ["resource_estimate_s", "baseline_s"]


def train_without(tmp):
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    src = open("performance_prediction_agent/run_training.py", encoding="utf-8").read()
    old = '    "resource_estimate_s", "baseline_s",\n    "network_quality",'
    assert src.count(old) == 1, "FEATURE_COLS layout changed; update this script"
    open(os.path.join(tmp, "run_training.py"), "w").write(src.replace(old, '    "network_quality",'))
    shutil.copy("performance_prediction_agent/data/real_runs.csv", os.path.join(tmp, "data"))
    log = subprocess.run([sys.executable, "run_training.py"], cwd=tmp,
                         capture_output=True, text=True, check=True).stdout
    keep = [l.strip() for l in log.splitlines()
            if any(k in l for k in ("MAE:", "R2:", "Balanced accuracy", "CV balanced", "Spot check", "predicts"))]
    return os.path.join(tmp, "models"), keep


def main():
    out_path = os.path.join(_CALLER_CWD, sys.argv[1] if len(sys.argv) > 1 else "perf_no_baseline.json")
    states = sys.argv[2] if len(sys.argv) > 2 else "data/paper_eval/live*/states/*.json"
    after = sys.argv[3] if len(sys.argv) > 3 else "2026-10-03T12:41"
    from performance_prediction_agent.ml_predictor import MLPredictor

    tmp = tempfile.mkdtemp(prefix="perf_no_baseline_")
    model_dir, train_log = train_without(tmp)
    MLPredictor._ensure_loaded()
    prod_reg, prod_clf = MLPredictor._duration_model, MLPredictor._outcome_model
    new_reg = joblib.load(os.path.join(model_dir, "duration_regressor.pkl"))
    new_clf = joblib.load(os.path.join(model_dir, "outcome_classifier.pkl"))

    def p_fail(clf, x):
        return float(dict(zip(clf.classes_, clf.predict_proba(x)[0]))["failure"])

    rows = []
    for f in sorted(glob.glob(states)):
        d = json.load(open(f))
        rp = d.get("resource_plan") or {}
        if not rp.get("allocations"):
            continue
        plan = {**(d.get("plan") or {}), "schema": d.get("schema") or {},
                "csv_size_bytes": d.get("csv_size_bytes") or 0}
        x = MLPredictor._build_feature_row(rp, d.get("predictions") or {}, plan)
        xn = x.drop(columns=DROP)
        pp = d.get("performance_prediction") or {}
        rows.append({
            "run": d["run_id"][:8], "started_at": d["started_at"], "status": d["status"],
            "actual_s": d.get("execution_s"),
            "logged_outcome": pp.get("outcome"), "logged_confidence": pp.get("confidence"),
            "with_baseline_s": max(60, int(np.expm1(prod_reg.predict(x)[0]))),
            "without_baseline_s": max(60, int(np.expm1(new_reg.predict(xn)[0]))),
            "outcome_with": str(prod_clf.predict(x)[0]), "outcome_without": str(new_clf.predict(xn)[0]),
            "p_failure_with": round(p_fail(prod_clf, x), 3),
            "p_failure_without": round(p_fail(new_clf, xn), 3),
        })

    def summary(rs):
        ex = [r for r in rs if r["actual_s"]]
        if not ex:
            return None
        ape = lambda k: round(100 * st.mean(abs(r[k] - r["actual_s"]) / r["actual_s"] for r in ex), 1)
        return {"runs": len(rs), "executed": len(ex),
                "duration_mape_with_pct": ape("with_baseline_s"),
                "duration_mape_without_pct": ape("without_baseline_s"),
                "failure_verdicts_with": sum(r["outcome_with"] == "failure" for r in rs),
                "failure_verdicts_without": sum(r["outcome_without"] == "failure" for r in rs)}

    report = {
        "note": "raw model outputs (no learned correction factor); production models untouched",
        "training_without_baseline": train_log,
        "all_runs": summary(rows),
        "out_of_sample_for_both": summary([r for r in rows if r["started_at"] > after]),
        "verdicts_match_logged": sum(r["outcome_with"] == r["logged_outcome"] for r in rows),
        "runs": rows,
    }
    json.dump(report, open(out_path, "w"), indent=1)
    print(json.dumps({k: v for k, v in report.items() if k != "runs"}, indent=1))
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
