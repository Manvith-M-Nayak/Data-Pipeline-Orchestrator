"""Reproducible, offline cost-model audit; does not modify model or training data."""
import os
os.environ.setdefault('OMP_NUM_THREADS', '1')
import sys, json, io, csv, random, copy
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'unified'))
import numpy as np
import sklearn
from sklearn.metrics import mean_absolute_error, r2_score, balanced_accuracy_score
from cost_optimization_agent import CostMLPredictor, CostOptimizationAgent
from cost_optimization_agent.cost_optimizer import OptimizationSuggestion, CostBreakdown
from cost_optimization_agent.training.generate_cost_dataset import _emit_pipeline, CSV_COLS, _deadline_aware_optimal, _estimate_stage_duration_s
from cost_optimization_agent.ml.feature_spec import FEATURE_COLS, TARGET_COLS
from resource_agent.ml.feature_spec import stage_features

out = {'runtime': {'numpy': np.__version__, 'sklearn': sklearn.__version__}}
out['model_available'] = CostMLPredictor.is_available()
out['load_error'] = CostMLPredictor._load_error
if not out['model_available']:
    print(json.dumps(out, indent=2)); sys.exit(1)
b = CostMLPredictor._bundle
out['bundle'] = {k: b[k] for k in ('sklearn_version', 'trained_rows', 'feature_cols')}
buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=CSV_COLS)
writer.writeheader()
rng = random.Random(20260930)
count = 0
while count < 5000:
    count += _emit_pipeline(rng, writer)
buf.seek(0)
rows = list(csv.DictReader(buf))
X = np.array([[float(r[c]) for c in FEATURE_COLS] for r in rows])
out['fresh_synthetic_rows'] = len(rows)
out['fresh_metrics'] = {}
for target in TARGET_COLS:
    y = np.array([float(r[target]) for r in rows])
    p = b['regressors'][target].predict(X)
    out['fresh_metrics'][target] = {'mae': float(mean_absolute_error(y,p)), 'r2': float(r2_score(y,p)), 'rounded_accuracy': float(np.mean(np.round(y) == np.round(p)))}
out['node_balanced_accuracy'] = float(balanced_accuracy_score([r['opt_node_type'] for r in rows], b['node_classifier'].predict(X)))
agent = CostOptimizationAgent()
stage = {'name': 'transform', 'type': 'notebook', 'transformations': ['x = x + 1']}
schema = {'row_count': 1000000, 'columns': ['x'], 'size_hint': 'large'}
plan = {'stages': [stage], 'schema': schema, 'csv_size_bytes': 140000000, 'recommended_settings': {'node_type': 'Standard_D8s_v3'}}
rp = {'allocations': [{'stage_name': 'transform', 'stage_type': 'notebook', 'workers': 4, 'node_type': 'Standard_D8s_v3', 'memory_gb': 64, 'duration_s': 150}], 'peak_concurrent_workers': 4, 'estimated_total_s': 150}
perf = {'predicted_total_s': 150, 'adjustment_factor': 1}
out['prediction'] = CostMLPredictor.predict_optimal_config(stage, schema, 140000000)
out['tight_deadline_optimize'] = agent.optimize(plan, perf, rp, {'deadline_s': 151, 'priority': 'critical'})
original = copy.deepcopy(rp)
out['tight_deadline_applied'] = agent.apply_optimization(plan, perf, rp, {'deadline_s': 151, 'priority': 'critical'})
out['input_preserved'] = original == rp
feat = stage_features(stage, schema, 140000000)
out['labeler_duration_before'] = _estimate_stage_duration_s(4, 'Standard_D8s_v3', feat)
a = out['tight_deadline_applied']['allocations'][0]
out['labeler_duration_after'] = _estimate_stage_duration_s(a['workers'], a['node_type'], feat)
out['deadline_labels_same_features'] = {str(d): _deadline_aware_optimal(feat,d) for d in (60, 151, 300, 600)}
try:
    out['empty_plan'] = agent.optimize({}, {}, {})
except Exception as e:
    out['empty_plan_error'] = repr(e)
cost = CostBreakdown(1,1,0,0,2)
unsafe = OptimizationSuggestion('reduce cluster from 4 to 1 nodes', '~50%', 'slower', 'test', cost, 'low', source='rule')
out['rejected_suggestion_reintroduced'] = bool(agent._enforce_constraints([unsafe], {'deadline_s': 151}, perf))
cheaper = copy.deepcopy(rp)
cheaper['allocations'][0]['node_type'] = 'Standard_DS2_v2'
out['node_change_costs'] = [agent._estimate_cost(plan,perf,x).total_usd for x in (rp,cheaper)]
out['copy_only_cost'] = agent.optimize({'stages':[{'name':'copy','type':'copy'}]}, {'predicted_total_s':300}, {'allocations':[{'stage_name':'copy','stage_type':'copy','diu':8,'workers':0}]})['estimated_cost']
saved = (CostMLPredictor._bundle, CostMLPredictor._load_attempted, CostMLPredictor._load_error)
CostMLPredictor._bundle = None
CostMLPredictor._load_attempted = True
CostMLPredictor._load_error = 'audit: simulated unavailable model'
out['missing_model_fallback'] = agent.optimize(plan,perf,rp)['optimization_source']
CostMLPredictor._bundle, CostMLPredictor._load_attempted, CostMLPredictor._load_error = saved
Path('cost_model_audit_results.json').write_text(json.dumps(out, indent=2), encoding='utf-8')
print(json.dumps(out, indent=2))
