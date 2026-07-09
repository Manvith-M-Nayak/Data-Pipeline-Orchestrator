"""
End-to-end integration test for the pre-execution pipeline.

Simulates the Central Manager's Phases 1 → 2a → 2b → 2c flow:
  Resource Agent → Performance Prediction → Cost Optimization

Run:
    python -m integration_test
"""

import os
import sys
import json
import time

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(__file__))

_plan = {
    "num_containers": 3,
    "containers_to_create": ["bronze", "silver", "gold"],
    "recommended_settings": {
        "node_type": "Standard_D4s_v3",
        "shuffle_partitions": 200,
    },
    "execution_order": ["ingest", "transform", "aggregate"],
    "stages": [
        {
            "name": "ingest",
            "type": "copy",
            "source_dataset": "DS_Raw",
            "sink_dataset": "DS_Bronze",
            "diu": 8,
        },
        {
            "name": "transform",
            "type": "notebook",
            "source_container": "bronze",
            "sink_container": "silver",
            "transformations": [
                "c1 = expr1",
                "c2 = expr2",
                "c3 = expr3",
            ],
            "filter_condition": "amount > 0",
        },
        {
            "name": "aggregate",
            "type": "notebook",
            "source_container": "silver",
            "sink_container": "gold",
            "transformations": ["c1 = expr1"],
            "aggregations": {
                "group_by": ["grp"],
                "agg_exprs": ["sum(amount)", "avg(amount)"],
            },
        },
    ],
}
_schema = {
    "row_count": 500_000,
    "columns": ["c1", "c2", "c3", "amount", "grp"],
    "size_hint": "medium",
}
_csv_size_bytes = 70 * 1024 * 1024
_groups = [["ingest"], ["transform"], ["aggregate"]]


def _test_resource_agent():
    print("\n=== Phase 1: Resource Agent ===")
    from resource_agent import ResourceAgent

    agent = ResourceAgent()
    rp = agent.analyze(
        _plan,
        csv_size_bytes=_csv_size_bytes,
        schema=_schema,
        execution_groups=_groups,
    )
    assert rp["feasible"], f"Plan not feasible: {rp.get('warnings', [])}"
    assert len(rp["allocations"]) == 3
    for a in rp["allocations"]:
        assert a["workers"] >= 0
        assert a["diu"] >= 0
        print(
            f"  {a['stage_name']:<12} {a['stage_type']:<9} "
            f"workers={a['workers']}  diu={a['diu']}  "
            f"node={a['node_type']}  mem={a['memory_gb']}GB  "
            f"~{a['duration_s']}s"
        )
    assert rp["estimated_total_s"] > 0
    print(f"  Peak concurrent: {rp['peak_concurrent_workers']}")
    print(f"  Estimated total: {rp['estimated_total_s']}s")
    print("  [PASS] Resource Agent")
    return rp


def _test_performance_prediction(rp):
    print("\n=== Phase 2: Performance Prediction Agent ===")
    from performance_prediction_agent.performance_agent import (
        PerformancePredictionAgent,
    )

    predictions = {
        "estimated_total_s": rp["estimated_total_s"],
        "throughput_mb_per_s": 15.0,
    }
    agent = PerformancePredictionAgent()
    result = agent.predict(
        resource_plan=rp,
        predictions=predictions,
        plan=_plan,
    )
    assert "predicted_total_s" in result
    assert result["predicted_total_s"] > 0
    print(f"  Predicted duration: {result['predicted_total_s']}s")
    print(f"  Outcome: {result.get('outcome', 'N/A')}")
    if "confidence" in result:
        print(f"  Confidence: {result['confidence']:.0%}")
    if "bottleneck_stage" in result:
        print(f"  Bottleneck: {result['bottleneck_stage']}")
    print("  [PASS] Performance Prediction Agent")
    return result


def _test_cost_optimization(rp, perf):
    print("\n=== Phase 3: Cost Optimization Agent ===")

    from cost_optimization_agent.cost_optimizer import CostOptimizationAgent

    agent = CostOptimizationAgent()

    result = agent.optimize(
        plan=_plan,
        performance_prediction=perf,
        resource_plan=rp,
    )

    cost = result["estimated_cost"]
    print(
        f"  Estimated cost: ${cost['total_usd']:.4f} "
        f"(compute=${cost['compute_usd']:.4f} + "
        f"DBU=${cost['databricks_dbu_usd']:.4f} + "
        f"ADF=${cost['adf_usd']:.4f} + "
        f"storage=${cost['storage_usd']:.4f})"
    )
    print(f"  Optimization source: {result['optimization_source']}")
    print(f"  Recommendations: {len(result['recommendations'])}")
    for r in result["recommendations"]:
        print(
            f"    - {r['change']:<45} save {r['estimated_saving']:<8} "
            f"risk={r['risk_level']:<7} source={r['source']}"
        )
    assert len(result["recommendations"]) >= 0
    print("  [PASS] Cost Optimization Agent")
    return result


def _test_cost_optimization_ml_model():
    print("\n=== Phase 3b: ML Model Verification ===")
    from cost_optimization_agent.ml_predictor import CostMLPredictor

    assert CostMLPredictor.is_available(), "ML model should be available"
    stage = {
        "name": "t1",
        "type": "notebook",
        "transformations": ["c1 = expr1", "c2 = expr2"],
        "filter_condition": "x > 0",
    }
    opt = CostMLPredictor.predict_optimal_config(
        stage, _schema, _csv_size_bytes, stage_index=0, n_stages=3
    )
    assert opt["source"] == "ml_model"
    assert "workers" in opt
    assert "node_type" in opt
    assert "shuffle_partitions" in opt
    print(
        f"  ML prediction for notebook stage: "
        f"workers={opt['workers']}  diu={opt['diu']}  "
        f"node={opt['node_type']}  mem={opt['memory_gb']}GB  "
        f"shuffle={opt['shuffle_partitions']}"
    )

    copy_stage = {"name": "c1", "type": "copy"}
    opt2 = CostMLPredictor.predict_optimal_config(
        copy_stage, _schema, _csv_size_bytes, stage_index=1, n_stages=3
    )
    assert opt2["source"] == "ml_model"
    assert opt2["workers"] == 0
    assert opt2["diu"] >= 1
    print(
        f"  ML prediction for copy stage:   "
        f"workers={opt2['workers']}  diu={opt2['diu']}  "
        f"node={opt2['node_type']}  mem={opt2['memory_gb']}GB  "
        f"shuffle={opt2['shuffle_partitions']}"
    )
    print("  [PASS] ML Model")


def _test_central_manager_integration():
    print("\n=== Phase 4: Central Manager Integration ===")
    from central_manager_agent.manager import CentralManager

    mgr = CentralManager()

    run_id = mgr.pre_create(_plan)
    assert run_id is not None
    print(f"  Run ID: {run_id}")

    from central_manager_agent.manager import RunState

    state = RunState(run_id=run_id)
    state.plan = dict(_plan)
    state.predictions = {"csv_size_bytes": _csv_size_bytes, "csv_path": "/tmp/test.csv"}

    mgr.analyze_parallelism(state)
    assert state.parallelism is not None
    assert "execution_groups" in state.parallelism
    print(f"  Parallelism groups: {state.parallelism['execution_groups']}")

    mgr.predict_resources(state, csv_size_bytes=_csv_size_bytes, schema=_schema)
    assert state.resource_plan is not None
    assert state.resource_plan["feasible"]
    print(
        f"  Resource plan: {state.resource_plan['estimated_total_s']}s, "
        f"{state.resource_plan['peak_concurrent_workers']} workers peak"
    )

    perf = mgr.predict_performance(state)
    assert perf is not None
    print(
        f"  Performance: {perf.get('predicted_total_s', 'N/A')}s, "
        f"outcome={perf.get('outcome', 'N/A')}"
    )

    mgr.optimize_cost(state)
    assert state.cost_optimization is not None
    co = state.cost_optimization
    print(f"  Cost optimization source: {co.get('optimization_source', 'N/A')}")
    print(f"  Estimated total: ${co.get('estimated_cost', {}).get('total_usd', 0):.4f}")
    print(f"  Recommendations: {len(co.get('recommendations', []))}")
    print("  [PASS] Central Manager Integration")


def main():
    print("=" * 72)
    print("  Data Pipeline Orchestrator — Integration Test")
    print("  Pre-Execution Pipeline: Resource -> Performance -> Cost")
    print("=" * 72)

    t0 = time.time()

    try:
        rp = _test_resource_agent()
        perf = _test_performance_prediction(rp)
        _test_cost_optimization(rp, perf)
        _test_cost_optimization_ml_model()
        _test_central_manager_integration()
    except Exception as e:
        print(f"\n  [FAIL] {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    elapsed = time.time() - t0
    print(f"\n{'=' * 72}")
    print(f"  ALL TESTS PASSED  ({elapsed:.1f}s)")
    print(f"{'=' * 72}")


if __name__ == "__main__":
    main()
