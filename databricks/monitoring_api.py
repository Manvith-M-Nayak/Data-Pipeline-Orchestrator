"""
monitoring_api.py
=================
Flask REST API for the Monitoring Agent.

Endpoints:
    GET  /health                        → agent health check
    GET  /metrics                       → full metrics snapshot
    GET  /pipeline/<id>/status          → per-pipeline status + jobs + anomalies
    GET  /anomalies                     → all detected anomalies
    GET  /anomalies?severity=critical   → filtered anomalies
    GET  /events                        → recent events from the event bus
    GET  /resources                     → recent resource history
    GET  /planner/context               → data for the Planner Agent
    GET  /optimizer/context             → data for the Optimizer Agent
    GET  /executor/alerts               → real-time alerts for the Executor Agent
    POST /pipeline/start                → notify pipeline started
    POST /pipeline/complete             → notify pipeline completed
    POST /pipeline/fail                 → notify pipeline failed
    POST /job/start                     → notify job started
    POST /job/succeed                   → notify job succeeded
    POST /job/fail                      → notify job failed
    POST /resource/scaled               → notify resource scale event

Run standalone:
    python monitoring_api.py
    # API available at http://localhost:5001
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from flask import Flask, jsonify, request, abort
from monitoring_agent import monitoring_agent

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


# ── Start the background collector when the API boots ─────────────────────────
monitoring_agent.start()


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "agent": "MonitoringAgent",
        "collector_running": monitoring_agent._running,
    })


# ══════════════════════════════════════════════════════════════════════════════
# METRICS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/metrics", methods=["GET"])
def get_metrics():
    """Full metrics snapshot — system, pipelines, jobs, resources, costs."""
    return jsonify(monitoring_agent.get_metrics())


@app.route("/resources", methods=["GET"])
def get_resources():
    """Recent resource time-series."""
    last_n = int(request.args.get("last_n", 60))
    return jsonify(monitoring_agent.get_resource_history(last_n=last_n))


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/pipeline/<pipeline_id>/status", methods=["GET"])
def get_pipeline_status(pipeline_id):
    """Status for a specific pipeline including jobs, anomalies, cost."""
    result = monitoring_agent.get_pipeline_status(pipeline_id)
    if "error" in result:
        abort(404, description=result["error"])
    return jsonify(result)


@app.route("/pipeline/start", methods=["POST"])
def pipeline_start():
    data = _require_json("pipeline_id", "pipeline_name")
    monitoring_agent.on_pipeline_started(
        pipeline_id=data["pipeline_id"],
        pipeline_name=data["pipeline_name"],
    )
    return jsonify({"ok": True, "event": "pipeline_started"})


@app.route("/pipeline/complete", methods=["POST"])
def pipeline_complete():
    data = _require_json("pipeline_id")
    monitoring_agent.on_pipeline_completed(
        pipeline_id=data["pipeline_id"],
        total_records=data.get("total_records", 0),
    )
    return jsonify({"ok": True, "event": "pipeline_completed"})


@app.route("/pipeline/fail", methods=["POST"])
def pipeline_fail():
    data = _require_json("pipeline_id")
    monitoring_agent.on_pipeline_failed(
        pipeline_id=data["pipeline_id"],
        reason=data.get("reason", ""),
    )
    return jsonify({"ok": True, "event": "pipeline_failed"})


# ══════════════════════════════════════════════════════════════════════════════
# JOBS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/job/start", methods=["POST"])
def job_start():
    data = _require_json("run_id", "job_id", "pipeline_id", "job_name")
    monitoring_agent.on_job_started(
        run_id=str(data["run_id"]),
        job_id=str(data["job_id"]),
        pipeline_id=data["pipeline_id"],
        job_name=data["job_name"],
        queue_time_s=data.get("queue_time_s", 0.0),
    )
    return jsonify({"ok": True, "event": "job_started"})


@app.route("/job/succeed", methods=["POST"])
def job_succeed():
    data = _require_json("run_id")
    monitoring_agent.on_job_succeeded(
        run_id=str(data["run_id"]),
        records_processed=data.get("records_processed", 0),
    )
    return jsonify({"ok": True, "event": "job_succeeded"})


@app.route("/job/fail", methods=["POST"])
def job_fail():
    data = _require_json("run_id")
    monitoring_agent.on_job_failed(
        run_id=str(data["run_id"]),
        failure_reason=data.get("failure_reason", ""),
        retry_count=data.get("retry_count", 0),
    )
    return jsonify({"ok": True, "event": "job_failed"})


# ══════════════════════════════════════════════════════════════════════════════
# RESOURCE SCALE
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/resource/scaled", methods=["POST"])
def resource_scaled():
    data = _require_json("direction", "resource_type")
    monitoring_agent.on_resource_scaled(
        direction=data["direction"],
        resource_type=data["resource_type"],
        pipeline_id=data.get("pipeline_id"),
    )
    return jsonify({"ok": True, "event": f"resource_scaled_{data['direction']}"})


# ══════════════════════════════════════════════════════════════════════════════
# ANOMALIES & EVENTS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/anomalies", methods=["GET"])
def get_anomalies():
    """
    Return detected anomalies.
    Query params:
        severity=warning|critical
        limit=N (default 50)
    """
    severity = request.args.get("severity")
    limit = int(request.args.get("limit", 50))
    return jsonify(monitoring_agent.get_anomalies(severity=severity, limit=limit))


@app.route("/events", methods=["GET"])
def get_events():
    limit = int(request.args.get("limit", 50))
    return jsonify(monitoring_agent.get_recent_events(limit=limit))


# ══════════════════════════════════════════════════════════════════════════════
# INTER-AGENT INTERFACES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/planner/context", methods=["GET"])
def planner_context():
    """Feed historical + resource data to the Planner Agent."""
    return jsonify(monitoring_agent.get_planner_context())


@app.route("/optimizer/context", methods=["GET"])
def optimizer_context():
    """Feed inefficiency + cost data to the Optimizer Agent."""
    return jsonify(monitoring_agent.get_optimizer_context())


@app.route("/executor/alerts", methods=["GET"])
def executor_alerts():
    """Return real-time actionable alerts for the Execution Agent."""
    return jsonify(monitoring_agent.get_executor_alerts())


# ══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": "bad_request", "message": str(e)}), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "not_found", "message": str(e)}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "internal_error", "message": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _require_json(*fields):
    """Parse request JSON and validate required fields."""
    if not request.is_json:
        abort(400, description="Request must be JSON")
    data = request.get_json()
    missing = [f for f in fields if f not in data]
    if missing:
        abort(400, description=f"Missing required fields: {missing}")
    return data


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("MONITORING_PORT", 5001))
    print(f"\n{'='*60}")
    print(f"  Monitoring Agent API")
    print(f"  http://localhost:{port}")
    print(f"{'='*60}")
    print(f"  GET  /metrics")
    print(f"  GET  /pipeline/<id>/status")
    print(f"  GET  /anomalies")
    print(f"  GET  /planner/context")
    print(f"  GET  /optimizer/context")
    print(f"  GET  /executor/alerts")
    print(f"{'='*60}\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)