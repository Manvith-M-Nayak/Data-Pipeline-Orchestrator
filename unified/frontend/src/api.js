const BASE = "/api";

// Sent as the x-api-key header when the backend has auth enabled (API_KEY set).
const API_KEY = import.meta.env.VITE_API_KEY || "";

async function req(path, opts = {}) {
  const headers = { ...(opts.headers || {}) };
  if (API_KEY) headers["x-api-key"] = API_KEY;
  const res = await fetch(`${BASE}${path}`, { ...opts, headers });
  if (!res.ok) {
    // FastAPI puts the real reason in the JSON `detail` body — surface it
    // instead of the generic "422 Unprocessable Entity" status text.
    let detail = "";
    try {
      const body = await res.json();
      detail = typeof body?.detail === "string"
        ? body.detail
        : body?.detail ? JSON.stringify(body.detail) : "";
    } catch {
      /* non-JSON error body — fall back to status text */
    }
    throw new Error(detail || `${res.status} ${res.statusText}`);
  }
  return res.json();
}

// ── Schema detection ─────────────────────────────────────────────────────────
export const schema = {
  detect: (csvFile) => {
    const fd = new FormData();
    fd.append("csv_file", csvFile);
    return req("/schema/detect", { method: "POST", body: fd });
  },
};

// ── Planner ─────────────────────────────────────────────────────────────────
export const planner = {
  // opts: { num_containers, custom_settings, container_names } — all optional
  plan: (schemaObj, prompt, opts = {}) =>
    req("/planner/plan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ schema: schemaObj, prompt, ...opts }),
    }),
};

// ── Assurance ────────────────────────────────────────────────────────────────
export const assurance = {
  // Validates a generated plan: structural checks (deterministic) + semantic
  // intent check (local LLM). block_on_intent left false — semantic is advisory.
  validate: (request, plan, schemaObj, runSemantic = true) =>
    req("/assurance/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request, plan, schema: schemaObj, run_semantic: runSemantic }),
    }),
};

// ── Executor ─────────────────────────────────────────────────────────────────
// No direct run API — pipeline runs go through the Central Manager
// (manager.run below), which invokes the executor after its pre-checks.
export const executor = {
  // fetch + blob instead of a plain <a href>: an anchor cannot send x-api-key.
  download: async (container) => {
    const headers = API_KEY ? { "x-api-key": API_KEY } : {};
    const res = await fetch(`${BASE}/executor/download/${encodeURIComponent(container)}`, { headers });
    if (!res.ok) throw new Error(`Download failed: ${res.status} ${res.statusText}`);
    const name = /filename="([^"]+)"/.exec(res.headers.get("Content-Disposition") || "")?.[1]
      || `${container}-output`;
    const url = URL.createObjectURL(await res.blob());
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  },
};

// ── Resource Agent ────────────────────────────────────────────────────────────
export const resource = {
  analyze: (plan, csvSizeBytes = 0, schema = null, executionGroups = null) =>
    req("/resource/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        plan,
        csv_size_bytes: csvSizeBytes,
        schema,
        execution_groups: executionGroups,
      }),
    }),
  reallocate: (liveRuns, allocations, elapsedS = 0) =>
    req("/resource/reallocate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ live_runs: liveRuns, allocations, elapsed_s: elapsedS }),
    }),
  feedback: (body) =>
    req("/resource/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  accuracy:          () => req("/resource/accuracy"),
  correctionFactors: () => req("/resource/correction-factors"),
  limits:            () => req("/resource/limits"),
  modelInfo:         () => req("/resource/model-info"),
};
export const perfPrediction = {
  predict: (resourcePlan, predictions, plan, slaTargetS = 900) =>
    req("/performance-prediction/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        resource_plan: resourcePlan,
        predictions,
        plan,
        sla_target_s: slaTargetS,
      }),
    }),
  history: () => req("/performance-prediction/history"),
};

// ── Central Manager ──────────────────────────────────────────────────────────
export const manager = {
  run: (csvFile, pipelineConfig, schemaObj, userRequest = "") => {
    const fd = new FormData();
    fd.append("csv_file", csvFile);
    fd.append("pipeline_config", JSON.stringify(pipelineConfig));
    fd.append("schema", JSON.stringify(schemaObj));
    fd.append("user_request", userRequest);
    return req("/manager/run", { method: "POST", body: fd });
  },
  status:   (runId)  => req(`/manager/status/${runId}`),
  listRuns: ()       => req("/manager/runs"),
  feedback: ()       => req("/manager/feedback"),
  // Combined results & logs endpoints
  combinedRun:  (runId) => req(`/manager/combined/run/${runId}`),
  analytics:    (limit = 200) => req(`/manager/combined/analytics?limit=${limit}`),
};

// ── Streaming console ─────────────────────────────────────────────────────────
// Live incremental streaming: start a stream, drop data → it processes now,
// poll status + output for results.
export const stream = {
  start: (config, schemaObj, fileFormat = "csv", intervalS = 0) =>
    req("/manager/stream/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ config, schema: schemaObj, file_format: fileFormat, interval_s: intervalS }),
    }),
  addData: (streamId, file) => {
    const fd = new FormData();
    fd.append("csv_file", file);
    return req(`/manager/stream/${streamId}/data`, { method: "POST", body: fd });
  },
  tick:   (streamId) => req(`/manager/stream/${streamId}/tick`, { method: "POST" }),
  stop:   (streamId) => req(`/manager/stream/${streamId}/stop`, { method: "POST" }),
  get:    (streamId) => req(`/manager/stream/${streamId}`),
  output: (streamId, limit = 200) => req(`/manager/stream/${streamId}/output?limit=${limit}`),
};

// ── Monitor ──────────────────────────────────────────────────────────────────
export const monitor = {
  getLiveRuns:      ()           => req("/monitor/pipelines/live"),
  getNames:         ()           => req("/monitor/pipelines/names"),
  sync:             (hours = 48) => req(`/monitor/pipelines/sync?hours=${hours}`, { method: "POST" }),
  cancelRun:        (runId)      => req(`/monitor/pipelines/cancel/${runId}`, { method: "POST" }),
  getStats:         (name)       => req(`/monitor/pipelines/stats/${encodeURIComponent(name)}`),
  getSummary:       ()           => req("/monitor/pipelines/summary"),
  getLogs:          (p = {})     => req(`/monitor/logs/${_qs(p)}`),
  getAnomalyLogs:   ()           => req("/monitor/logs/anomalies"),
  getPrediction:    (name)       => req(`/monitor/predictions/${encodeURIComponent(name)}`),
  getAnomalies:     ()           => req("/monitor/anomalies/"),
  // Classified events from anomaly_detector.py (failure, slow_runtime, …)
  getAnomalyEvents: (kind = "", limit = 200) =>
    req(`/monitor/anomalies/events?limit=${limit}${kind ? `&kind=${encodeURIComponent(kind)}` : ""}`),
};

function _qs(params) {
  const q = new URLSearchParams(params).toString();
  return q ? "?" + q : "";
}

// ── WebSocket ────────────────────────────────────────────────────────────────
let _ws = null;
const _subs = new Set();

// Opens the socket (idempotent) and wires reconnect. Kept separate from
// connectWS so reconnect attempts never register a subscriber — the previous
// `connectWS(() => {})` reconnect leaked one permanent empty subscriber per
// reconnect, growing _subs unbounded while the server was down.
function _openSocket() {
  if (_ws && (_ws.readyState === WebSocket.OPEN || _ws.readyState === WebSocket.CONNECTING)) {
    return;
  }
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  // Browsers can't set headers on a WebSocket, so the key goes in the query.
  const qs = API_KEY ? `?api_key=${encodeURIComponent(API_KEY)}` : "";
  _ws = new WebSocket(`${proto}://${window.location.host}/ws/live${qs}`);

  _ws.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    _subs.forEach((fn) => fn(data));
  };

  _ws.onclose = () => {
    _ws = null;
    if (_subs.size > 0) setTimeout(_openSocket, 3000);
  };
}

export function connectWS(onMessage) {
  _subs.add(onMessage);
  _openSocket();
  return () => _subs.delete(onMessage);
}

// ── Cost Optimization Agent ────────────────────────────────────────────────────
export const cost = {
  optimize: (plan, performancePrediction, resourcePlan, constraints = {}) =>
    req("/cost-optimization/optimize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        plan,
        performance_prediction: performancePrediction,
        resource_plan: resourcePlan,
        constraints,
      }),
    }),
  estimate: (plan, performancePrediction, resourcePlan) =>
    req("/cost-optimization/estimate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        plan,
        performance_prediction: performancePrediction,
        resource_plan: resourcePlan,
      }),
    }),
  nodeRates: () => req("/cost-optimization/node-rates"),
};

export const health = () => req("/health");