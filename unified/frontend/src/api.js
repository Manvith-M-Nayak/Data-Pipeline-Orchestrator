const BASE = "/api";

async function req(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, opts);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
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
  status:      (jobId)     => req(`/executor/status/${jobId}`),
  listJobs:    ()          => req("/executor/jobs"),
  downloadUrl: (container) => `${BASE}/executor/download/${encodeURIComponent(container)}`,
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
};

function _qs(params) {
  const q = new URLSearchParams(params).toString();
  return q ? "?" + q : "";
}

// ── WebSocket ────────────────────────────────────────────────────────────────
let _ws = null;
const _subs = new Set();

export function connectWS(onMessage) {
  _subs.add(onMessage);
  if (_ws && _ws.readyState === WebSocket.OPEN) return () => _subs.delete(onMessage);

  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  _ws = new WebSocket(`${proto}://${window.location.host}/ws/live`);

  _ws.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    _subs.forEach((fn) => fn(data));
  };

  _ws.onclose = () => {
    _ws = null;
    if (_subs.size > 0) setTimeout(() => connectWS(() => {}), 3000);
  };

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

// ── Learning & Policy Update Agent ──────────────────────────────────────────
export const learning = {
  status:  () => req("/learning/status"),
  metrics: () => req("/learning/metrics"),
  policies:() => req("/learning/policies"),
  log:     (limit = 100) => req(`/learning/log?limit=${limit}`),
  versions:() => req("/learning/versions"),
  resourceDrift: () => req("/learning/resource-drift"),
  cycle: (backgroundRetrain = true) =>
    req("/learning/cycle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ background_retrain: backgroundRetrain }),
    }),
  retrain: (sync = false) =>
    req("/learning/retrain", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sync }),
    }),
  rollback: (versionId) =>
    req("/learning/rollback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ version_id: versionId }),
    }),
};

export const health = () => req("/health");