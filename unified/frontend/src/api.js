const BASE = "/api";

async function req(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, opts);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// ── Planner ─────────────────────────────────────────────────────────────────
export const planner = {
  plan: (schema, prompt) =>
    req("/planner/plan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ schema, prompt }),
    }),
};

// ── Executor ─────────────────────────────────────────────────────────────────
export const executor = {
  run: (csvFile, pipelineConfig, schema) => {
    const fd = new FormData();
    fd.append("csv_file", csvFile);
    fd.append("pipeline_config", JSON.stringify(pipelineConfig));
    fd.append("schema", JSON.stringify(schema));
    return req("/executor/run", { method: "POST", body: fd });
  },
  status:   (jobId) => req(`/executor/status/${jobId}`),
  listJobs: ()      => req("/executor/jobs"),
};

// ── Monitor ──────────────────────────────────────────────────────────────────
export const monitor = {
  getLiveRuns:      ()           => req("/monitor/pipelines/live"),
  getNames:         ()           => req("/monitor/pipelines/names"),
  sync:             (hours = 48) => req(`/monitor/pipelines/sync?hours=${hours}`, { method: "POST" }),
  getStats:         (name)       => req(`/monitor/pipelines/stats/${encodeURIComponent(name)}`),
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

export const health = () => req("/health");
