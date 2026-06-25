import React, { useEffect, useState, useCallback } from "react";
import { connectWS, monitor, executor } from "../api.js";
import { AlertTriangle, CheckCircle, Clock, XCircle, Zap } from "lucide-react";

const S = {
  title:  { fontSize: 22, fontWeight: 700, marginBottom: 4, color: "#f1f5f9" },
  sub:    { fontSize: 13, color: "#64748b", marginBottom: 24 },
  grid:   { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 16 },
  card:   { background: "#1e293b", borderRadius: 12, padding: 20, border: "1px solid #334155" },
  name:   { fontWeight: 700, fontSize: 15, marginBottom: 8, color: "#f1f5f9" },
  badge:  { display: "inline-block", padding: "2px 10px", borderRadius: 20, fontSize: 12, fontWeight: 600, background: "#1e3a5f", color: "#38bdf8" },
  meta:   { fontSize: 13, color: "#64748b", marginTop: 8, display: "flex", alignItems: "center", gap: 6 },
  anomaly:{ marginTop: 10, padding: "8px 12px", borderRadius: 8, background: "#451a03", color: "#fb923c", fontSize: 13, display: "flex", gap: 8 },
  empty:  { color: "#475569", textAlign: "center", marginTop: 60, fontSize: 15 },
  dot:    { width: 8, height: 8, borderRadius: "50%", background: "#22c55e", display: "inline-block", marginRight: 6 },
};

function fmtSec(s) {
  if (!s) return "0s";
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`;
}

export default function LiveDashboard() {
  const [runs,       setRuns]       = useState([]);
  const [execJobs,   setExecJobs]   = useState([]);
  const [ts,         setTs]         = useState(null);
  const [completed,  setCompleted]  = useState([]);
  const [cancelling, setCancelling] = useState({});

  async function handleCancel(runId) {
    setCancelling((p) => ({ ...p, [runId]: true }));
    try {
      await monitor.cancelRun(runId);
      setRuns((prev) => prev.filter((r) => r.runId !== runId));
      monitor.sync(1).catch(() => {});
    } catch (e) {
      alert("Cancel failed: " + e.message);
    } finally {
      setCancelling((p) => ({ ...p, [runId]: false }));
    }
  }

  const onWs = useCallback((data) => {
    if (data.event === "live_update") {
      setRuns(data.runs || []);
      setTs(new Date().toLocaleTimeString());
    }
    if (data.event === "run_completed") {
      setCompleted((prev) => [data, ...prev].slice(0, 5));
    }
  }, []);

  // Poll executor jobs every 5s for live Databricks run visibility
  useEffect(() => {
    function refreshJobs() {
      executor.listJobs().then((jobs) =>
        setExecJobs(jobs.filter((j) => j.status === "running"))
      ).catch(() => {});
    }
    refreshJobs();
    const t = setInterval(refreshJobs, 5000);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    monitor.getLiveRuns().then(setRuns).catch(() => {});
    return connectWS(onWs);
  }, [onWs]);

  return (
    <div>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 24 }}>
        <div>
          <h1 style={S.title}>Live Pipelines</h1>
          <p style={S.sub}>Polls ADF every 20 seconds — anomalies flagged automatically.</p>
        </div>
        {ts && <span style={{ fontSize: 12, color: "#475569" }}><span style={S.dot} />Updated {ts}</span>}
      </div>

      {completed.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          {completed.map((c) => (
            <div key={c.runId} style={{ background: "#0d2b0d", borderRadius: 8, padding: "10px 14px", marginBottom: 8, fontSize: 13, color: "#4ade80", display: "flex", gap: 10, alignItems: "center" }}>
              <CheckCircle size={14} />
              <strong>{c.pipelineName}</strong> completed — severity: {c.severity} · {c.summary}
            </div>
          ))}
        </div>
      )}

      {/* Executor / Databricks jobs currently running */}
      {execJobs.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, color: "#64748b", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 10 }}>
            Active executor jobs
          </div>
          <div style={S.grid}>
            {execJobs.map((j) => (
              <div key={j.job_id} style={{ ...S.card, borderColor: "#2d1b69" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: "#a78bfa", display: "flex", alignItems: "center", gap: 6 }}>
                    <Zap size={13} /> Databricks Pipeline
                  </div>
                  <span style={{ fontSize: 10, color: "#475569" }}>{j.job_id.slice(0, 8)}…</span>
                </div>
                <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 6 }}>{j.step || "Running…"}</div>
                {j.dbx_run_id && (
                  <div style={{ fontSize: 11, color: "#64748b" }}>DBX run_id: {j.dbx_run_id}</div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {runs.length === 0 && execJobs.length === 0 ? (
        <div style={S.empty}>
          <CheckCircle size={40} style={{ marginBottom: 12, color: "#334155" }} />
          <p>No pipelines currently running.</p>
        </div>
      ) : runs.length > 0 ? (
        <>
          <div style={{ fontSize: 12, color: "#64748b", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 10 }}>
            ADF pipeline runs
          </div>
          <div style={S.grid}>
            {runs.map((r) => (
              <div key={r.runId} style={S.card}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                  <div style={S.name}>{r.pipelineName}</div>
                  <button
                    onClick={() => handleCancel(r.runId)}
                    disabled={cancelling[r.runId]}
                    title="Cancel this run"
                    style={{ background: "none", border: "1px solid #7f1d1d", borderRadius: 6, color: "#f87171", cursor: "pointer", padding: "2px 8px", fontSize: 11, display: "flex", alignItems: "center", gap: 4 }}
                  >
                    <XCircle size={11} />{cancelling[r.runId] ? "…" : "Cancel"}
                  </button>
                </div>
                <span style={S.badge}>{r.status}</span>
                <div style={S.meta}><Clock size={13} />Running: {fmtSec(r.elapsedSec)}</div>
                <div style={{ fontSize: 11, color: "#475569", marginTop: 4 }}>{r.runId?.slice(0, 8)}…</div>
                {r.anomaly && (
                  <div style={S.anomaly}>
                    <AlertTriangle size={14} style={{ flexShrink: 0, marginTop: 1 }} />
                    {r.anomaly}
                  </div>
                )}
              </div>
            ))}
          </div>
        </>
      ) : null}
    </div>
  );
}
