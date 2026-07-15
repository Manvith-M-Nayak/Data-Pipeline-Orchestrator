import React, { useEffect, useState, useCallback, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { monitor, connectWS } from "../api.js";
import {
  Activity, Brain, Zap, AlertTriangle, CheckCircle,
  Clock, ArrowRight, XCircle, RefreshCw,
} from "lucide-react";

const S = {
  page:    { maxWidth: 960, margin: "0 auto" },
  hero:    { marginBottom: 28, display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 12 },
  title:   { fontSize: 26, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:     { fontSize: 14, color: "#64748b" },
  grid2:   { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3:   { display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16, marginBottom: 16 },
  grid4:   { display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 16 },
  card:    { background: "#1e293b", borderRadius: 14, padding: 20, border: "1px solid #334155" },
  cardSm:  { background: "#1e293b", borderRadius: 14, padding: 16, border: "1px solid #334155" },
  hdr:     { display: "flex", alignItems: "center", gap: 8, marginBottom: 12 },
  hdrTxt:  { fontSize: 12, fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.5 },
  statVal: { fontSize: 34, fontWeight: 800, color: "#f1f5f9", lineHeight: 1, marginBottom: 3 },
  statSub: { fontSize: 12, color: "#64748b" },
  row:     { display: "flex", alignItems: "center", gap: 10, padding: "9px 0", borderBottom: "1px solid #1e293b" },
  rowLast: { display: "flex", alignItems: "center", gap: 10, padding: "9px 0" },
  lbl:     { fontSize: 13, color: "#cbd5e1", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  meta:    { fontSize: 11, color: "#475569", flexShrink: 0 },
  badge:   (status) => {
    const map = {
      Succeeded: { bg: "#14532d", color: "#4ade80" },
      Failed:    { bg: "#7f1d1d", color: "#f87171" },
      InProgress:{ bg: "#1e3a5f", color: "#38bdf8" },
    };
    const c = map[status] || { bg: "#1e293b", color: "#94a3b8" };
    return { padding: "2px 8px", borderRadius: 10, fontSize: 11, fontWeight: 600, background: c.bg, color: c.color, flexShrink: 0 };
  },
  severityBadge: (s) => {
    const map = { high: "#f97316", medium: "#f59e0b", low: "#22c55e" };
    return { fontSize: 11, color: map[s] || "#64748b", fontWeight: 600, flexShrink: 0 };
  },
  dot:     (on) => ({ width: 8, height: 8, borderRadius: "50%", flexShrink: 0, background: on ? "#22c55e" : "#334155" }),
  empty:   { fontSize: 13, color: "#475569", textAlign: "center", padding: "14px 0" },
  ctaBtn:  (primary) => ({
    padding: "8px 16px", borderRadius: 8, fontSize: 12, fontWeight: 600,
    cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 6,
    background: primary ? "#3b82f6" : "transparent",
    color:      primary ? "#fff"    : "#64748b",
    border:     primary ? "none"    : "1px solid #334155",
  }),
  planBox: { background: "#0f172a", borderRadius: 10, padding: 12, border: "1px solid #334155", marginTop: 8 },
  planStage: {
    display: "inline-block", padding: "2px 8px", borderRadius: 6,
    fontSize: 11, fontWeight: 600, background: "#1e293b", color: "#38bdf8",
    margin: "2px 2px 0 0",
  },
  noData: {
    background: "#0f172a", border: "1px dashed #334155", borderRadius: 10,
    padding: "12px 14px", fontSize: 12, color: "#475569", marginBottom: 8,
    display: "flex", alignItems: "center", gap: 8,
  },
  refreshBtn: {
    padding: "6px 12px", background: "transparent", color: "#475569",
    border: "1px solid #334155", borderRadius: 8, cursor: "pointer",
    fontSize: 12, display: "flex", alignItems: "center", gap: 5,
  },
  anomalyRow: {
    display: "flex", gap: 8, padding: "8px 0",
    borderBottom: "1px solid #1e293b", alignItems: "flex-start",
  },
};

function fmtSec(s) {
  if (!s) return "0s";
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`;
}
function fmtMs(ms) { return ms ? fmtSec(Math.round(ms / 1000)) : "—"; }

function readPlan() {
  try { return JSON.parse(localStorage.getItem("last_plan") || "null"); } catch { return null; }
}

export default function HomePage() {
  const navigate = useNavigate();
  const [summary,   setSummary]   = useState(null);
  const [liveRuns,  setLiveRuns]  = useState([]);
  const [wsOk,      setWsOk]      = useState(false);
  const [loading,   setLoading]   = useState(true);
  const [savedPlan, setSavedPlan] = useState(readPlan);
  const refreshRef = useRef();

  async function loadSummary() {
    try {
      const [s, live] = await Promise.all([
        monitor.getSummary(),
        monitor.getLiveRuns(),
      ]);
      setSummary(s);
      setLiveRuns(live);
    } catch {}
    finally { setLoading(false); }
  }

  useEffect(() => {
    loadSummary();
    // auto-refresh every 30s
    refreshRef.current = setInterval(loadSummary, 30000);
    return () => clearInterval(refreshRef.current);
  }, []);

  // Sync savedPlan when localStorage changes (e.g. after returning from PlannerTab)
  useEffect(() => {
    function onStorage(e) {
      if (e.key === "last_plan") setSavedPlan(readPlan());
    }
    window.addEventListener("storage", onStorage);
    // Also check on focus (same-tab navigation doesn't fire storage event)
    function onFocus() { setSavedPlan(readPlan()); }
    window.addEventListener("focus", onFocus);
    return () => { window.removeEventListener("storage", onStorage); window.removeEventListener("focus", onFocus); };
  }, []);

  const onWs = useCallback((data) => {
    setWsOk(true);
    if (data.event === "live_update") {
      setLiveRuns(data.runs || []);
    }
    if (data.event === "run_completed") {
      // Refresh summary so counts stay accurate
      monitor.getSummary().then(setSummary).catch(() => {});
    }
  }, []);

  useEffect(() => connectWS(onWs), [onWs]);

  const noData = !loading && (!summary || summary.total_runs === 0);

  return (
    <div style={S.page}>
      {/* Header */}
      <div style={S.hero}>
        <div>
          <h1 style={S.title}>Pipeline Orchestrator</h1>
          <p style={S.sub}>Live insights across all three agents.</p>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 6 }}>
          <span style={{ fontSize: 11, color: wsOk ? "#22c55e" : "#475569", display: "flex", alignItems: "center", gap: 5 }}>
            <div style={S.dot(wsOk)} /> {wsOk ? "Live" : "Connecting…"}
          </span>
          <button style={S.refreshBtn} onClick={loadSummary}>
            <RefreshCw size={12} /> Refresh
          </button>
        </div>
      </div>

      {noData && (
        <div style={S.noData}>
          <AlertTriangle size={14} color="#f59e0b" />
          No pipeline data yet. Click <strong style={{ color: "#38bdf8", cursor: "pointer", margin: "0 4px" }} onClick={() => monitor.sync(48).then(loadSummary)}>Sync (48h)</strong> in the header to pull recent ADF runs, or run your first pipeline.
        </div>
      )}

      {/* Stat cards */}
      <div style={S.grid4}>
        <div style={S.cardSm}>
          <div style={S.hdr}><Activity size={14} color="#38bdf8" /><span style={S.hdrTxt}>Active Now</span></div>
          <div style={S.statVal}>{liveRuns.length}</div>
          <div style={S.statSub}>pipelines running in ADF</div>
        </div>
        <div style={S.cardSm}>
          <div style={S.hdr}><CheckCircle size={14} color="#22c55e" /><span style={S.hdrTxt}>Succeeded</span></div>
          <div style={S.statVal}>{summary?.succeeded ?? "—"}</div>
          <div style={S.statSub}>of {summary?.total_runs ?? 0} total runs</div>
        </div>
        <div style={S.cardSm}>
          <div style={S.hdr}><XCircle size={14} color="#f87171" /><span style={S.hdrTxt}>Failed</span></div>
          <div style={{ ...S.statVal, color: (summary?.failed ?? 0) > 0 ? "#f87171" : "#f1f5f9" }}>
            {summary?.failed ?? "—"}
          </div>
          <div style={S.statSub}>pipeline failures</div>
        </div>
        <div style={S.cardSm}>
          <div style={S.hdr}><AlertTriangle size={14} color="#f97316" /><span style={S.hdrTxt}>Anomalies</span></div>
          <div style={{ ...S.statVal, color: (summary?.anomaly_count ?? 0) > 0 ? "#f97316" : "#f1f5f9" }}>
            {summary?.anomaly_count ?? "—"}
          </div>
          <div style={S.statSub}>failures + stuck pipelines</div>
        </div>
      </div>

      <div style={S.grid2}>
        {/* Monitor Agent — live + recent anomalies */}
        <div style={S.card}>
          <div style={S.hdr}>
            <Activity size={14} color="#38bdf8" />
            <span style={S.hdrTxt}>Monitor Agent</span>
          </div>

          {/* Live runs */}
          <div style={{ fontSize: 11, color: "#475569", marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>Live</div>
          {liveRuns.length === 0 ? (
            <div style={S.empty}>No pipelines running right now.</div>
          ) : liveRuns.map((r, i) => (
            <div key={r.runId} style={i < liveRuns.length - 1 ? S.row : S.rowLast}>
              <div style={S.dot(true)} />
              <span style={S.lbl}>{r.pipelineName}</span>
              <span style={S.meta}><Clock size={10} style={{ verticalAlign: "middle", marginRight: 2 }} />{fmtSec(r.elapsedSec)}</span>
              {r.anomaly && <AlertTriangle size={12} color="#f97316" />}
            </div>
          ))}

          {/* Recent anomalies from anomaly_log */}
          {(summary?.recent_anomalies?.length ?? 0) > 0 && (
            <>
              <div style={{ fontSize: 11, color: "#f97316", marginTop: 14, marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5, fontWeight: 700 }}>
                Anomaly Log
              </div>
              {summary.recent_anomalies.map((a, i) => (
                <div key={a.id} style={{ ...S.anomalyRow, borderBottom: i < summary.recent_anomalies.length - 1 ? "1px solid #1e293b" : "none" }}>
                  <AlertTriangle size={12} color="#f97316" style={{ flexShrink: 0, marginTop: 1 }} />
                  <div>
                    <div style={{ fontSize: 12, color: "#cbd5e1", fontWeight: 600 }}>{a.pipeline_name}</div>
                    <div style={{ fontSize: 11, color: "#64748b" }}>{(a.groq_verdict || "").slice(0, 90)}{(a.groq_verdict || "").length > 90 ? "…" : ""}</div>
                  </div>
                </div>
              ))}
            </>
          )}

          {/* Recent failed runs */}
          {(summary?.recent_failed?.length ?? 0) > 0 && (
            <>
              <div style={{ fontSize: 11, color: "#f87171", marginTop: 14, marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5, fontWeight: 700 }}>
                Recent Failures
              </div>
              {summary.recent_failed.map((r, i) => (
                <div key={r.run_id} style={{ ...S.row, borderBottom: i < summary.recent_failed.length - 1 ? "1px solid #1e293b" : "none" }}>
                  <XCircle size={12} color="#f87171" style={{ flexShrink: 0 }} />
                  <span style={S.lbl}>{r.pipeline_name}</span>
                  <span style={S.meta}>{fmtMs(r.duration_ms)}</span>
                  {r.severity && <span style={S.severityBadge(r.severity)}>{r.severity}</span>}
                </div>
              ))}
            </>
          )}

          <button onClick={() => navigate("/monitor")} style={{ ...S.ctaBtn(false), marginTop: 14 }}>
            Open Monitor Agent <ArrowRight size={12} />
          </button>
        </div>

        {/* Recent runs */}
        <div style={S.card}>
          <div style={S.hdr}><Clock size={14} color="#94a3b8" /><span style={S.hdrTxt}>Recent Runs</span></div>

          {(summary?.recent_runs?.length ?? 0) === 0 ? (
            <div style={S.empty}>No run history. Sync or run a pipeline.</div>
          ) : summary.recent_runs.map((r, i) => (
            <div key={r.run_id} style={i < summary.recent_runs.length - 1 ? S.row : S.rowLast}>
              <span style={S.lbl}>{r.pipeline_name}</span>
              <span style={S.meta}>{fmtMs(r.duration_ms)}</span>
              <span style={S.badge(r.status)}>{r.status}</span>
            </div>
          ))}

          {(summary?.recent_runs?.length ?? 0) > 0 && (
            <div style={{ fontSize: 12, color: "#334155", marginTop: 10 }}>
              AI analysis available for completed runs — open Run Logs in Monitor Agent.
            </div>
          )}

          <button onClick={() => navigate("/monitor")} style={{ ...S.ctaBtn(false), marginTop: 14 }}>
            View all logs <ArrowRight size={12} />
          </button>
        </div>
      </div>

      {/* Planner + Executor */}
      <div style={S.grid2}>
        <div style={S.card}>
          <div style={S.hdr}><Brain size={14} color="#a78bfa" /><span style={S.hdrTxt}>Planner Agent</span></div>
          {savedPlan ? (
            <>
              <div style={{ fontSize: 12, color: "#64748b", marginBottom: 6 }}>Last generated plan</div>
              <div style={S.planBox}>
                <div style={{ fontSize: 13, color: "#f1f5f9", fontWeight: 600, marginBottom: 6 }}>
                  {savedPlan.config?.stages?.length || 0} stage(s)
                  {savedPlan.used_fallback && <span style={{ marginLeft: 8, fontSize: 11, color: "#f59e0b" }}>(fallback)</span>}
                </div>
                <div>{(savedPlan.config?.stages || []).map((s, i) => <span key={i} style={S.planStage}>{s.name}</span>)}</div>
                {savedPlan.config?.reasoning && (
                  <div style={{ fontSize: 12, color: "#64748b", marginTop: 8 }}>
                    {savedPlan.config.reasoning.slice(0, 110)}{savedPlan.config.reasoning.length > 110 ? "…" : ""}
                  </div>
                )}
              </div>
              <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                <button onClick={() => navigate("/planner")} style={S.ctaBtn(false)}>New plan <ArrowRight size={11} /></button>
                <button onClick={() => navigate("/manager")} style={S.ctaBtn(true)}><Zap size={12} /> Run this plan</button>
              </div>
            </>
          ) : (
            <>
              <div style={S.empty}>No plan yet.</div>
              <button onClick={() => navigate("/planner")} style={S.ctaBtn(true)}><Brain size={12} /> Create pipeline plan</button>
            </>
          )}
        </div>

        <div style={S.card}>
          <div style={S.hdr}><Zap size={14} color="#f59e0b" /><span style={S.hdrTxt}>Executor Agent</span></div>
          {savedPlan ? (
            <>
              <div style={{ fontSize: 12, color: "#64748b", marginBottom: 6 }}>Plan ready to execute</div>
              <div style={S.planBox}>
                <div style={{ fontSize: 13, color: "#4ade80", display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
                  <CheckCircle size={13} /> Plan loaded from Planner Agent
                </div>
                <div style={{ fontSize: 12, color: "#64748b" }}>
                  {savedPlan.config?.stages?.length || 0} stages
                  {savedPlan.config?.execution_groups?.some((g) => g.length > 1) && (
                    <span style={{ color: "#a78bfa" }}> (parallel ⚡)</span>
                  )} ·
                  cluster: {savedPlan.config?.recommended_settings?.node_type || "auto"} ·
                  workers: {savedPlan.config?.recommended_settings?.num_workers ?? "auto"} ·
                  DIU: {savedPlan.config?.recommended_settings?.diu ?? "auto"}
                </div>
              </div>
              <button onClick={() => navigate("/manager")} style={{ ...S.ctaBtn(true), marginTop: 12 }}>
                <Zap size={12} /> Run pipeline now
              </button>
            </>
          ) : (
            <>
              <div style={S.empty}>No plan loaded. Generate one first.</div>
              <button onClick={() => navigate("/planner")} style={S.ctaBtn(false)}>Go to Planner <ArrowRight size={11} /></button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
