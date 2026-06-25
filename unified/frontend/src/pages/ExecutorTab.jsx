import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { executor, connectWS } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import {
  Zap, Upload, CheckCircle, XCircle, RotateCcw, Brain, AlertTriangle, Activity, Clock, Download,
} from "lucide-react";

const C = {
  page:   { maxWidth: 760, margin: "0 auto" },
  header: { marginBottom: 28 },
  agent:  { display: "flex", alignItems: "center", gap: 10, marginBottom: 6 },
  agentBadge: {
    padding: "4px 12px", background: "#451a03", border: "1px solid #92400e",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "#f59e0b",
    display: "flex", alignItems: "center", gap: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:    { fontSize: 13, color: "#64748b" },
  card:   { background: "#1e293b", borderRadius: 14, padding: 24, border: "1px solid #334155", marginBottom: 16 },
  cardHdr:{ fontSize: 15, fontWeight: 700, color: "#f1f5f9", marginBottom: 4, display: "flex", alignItems: "center", gap: 8 },
  cardSub:{ fontSize: 13, color: "#64748b", marginBottom: 18 },
  drop:   (active, hasFile) => ({
    border: `2px dashed ${hasFile ? "#22c55e" : active ? "#3b82f6" : "#334155"}`,
    borderRadius: 12, padding: "24px 20px", textAlign: "center", cursor: "pointer",
    background: active ? "#0f172a" : "transparent", transition: "all 0.2s",
  }),
  btnRow: { display: "flex", gap: 10, marginTop: 18, alignItems: "center", flexWrap: "wrap" },
  btnPrimary: (disabled) => ({
    padding: "10px 22px", background: disabled ? "#1e293b" : "#f59e0b",
    color: disabled ? "#475569" : "#0f172a", border: "none", borderRadius: 10,
    cursor: disabled ? "not-allowed" : "pointer", fontSize: 13, fontWeight: 700,
    display: "inline-flex", alignItems: "center", gap: 7,
  }),
  btnSecondary: {
    padding: "10px 18px", background: "transparent", color: "#64748b",
    border: "1px solid #334155", borderRadius: 10, cursor: "pointer",
    fontSize: 13, display: "inline-flex", alignItems: "center", gap: 6,
  },
  planBox: {
    background: "#0f172a", borderRadius: 10, padding: 14,
    border: "1px solid #334155",
  },
  planStage: {
    display: "inline-block", padding: "3px 10px", borderRadius: 8,
    fontSize: 11, fontWeight: 600, background: "#1e293b", color: "#38bdf8",
    margin: "3px 3px 0 0",
  },
  execStep: (state) => ({
    display: "flex", alignItems: "center", gap: 12, padding: "9px 0",
    borderBottom: "1px solid #1e293b",
    opacity: state === "pending" ? 0.3 : 1, transition: "opacity 0.3s",
  }),
  execDot: (state) => ({
    width: 10, height: 10, borderRadius: "50%", flexShrink: 0,
    background: state === "done" ? "#22c55e" : state === "running" ? "#f59e0b" : "#334155",
    boxShadow: state === "running" ? "0 0 0 3px rgba(245,158,11,0.2)" : "none",
    transition: "all 0.3s",
  }),
  execLabel: (state) => ({
    fontSize: 13, flex: 1,
    color: state === "done" ? "#4ade80" : state === "running" ? "#fbbf24" : "#475569",
    fontWeight: state === "running" ? 600 : 400,
  }),
  resultBox: (ok) => ({
    background: ok ? "#0d2b0d" : "#2d0808", borderRadius: 10, padding: 16,
    border: `1px solid ${ok ? "#166534" : "#7f1d1d"}`, marginTop: 14,
  }),
  errBox: {
    background: "#450a0a", borderRadius: 8, padding: "10px 14px", marginBottom: 14,
    color: "#f87171", fontSize: 13, display: "flex", gap: 8,
  },
  monitorEvent: {
    fontSize: 12, color: "#64748b", padding: "6px 10px",
    borderBottom: "1px solid #1e293b", display: "flex", gap: 10,
  },
};

// Step labels are now driven by backend progress (jobState.step).
// These are fallback labels shown when no backend step is available yet.
const EXEC_STEPS = [
  "Authenticating with Azure",
  "Creating storage containers",
  "Uploading your data",
  "Uploading notebooks to Databricks",
  "Running copy pipeline (ADF)",
  "Running notebook stage (Databricks)",
  "Complete",
];

function Spinner({ color = "#f59e0b" }) {
  return (
    <span style={{
      display: "inline-block", width: 13, height: 13,
      border: "2px solid #334155", borderTopColor: color,
      borderRadius: "50%", animation: "spin 0.7s linear infinite",
    }} />
  );
}

export default function ExecutorTab() {
  const navigate = useNavigate();
  const {
    csvFile, setCsvFile,
    planResult:      savedPlan,
    executorJobId:   jobId,    setExecutorJobId:    setJobId,
    executorJobState:jobState, setExecutorJobState: setJobState,
    executorStep:    execStep, setExecutorStep:     setExecStep,
  } = useAppContext();

  const savedSchema = (() => { try { return JSON.parse(localStorage.getItem("last_csv_schema") || "null"); } catch { return null; } })();

  const [dragging,  setDragging]  = useState(false);
  const [running,   setRunning]   = useState(false);
  const [error,     setError]     = useState("");
  const [monEvents, setMonEvents] = useState([]);
  const fileRef      = useRef();
  const pollRef = useRef();

  function _handleStaleJob() {
    clearInterval(pollRef.current);
    setRunning(false);
    setJobId(null);
    setJobState(null);
    setExecStep(-1);
    setError("Session expired — server was restarted. Click Run Pipeline to start again.");
  }

  // Resume polling if job was running when user switched tabs
  useEffect(() => {
    if (!jobId || jobState?.status !== "running") return;
    setRunning(true);
    pollRef.current = setInterval(async () => {
      try {
        const s = await executor.status(jobId);
        setJobState(s);
        if (s.status !== "running") {
          clearInterval(pollRef.current);
          setRunning(false);
          setExecStep(EXEC_STEPS.length - 1);
          monitor.sync(2).catch(() => {});
        }
      } catch (e) {
        if (e.message && (e.message.startsWith("410") || e.message.startsWith("404"))) {
          _handleStaleJob();
        }
      }
    }, 3000);
    return () => clearInterval(pollRef.current);
  }, []); // only on mount

  // Monitor WS
  const onWs = useCallback((data) => {
    if (data.event === "live_update" || data.event === "run_completed") {
      setMonEvents((prev) => [{ ts: new Date().toLocaleTimeString(), ...data }, ...prev].slice(0, 10));
    }
  }, []);
  useEffect(() => connectWS(onWs), [onWs]);

  function onDrop(e) {
    e.preventDefault(); setDragging(false);
    const f = e.dataTransfer.files[0];
    setCsvFile(f);
  }

  async function handleRun() {
    if (!csvFile || !savedPlan) return;
    setError(""); setRunning(true); setExecStep(0); setJobState(null);

    try {
      const res = await executor.run(csvFile, savedPlan.config, savedSchema || {});
      setJobId(res.job_id);
      setJobState({ status: "running", step: "Starting…" });
    } catch (e) {
      setRunning(false);
      setError("Failed to start: " + e.message);
    }
  }

  useEffect(() => {
    if (!jobId || jobState?.status !== "running") return;
    pollRef.current = setInterval(async () => {
      try {
        const s = await executor.status(jobId);
        setJobState(s);
        if (s.step) {
          const idx = EXEC_STEPS.findIndex((label) =>
            s.step.toLowerCase().includes(label.split(" ")[0].toLowerCase())
          );
          if (idx >= 0) setExecStep(idx);
        }
        if (s.status !== "running") {
          clearInterval(pollRef.current);
          setRunning(false);
          setExecStep(EXEC_STEPS.length - 1);
          // Pull this run into the monitor DB immediately
          monitor.sync(2).catch(() => {});
        }
      } catch (e) {
        if (e.message && (e.message.startsWith("410") || e.message.startsWith("404"))) {
          _handleStaleJob();
        }
      }
    }, 3000);
    return () => clearInterval(pollRef.current);
  }, [jobId]); // eslint-disable-line

  function reset() {
    // Keep csvFile — user likely wants to run the same file again
    setRunning(false); setJobId(null);
    setJobState(null); setExecStep(-1); setError(""); setMonEvents([]);
  }

  const canRun = !!csvFile && !!savedPlan && !running;

  return (
    <div style={C.page}>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>

      <div style={C.header}>
        <div style={C.agent}>
          <span style={C.agentBadge}><Zap size={13} /> Executor Agent</span>
        </div>
        <h1 style={C.title}>Run your pipeline</h1>
        <p style={C.sub}>Deploys your plan to Azure Data Factory and Databricks, then monitors execution.</p>
      </div>

      {error && (
        <div style={C.errBox}><XCircle size={14} style={{ flexShrink: 0 }} />{error}</div>
      )}

      {/* Plan loaded from planner */}
      <div style={C.card}>
        <div style={C.cardHdr}><Brain size={16} color="#a78bfa" />Pipeline Plan</div>
        {savedPlan ? (
          <>
            <div style={C.planBox}>
              <div style={{ fontSize: 13, color: "#4ade80", fontWeight: 600, marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
                <CheckCircle size={13} /> Plan loaded from Planner Agent
              </div>
              <div style={{ marginBottom: 6 }}>
                {(savedPlan.config?.stages || []).map((s, i) => (
                  <span key={i} style={C.planStage}>{s.name}</span>
                ))}
              </div>
              <div style={{ fontSize: 12, color: "#64748b" }}>
                Cluster: {savedPlan.config?.recommended_settings?.node_type || "auto"} ·
                Workers: {savedPlan.config?.recommended_settings?.num_workers ?? "auto"}
                {savedPlan.used_fallback && <span style={{ marginLeft: 8, color: "#f59e0b" }}>(fallback config)</span>}
              </div>
            </div>
            <button onClick={() => navigate("/planner")} style={{ ...C.btnSecondary, marginTop: 12, fontSize: 12 }}>
              ← Create different plan
            </button>
          </>
        ) : (
          <div style={{ textAlign: "center", padding: "20px 0" }}>
            <div style={{ fontSize: 13, color: "#64748b", marginBottom: 14 }}>No plan loaded — generate one in the Planner Agent first.</div>
            <button onClick={() => navigate("/planner")} style={C.btnPrimary(false)}>
              <Brain size={13} /> Go to Planner
            </button>
          </div>
        )}
      </div>

      {/* CSV upload */}
      {savedPlan && (
        <div style={C.card}>
          <div style={C.cardHdr}><Upload size={16} color="#38bdf8" />CSV File</div>
          <input ref={fileRef} type="file" accept=".csv" hidden onChange={(e) => { const f = e.target.files[0]; setCsvFile(f); }} />

          {csvFile ? (
            /* File already loaded — from Planner or previous upload */
            <div style={{ background: "#0f172a", borderRadius: 10, padding: "12px 16px", border: "1px solid #166534", display: "flex", alignItems: "center", gap: 10 }}>
              <CheckCircle size={16} color="#4ade80" style={{ flexShrink: 0 }} />
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, color: "#4ade80", fontWeight: 600 }}>{csvFile.name}</div>
                <div style={{ fontSize: 12, color: "#64748b", marginTop: 2 }}>
                  {"Carried over from Planner Agent — no re-upload needed."}
                </div>
              </div>
              <button
                onClick={() => !running && fileRef.current.click()}
                style={{ fontSize: 12, color: "#475569", background: "none", border: "1px solid #334155", borderRadius: 6, padding: "4px 10px", cursor: "pointer", flexShrink: 0 }}
              >
                Change
              </button>
            </div>
          ) : (
            /* No file yet */
            <div
              style={C.drop(dragging, false)}
              onClick={() => !running && fileRef.current.click()}
              onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
              onDragLeave={() => setDragging(false)}
              onDrop={onDrop}
            >
              <Upload size={28} color="#334155" style={{ marginBottom: 8 }} />
              <div style={{ fontSize: 13, color: "#64748b" }}>Click or drag your CSV here</div>
            </div>
          )}
        </div>
      )}

      {/* Run + execution progress */}
      {savedPlan && (
        <div style={C.card}>
          <div style={C.cardHdr}><Zap size={16} color="#f59e0b" />Execution</div>

          {!running && !jobState && (
            <>
              <div style={{ fontSize: 13, color: "#64748b", marginBottom: 16 }}>
                {csvFile ? "Ready to run. Click below to deploy and trigger the pipeline." : "Upload a CSV file above to continue."}
              </div>
              <button style={C.btnPrimary(!canRun)} disabled={!canRun} onClick={handleRun}>
                <Zap size={14} /> Run Pipeline
              </button>
            </>
          )}

          {(running || jobState) && (
            <>
              <div style={{ marginBottom: 14 }}>
                {EXEC_STEPS.map((label, i) => {
                  const failed = jobState?.status === "failed";
                  const isLast = i === EXEC_STEPS.length - 1;
                  let state;
                  if (failed) {
                    if (isLast) state = "pending";
                    else state = execStep > i ? "done" : execStep === i ? "running" : "pending";
                  } else {
                    state = execStep > i ? "done" : execStep === i ? "running" : "pending";
                  }
                  // Show live backend step name on the currently-running row
                  const liveLabel = (state === "running" && jobState?.step) ? jobState.step : label;
                  return (
                    <div key={i} style={C.execStep(state)}>
                      <div style={{
                        ...C.execDot(state),
                        background: failed && execStep === i ? "#f87171" : undefined,
                      }} />
                      <span style={C.execLabel(state)}>{liveLabel}</span>
                      {state === "running" && !failed && <Spinner />}
                      {state === "done"    && <CheckCircle size={13} color="#22c55e" />}
                      {failed && execStep === i && <XCircle size={13} color="#f87171" />}
                    </div>
                  );
                })}
              </div>

              {/* Result */}
              {jobState && jobState.status !== "running" && (
                <div style={C.resultBox(jobState.status === "completed")}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                    <div style={{ fontSize: 15, fontWeight: 700,
                      color: jobState.status === "completed" ? "#4ade80" : "#f87171" }}>
                      {jobState.status === "completed" ? "Pipeline completed successfully!" : "Pipeline failed"}
                    </div>
                    {jobState.status === "completed" && (jobState.result?.sink_container) && (
                      <a
                        href={`http://localhost:8000/api/executor/download/${encodeURIComponent(jobState.result.sink_container)}`}
                        download
                        style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 14px",
                          background: "#0ea5e9", color: "#fff", borderRadius: 8, fontSize: 12,
                          fontWeight: 600, textDecoration: "none" }}
                      >
                        <Download size={13} /> Download output CSV
                      </a>
                    )}
                  </div>
                  {jobState.error && (
                    <pre style={{ fontSize: 12, color: "#f87171", whiteSpace: "pre-wrap", wordBreak: "break-word", marginBottom: 8 }}>
                      {jobState.error}
                    </pre>
                  )}
                  {jobState.result?.result?.message && (
                    <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 8, padding: "8px 10px", background: "#0f172a", borderRadius: 6 }}>
                      ADF: {jobState.result.result.message}
                    </div>
                  )}
                  {jobState.result && (
                    <details style={{ marginTop: 8 }}>
                      <summary style={{ cursor: "pointer", fontSize: 12, color: "#4ade80" }}>Run details</summary>
                      <pre style={{ marginTop: 8, fontSize: 11, color: "#94a3b8", overflow: "auto", maxHeight: 180 }}>
                        {JSON.stringify(jobState.result, null, 2)}
                      </pre>
                    </details>
                  )}
                </div>
              )}
            </>
          )}

          {jobState && (
            <div style={C.btnRow}>
              <button style={C.btnPrimary(false)} onClick={reset}><RotateCcw size={13} /> Run again</button>
            </div>
          )}
        </div>
      )}

      {/* Monitor Agent live feed */}
      {(running || jobState) && (
        <div style={C.card}>
          <div style={C.cardHdr}><Activity size={16} color="#38bdf8" />Monitor Agent — Live Feed</div>
          <div style={{ fontSize: 13, color: "#64748b", marginBottom: 12 }}>
            ADF events received via WebSocket. Anomalies are flagged automatically.
          </div>
          {monEvents.length === 0 ? (
            <div style={{ fontSize: 13, color: "#334155", textAlign: "center", padding: "12px 0" }}>
              Waiting for ADF events… (appears once ADF picks up the triggered run)
            </div>
          ) : (
            <div style={{ maxHeight: 200, overflowY: "auto" }}>
              {monEvents.map((ev, i) => (
                <div key={i} style={C.monitorEvent}>
                  <span style={{ color: "#334155", flexShrink: 0 }}>{ev.ts}</span>
                  {ev.event === "run_completed" ? (
                    <span style={{ color: "#4ade80" }}>
                      <CheckCircle size={11} style={{ verticalAlign: "middle", marginRight: 4 }} />
                      {ev.pipelineName} completed · severity: {ev.severity}
                    </span>
                  ) : ev.event === "live_update" ? (
                    <span style={{ color: "#38bdf8" }}>
                      {(ev.runs || []).length} active pipeline(s) in ADF
                      {(ev.runs || []).map((r) => (
                        <span key={r.runId} style={{ marginLeft: 6, color: "#475569" }}>
                          [{r.pipelineName}
                          {r.anomaly ? <AlertTriangle size={10} style={{ color: "#f97316", marginLeft: 3, verticalAlign: "middle" }} /> : ""}
                          ]
                        </span>
                      ))}
                    </span>
                  ) : <span>{ev.event}</span>}
                </div>
              ))}
            </div>
          )}
          <button onClick={() => navigate("/monitor")} style={{ ...C.btnSecondary, marginTop: 12, fontSize: 12 }}>
            <Activity size={12} /> Open Monitor Agent
          </button>
        </div>
      )}
    </div>
  );
}
