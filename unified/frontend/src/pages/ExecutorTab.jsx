import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { executor, manager, monitor, connectWS } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import {
  Zap, Upload, CheckCircle, XCircle, RotateCcw, Brain, AlertTriangle, Activity, Download,
} from "lucide-react";

const C = {
  page:   { maxWidth: 760, margin: "0 auto" },
  header: { marginBottom: 28 },
  agent:  { display: "flex", alignItems: "center", gap: 10, marginBottom: 6 },
  agentBadge: {
    padding: "4px 12px", background: "var(--warn-soft)", border: "1px solid var(--warn-line)",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "var(--warn)",
    display: "flex", alignItems: "center", gap: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "var(--text)", marginBottom: 4 },
  sub:    { fontSize: 13, color: "var(--text-3)" },
  card:   { background: "var(--surface)", borderRadius: 14, padding: 24, border: "1px solid var(--border)", marginBottom: 16 },
  cardHdr:{ fontSize: 15, fontWeight: 700, color: "var(--text)", marginBottom: 4, display: "flex", alignItems: "center", gap: 8 },
  cardSub:{ fontSize: 13, color: "var(--text-3)", marginBottom: 18 },
  drop:   (active, hasFile) => ({
    border: `2px dashed ${hasFile ? "var(--ok)" : active ? "var(--accent)" : "var(--border-strong)"}`,
    borderRadius: 12, padding: "24px 20px", textAlign: "center", cursor: "pointer",
    background: active ? "var(--surface-2)" : "transparent", transition: "all 0.2s",
  }),
  btnRow: { display: "flex", gap: 10, marginTop: 18, alignItems: "center", flexWrap: "wrap" },
  btnPrimary: (disabled) => ({
    padding: "10px 22px", background: disabled ? "var(--surface-2)" : "var(--accent)",
    color: disabled ? "var(--text-4)" : "var(--accent-fg)", border: "none", borderRadius: 10,
    cursor: disabled ? "not-allowed" : "pointer", fontSize: 13, fontWeight: 700,
    display: "inline-flex", alignItems: "center", gap: 7,
  }),
  btnSecondary: {
    padding: "10px 18px", background: "transparent", color: "var(--text-3)",
    border: "1px solid var(--border)", borderRadius: 10, cursor: "pointer",
    fontSize: 13, display: "inline-flex", alignItems: "center", gap: 6,
  },
  planBox: {
    background: "var(--surface-2)", borderRadius: 10, padding: 14,
    border: "1px solid var(--border)",
  },
  planStage: {
    display: "inline-block", padding: "3px 10px", borderRadius: 8,
    fontSize: 11, fontWeight: 600, background: "var(--surface)", color: "var(--accent)",
    margin: "3px 3px 0 0",
  },
  execStep: (state) => ({
    display: "flex", alignItems: "center", gap: 12, padding: "9px 0",
    borderBottom: "1px solid var(--divider)",
    opacity: state === "pending" ? 0.6 : 1, transition: "opacity 0.3s",
  }),
  execDot: (state) => ({
    width: 10, height: 10, borderRadius: "50%", flexShrink: 0,
    background: state === "done" ? "var(--ok)" : state === "running" ? "var(--warn)" : "var(--border-strong)",
    boxShadow: state === "running" ? "0 0 0 3px color-mix(in srgb, var(--warn) 22%, transparent)" : "none",
    transition: "all 0.3s",
  }),
  execLabel: (state) => ({
    fontSize: 13, flex: 1,
    color: state === "done" ? "var(--ok)" : state === "running" ? "var(--warn)" : "var(--text-4)",
    fontWeight: state === "running" ? 600 : 400,
  }),
  resultBox: (ok) => ({
    background: ok ? "var(--ok-soft)" : "var(--bad-soft)", borderRadius: 10, padding: 16,
    border: `1px solid ${ok ? "var(--ok-soft)" : "var(--bad-soft)"}`, marginTop: 14,
  }),
  errBox: {
    background: "var(--bad-soft)", borderRadius: 8, padding: "10px 14px", marginBottom: 14,
    color: "var(--bad)", fontSize: 13, display: "flex", gap: 8,
  },
  monitorEvent: {
    fontSize: 12, color: "var(--text-3)", padding: "6px 10px",
    borderBottom: "1px solid var(--divider)", display: "flex", gap: 10,
  },
};

// Step labels are now driven by backend progress (jobState.step).
// These are fallback labels shown when no backend step is available yet.
const EXEC_STEPS = [
  "Central Manager pre-flight (validate · predict · optimize)",
  "Authenticating with Azure",
  "Creating storage containers",
  "Uploading your data",
  "Uploading notebooks to Databricks",
  "Running copy pipeline (ADF)",
  "Running notebook stages (Databricks)",
  "Complete",
];

// Match a backend step message to its EXEC_STEPS row. Checked in reverse so
// the most advanced matching phase wins.
const STEP_MATCHERS = [
  (t) => t.includes("validating plan") || t.includes("assurance agent") || t.includes("resource prediction") || t.includes("handing off"),
  (t) => t.includes("authenticating"),
  (t) => t.includes("creating storage"),
  (t) => t.includes("uploading csv") || t.includes("input to"),
  (t) => t.includes("notebook(s)"),
  (t) => t.includes("linked service") || t.includes("copy pipeline"),
  (t) => t.includes("stage group") || t.includes("monitoring databricks") || t.includes("running notebook"),
];

// The tab runs pipelines through the Central Manager (/api/manager/run) so
// every run gets validation, assurance, resource/cost pre-checks, and retry
// handling before the Executor Agent is invoked. Manager state is mapped to
// the executor-style job shape this tab renders.
const MGR_TERMINAL = ["completed", "failed"];
function mapManagerState(s) {
  return {
    status: MGR_TERMINAL.includes(s.status) ? s.status : "running",
    step:   s.step,
    result: s.executor_result,
    error:  s.error,
  };
}

function Spinner({ color = "var(--warn)" }) {
  return (
    <span style={{
      display: "inline-block", width: 13, height: 13,
      border: "2px solid var(--border)", borderTopColor: color,
      borderRadius: "50%", animation: "spin 0.7s linear infinite",
    }} />
  );
}

export default function ExecutorTab() {
  const navigate = useNavigate();
  const {
    csvFile, setCsvFile,
    planResult:      savedPlan,
    plannerPrompt,
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

  // Apply one manager status snapshot to this tab's job view. Returns true
  // while the run is still in progress.
  const _applyStatus = useCallback((raw) => {
    const s = mapManagerState(raw);
    setJobState(s);
    if (s.step) {
      const st = s.step.toLowerCase();
      for (let i = STEP_MATCHERS.length - 1; i >= 0; i--) {
        if (STEP_MATCHERS[i](st)) { setExecStep(i); break; }
      }
    }
    if (s.status !== "running") {
      clearInterval(pollRef.current);
      setRunning(false);
      setExecStep(EXEC_STEPS.length - 1);
      return false;
    }
    return true;
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Extracted poll tick — takes explicit jobId to avoid stale closure
  const _startPolling = useCallback((jid) => {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        if (!_applyStatus(await manager.status(jid))) {
          // Backend _notify_monitor handles DB sync; this is best-effort UI refresh
          monitor.sync(2).catch(() => {});
        }
      } catch (e) {
        if (e?.status === 410 || e?.status === 404) {
          _handleStaleJob();
        }
        // Any other error (network blip): interval keeps running, next tick will retry
      }
    }, 3000);
  }, []); // eslint-disable-line

  // On mount: resume polling if context has a running job (user switched tabs mid-run)
  useEffect(() => {
    if (jobId && jobState?.status === "running") {
      setRunning(true);
      _startPolling(jobId);
    }
    return () => clearInterval(pollRef.current);
  }, []); // eslint-disable-line

  // Follow runs started anywhere — the Central Manager tab, another window.
  // Every run goes through the manager, so its run list is the source of truth;
  // when nothing is running here, attach to the newest live run.
  const [attachedFrom, setAttachedFrom] = useState(false);
  const busyRef = useRef(false);
  busyRef.current = running;
  const jobIdRef = useRef(jobId);
  jobIdRef.current = jobId;
  useEffect(() => {
    let alive = true;
    async function discover() {
      if (busyRef.current) return;
      try {
        const runs = await manager.listRuns();
        const live = (runs || []).find((r) => !MGR_TERMINAL.includes(r.status));
        if (!alive || !live || busyRef.current) return;
        const st = await manager.status(live.run_id);
        if (!alive || busyRef.current) return;
        setError("");
        // A different run than the one this tab started → it came from elsewhere.
        if (live.run_id !== jobIdRef.current) setAttachedFrom(true);
        setJobId(live.run_id);
        if (_applyStatus(st)) {
          setRunning(true);
          _startPolling(live.run_id);
        }
      } catch { /* backend down — try again next tick */ }
    }
    discover();
    const t = setInterval(discover, 4000);
    return () => { alive = false; clearInterval(t); };
  }, [_applyStatus, _startPolling]); // eslint-disable-line react-hooks/exhaustive-deps

  // Monitor WS
  const onWs = useCallback((data) => {
    if (data.event === "live_update" || data.event === "run_completed") {
      setMonEvents((prev) => [{ ts: new Date().toLocaleTimeString(), ...data }, ...prev].slice(0, 10));
    }
  }, []);
  useEffect(() => connectWS(onWs), [onWs]);

  function pickFile(f) {
    if (!f) return;
    if (!/\.(csv|json|jsonl|ndjson)$/i.test(f.name)) {
      setError("Upload a .csv or .json file.");
      return;
    }
    setError("");
    setCsvFile(f);
  }

  function onDrop(e) {
    e.preventDefault(); setDragging(false);
    pickFile(e.dataTransfer.files[0]);
  }

  // The plan and schema were built from the file picked in the Planner. A
  // different file here still runs, but against that file's schema.
  const planFileName = savedSchema?.file_name;
  const fileMismatch = !!(csvFile && planFileName && csvFile.name !== planFileName);

  async function handleRun() {
    if (!csvFile || !savedPlan) return;
    setError(""); setRunning(true); setExecStep(0); setJobState(null); setAttachedFrom(false);

    try {
      const res = await manager.run(csvFile, savedPlan.config, savedSchema || {}, plannerPrompt || "");
      const jid = res.run_id;
      setJobId(jid);
      setJobState({ status: "running", step: "Starting…" });
      // Start polling immediately with explicit run ID — no effect/closure dependency
      _startPolling(jid);
    } catch (e) {
      setRunning(false);
      setError("Failed to start: " + e.message);
    }
  }

  function reset() {
    // Keep csvFile — user likely wants to run the same file again
    setRunning(false); setJobId(null); setAttachedFrom(false);
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
        <p style={C.sub}>Runs go through the Central Manager (validation, resource &amp; cost pre-checks, retries), which hands off to the Executor for ADF + Databricks deployment.</p>
      </div>

      {error && (
        <div style={C.errBox}><XCircle size={14} style={{ flexShrink: 0 }} />{error}</div>
      )}

      {/* Plan loaded from planner */}
      <div style={C.card}>
        <div style={C.cardHdr}><Brain size={16} color="var(--violet)" />Pipeline Plan</div>
        {savedPlan ? (
          <>
            <div style={C.planBox}>
              <div style={{ fontSize: 13, color: "var(--ok)", fontWeight: 600, marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
                <CheckCircle size={13} /> Plan loaded from Planner Agent
              </div>
              <div style={{ marginBottom: 6 }}>
                {(savedPlan.config?.stages || []).map((s, i) => (
                  <span key={i} style={C.planStage}>{s.name}</span>
                ))}
              </div>
              {(savedPlan.config?.execution_groups?.length ?? 0) > 0 && (
                <div style={{ fontSize: 12, color: "var(--text-3)", marginBottom: 6 }}>
                  Flow:{" "}
                  {savedPlan.config.execution_groups.map((g, i) => (
                    <span key={i}>
                      {i > 0 && <span style={{ color: "var(--text-4)" }}> → </span>}
                      <span style={{ color: g.length > 1 ? "var(--violet)" : "var(--text-2)", fontWeight: g.length > 1 ? 600 : 400 }}>
                        [{g.join(" ∥ ")}]
                      </span>
                    </span>
                  ))}
                  {savedPlan.config.execution_groups.some((g) => g.length > 1) && (
                    <span style={{ marginLeft: 6, color: "var(--violet)" }}>⚡ parallel</span>
                  )}
                </div>
              )}
              <div style={{ fontSize: 12, color: "var(--text-3)" }}>
                Cluster: {savedPlan.config?.recommended_settings?.node_type || "auto"} ·
                Workers: {savedPlan.config?.recommended_settings?.num_workers ?? "auto"} ·
                DIU: {savedPlan.config?.recommended_settings?.diu ?? "auto"}
                {savedPlan.used_fallback && <span style={{ marginLeft: 8, color: "var(--warn)" }}>(fallback config)</span>}
              </div>
            </div>
            <button onClick={() => navigate("/planner")} style={{ ...C.btnSecondary, marginTop: 12, fontSize: 12 }}>
              ← Create different plan
            </button>
          </>
        ) : (
          <div style={{ textAlign: "center", padding: "20px 0" }}>
            <div style={{ fontSize: 13, color: "var(--text-3)", marginBottom: 14 }}>No plan loaded — generate one in the Planner Agent first.</div>
            <button onClick={() => navigate("/planner")} style={C.btnPrimary(false)}>
              <Brain size={13} /> Go to Planner
            </button>
          </div>
        )}
      </div>

      {/* CSV upload */}
      {savedPlan && (
        <div style={C.card}>
          <div style={C.cardHdr}><Upload size={16} color="var(--accent)" />Data File</div>
          <input ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden onChange={(e) => { pickFile(e.target.files[0]); e.target.value = ""; }} />

          {csvFile ? (
            /* File already loaded — from Planner or previous upload */
            <div style={{ background: "var(--surface-2)", borderRadius: 10, padding: "12px 16px", border: "1px solid var(--ok-line)", display: "flex", alignItems: "center", gap: 10 }}>
              <CheckCircle size={16} color="var(--ok)" style={{ flexShrink: 0 }} />
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, color: "var(--ok)", fontWeight: 600 }}>{csvFile.name}</div>
                <div style={{ fontSize: 12, color: "var(--text-3)", marginTop: 2 }}>
                  {fileMismatch
                    ? <span style={{ color: "var(--warn)" }}>
                        <AlertTriangle size={11} style={{ verticalAlign: "middle", marginRight: 4 }} />
                        The plan was generated for "{planFileName}". Re-plan in the Planner if this file has different columns.
                      </span>
                    : "Carried over from Planner Agent — no re-upload needed."}
                </div>
              </div>
              <button
                onClick={() => !running && fileRef.current.click()}
                style={{ fontSize: 12, color: "var(--text-4)", background: "none", border: "1px solid var(--border)", borderRadius: 6, padding: "4px 10px", cursor: "pointer", flexShrink: 0 }}
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
              <Upload size={28} color="var(--text-4)" style={{ marginBottom: 8 }} />
              <div style={{ fontSize: 13, color: "var(--text-3)" }}>Click or drag your CSV or JSON here</div>
            </div>
          )}
        </div>
      )}

      {/* Run + execution progress — also shown for a run followed from the
          Central Manager, even when this browser holds no plan */}
      {(savedPlan || running || jobState) && (
        <div style={C.card}>
          <div style={C.cardHdr}><Zap size={16} color="var(--warn)" />Execution</div>

          {attachedFrom && jobId && (
            <div style={{
              display: "flex", alignItems: "center", gap: 8, margin: "8px 0 14px",
              padding: "8px 12px", borderRadius: 8, fontSize: 12.5,
              background: "var(--accent-soft)", border: "1px solid var(--accent-line)", color: "var(--text-2)",
            }}>
              <Activity size={14} color="var(--accent)" />
              Following run <span style={{ fontFamily: "var(--font-mono)", color: "var(--text)" }}>{jobId.slice(0, 8)}</span>,
              started from the Central Manager.
              <button onClick={() => navigate("/manager")}
                style={{ marginLeft: "auto", background: "none", border: 0, color: "var(--accent)", cursor: "pointer", fontSize: 12.5, fontWeight: 500 }}>
                Open in Manager →
              </button>
            </div>
          )}

          {!running && !jobState && (
            <>
              <div style={{ fontSize: 13, color: "var(--text-3)", marginBottom: 16 }}>
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
                  const failed    = jobState?.status === "failed";
                  const completed = jobState?.status === "completed";
                  const isLast = i === EXEC_STEPS.length - 1;
                  let state;
                  if (completed) {
                    state = "done";   // finished — every step is done, incl. "Complete"
                  } else if (failed) {
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
                        // override only for the failed step — `undefined` here used to wipe the dot colour
                        ...(failed && execStep === i ? { background: "var(--bad)" } : null),
                      }} />
                      <span style={C.execLabel(state)}>{liveLabel}</span>
                      {state === "running" && !failed && <Spinner />}
                      {state === "done"    && <CheckCircle size={13} color="var(--ok)" />}
                      {failed && execStep === i && <XCircle size={13} color="var(--bad)" />}
                    </div>
                  );
                })}
              </div>

              {/* Result */}
              {jobState && jobState.status !== "running" && (
                <div style={C.resultBox(jobState.status === "completed")}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                    <div style={{ fontSize: 15, fontWeight: 700,
                      color: jobState.status === "completed" ? "var(--ok)" : "var(--bad)" }}>
                      {jobState.status === "completed" ? "Pipeline completed successfully!" : "Pipeline failed"}
                    </div>
                    {jobState.status === "completed" && (jobState.result?.sink_container) && (
                      <a
                        href="#"
                        onClick={(e) => { e.preventDefault(); executor.download(jobState.result.sink_container).catch((err) => setError(err.message)); }}
                        style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 14px",
                          background: "var(--accent)", color: "var(--accent-fg)", borderRadius: 8, fontSize: 12,
                          fontWeight: 600, textDecoration: "none" }}
                      >
                        <Download size={13} /> Download output
                      </a>
                    )}
                  </div>
                  {jobState.error && (
                    <pre style={{ fontSize: 12, color: "var(--bad)", whiteSpace: "pre-wrap", wordBreak: "break-word", marginBottom: 8 }}>
                      {jobState.error}
                    </pre>
                  )}
                  {jobState.result?.result?.message && (
                    <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 8, padding: "8px 10px", background: "var(--surface-2)", borderRadius: 6 }}>
                      ADF: {jobState.result.result.message}
                    </div>
                  )}
                  {jobState.result && (
                    <details style={{ marginTop: 8 }}>
                      <summary style={{ cursor: "pointer", fontSize: 12, color: "var(--ok)" }}>Run details</summary>
                      <pre style={{ marginTop: 8, fontSize: 11, color: "var(--text-2)", overflow: "auto", maxHeight: 180 }}>
                        {JSON.stringify(jobState.result, null, 2)}
                      </pre>
                    </details>
                  )}
                </div>
              )}
            </>
          )}

          {jobState && !running && (
            <div style={C.btnRow}>
              <button style={C.btnPrimary(false)} onClick={reset}><RotateCcw size={13} /> Run again</button>
            </div>
          )}
        </div>
      )}

      {/* Monitor Agent live feed */}
      {(running || jobState) && (
        <div style={C.card}>
          <div style={C.cardHdr}><Activity size={16} color="var(--accent)" />Monitor Agent — Live Feed</div>
          <div style={{ fontSize: 13, color: "var(--text-3)", marginBottom: 12 }}>
            ADF events received via WebSocket. Anomalies are flagged automatically.
          </div>
          {monEvents.length === 0 ? (
            <div style={{ fontSize: 13, color: "var(--text-4)", textAlign: "center", padding: "12px 0" }}>
              Waiting for ADF events… (appears once ADF picks up the triggered run)
            </div>
          ) : (
            <div style={{ maxHeight: 200, overflowY: "auto" }}>
              {monEvents.map((ev, i) => (
                <div key={i} style={C.monitorEvent}>
                  <span style={{ color: "var(--text-4)", flexShrink: 0 }}>{ev.ts}</span>
                  {ev.event === "run_completed" ? (
                    <span style={{ color: "var(--ok)" }}>
                      <CheckCircle size={11} style={{ verticalAlign: "middle", marginRight: 4 }} />
                      {ev.pipelineName} completed · severity: {ev.severity}
                    </span>
                  ) : ev.event === "live_update" ? (
                    <span style={{ color: "var(--accent)" }}>
                      {(ev.runs || []).length} active pipeline(s) in ADF
                      {(ev.runs || []).map((r) => (
                        <span key={r.runId} style={{ marginLeft: 6, color: "var(--text-4)" }}>
                          [{r.pipelineName}
                          {r.anomaly ? <AlertTriangle size={10} style={{ color: "var(--orange)", marginLeft: 3, verticalAlign: "middle" }} /> : ""}
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
