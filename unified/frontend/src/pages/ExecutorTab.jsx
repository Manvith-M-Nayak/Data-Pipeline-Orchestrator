import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Activity, AlertTriangle, Brain, CheckCircle2, Circle, Download, FileText, Play, RotateCcw, Upload, XCircle, Zap,
} from "lucide-react";
import { executor, monitor, connectWS } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import { failedPhaseOf } from "../flows/status.js";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import { Alert, Badge, Button, Card, Empty, PageHeader, Spinner } from "../ui/components.jsx";

// Step labels are driven by backend progress (jobState.step); these are the
// fallback labels shown when no backend step is available yet.
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

// The page runs pipelines through the Central Manager (/api/manager/run) so
// every run gets validation, assurance, resource/cost pre-checks, and retry
// handling before the Executor Agent is invoked. Manager state is mapped to
// the executor-style job shape this page renders.
const MGR_TERMINAL = ["completed", "failed"];
function mapManagerState(s) {
  return {
    status: MGR_TERMINAL.includes(s.status) ? s.status : "running",
    step:   s.step,
    result: s.executor_result,
    error:  s.error,
  };
}

function StepIcon({ state }) {
  if (state === "running") return <Spinner size={14} />;
  if (state === "done")    return <CheckCircle2 size={16} strokeWidth={2} style={{ color: "var(--ok)" }} />;
  if (state === "failed")  return <XCircle size={16} strokeWidth={2} style={{ color: "var(--bad)" }} />;
  return <Circle size={14} strokeWidth={2} style={{ color: "var(--text-4)" }} />;
}

export default function ExecutorTab() {
  const navigate = useNavigate();
  const {
    csvFile, setCsvFile, csvRestoring,
    planResult:      savedPlan,
    plannerPrompt,
    detectedSchema, runSchema,
    runId: jobId, run: rawRun, runOrigin, runError,
    startRun, clearRun,
  } = useAppContext();

  const [dragging,  setDragging]  = useState(false);
  const [starting,  setStarting]  = useState(false);
  const [localError, setLocalError] = useState("");
  const [monEvents, setMonEvents] = useState([]);
  const fileRef = useRef();
  const error = localError || runError;
  const setError = setLocalError;

  // The run comes from AppContext — the same copy the Central Manager shows,
  // fetched fresh from the server after every reload.
  const jobState = rawRun ? mapManagerState(rawRun) : null;
  const running = starting || jobState?.status === "running" || (!!jobId && !rawRun);
  // Runs this page did not start (Central Manager, another window, auto-followed).
  const attachedFrom = !!jobId && runOrigin !== "executor";

  // Which step row is active. Read from the executor's progress text; the
  // furthest step seen for this run is remembered, because a failure replaces
  // the text with "Failed: …" and the row it failed on must stay marked.
  const furthest = useRef({ id: null, i: -1 });
  if (furthest.current.id !== jobId) furthest.current = { id: jobId, i: -1 };
  let execStep = -1;
  if (jobState?.status === "completed") execStep = EXEC_STEPS.length - 1;
  else if (jobState) {
    const st = (jobState.step || "").toLowerCase();
    for (let i = STEP_MATCHERS.length - 1; i >= 0; i--) {
      if (STEP_MATCHERS[i](st)) { execStep = i; break; }
    }
    if (execStep === -1 && jobState.status === "running") execStep = 0;
    furthest.current.i = Math.max(furthest.current.i, execStep);
    execStep = furthest.current.i;
    // After a reload a failed run has no progress text left: if it never
    // reached execution, it failed in the Manager's pre-flight row.
    if (execStep === -1 && jobState.status === "failed") {
      execStep = ["validating", "assuring_plan", "pre_checks"].includes(failedPhaseOf(rawRun)) ? 0 : -1;
    }
  }

  // Refresh the monitor once when a run finishes (best-effort UI refresh;
  // the backend's _notify_monitor does the real sync).
  const lastStatus = useRef(jobState?.status);
  useEffect(() => {
    const prev = lastStatus.current;
    lastStatus.current = jobState?.status;
    if (prev === "running" && jobState && jobState.status !== "running") monitor.sync(2).catch(() => {});
  }, [jobState?.status]); // eslint-disable-line react-hooks/exhaustive-deps

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
  const planFileName = detectedSchema?.file_name;
  const fileMismatch = !!(csvFile && planFileName && csvFile.name !== planFileName);

  async function handleRun() {
    if (!csvFile || !savedPlan) return;
    setError(""); setStarting(true); setMonEvents([]);
    try {
      await startRun({ file: csvFile, config: savedPlan.config, schema: runSchema, request: plannerPrompt, origin: "executor" });
    } catch (e) {
      setError("Failed to start: " + e.message);
    } finally {
      setStarting(false);
    }
  }

  function reset() {
    // Keep csvFile — user likely wants to run the same file again
    clearRun();
    setError(""); setMonEvents([]);
  }

  const canRun = !!csvFile && !!savedPlan && !running;
  const cfg = savedPlan?.config;
  const failed = jobState?.status === "failed";
  const completed = jobState?.status === "completed";
  const rs = cfg?.recommended_settings || {};

  return (
    <div>
      <input ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden
        onChange={(e) => { pickFile(e.target.files[0]); e.target.value = ""; }} />

      <PageHeader
        eyebrow="Run" icon={Zap}
        title="Executor"
        description="Deploys the plan to Azure — storage containers, ADF copy, Databricks notebooks — through the Central Manager, which adds validation, pre-checks and retries."
        actions={jobState && !running
          ? <Button variant="primary" icon={RotateCcw} onClick={reset}>Run again</Button>
          : <Button variant="primary" icon={Play} loading={starting || (running && !jobState)} disabled={!canRun} onClick={handleRun}>Run pipeline</Button>}
      />

      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}

      {!savedPlan && !jobState ? (
        <Card>
          <Empty icon={Brain} title="No plan to run"
            action={<Button variant="primary" onClick={() => navigate("/planner")}>Open the Planner</Button>}>
            Design a pipeline in the Planner (or use guided mode), then run it here.
          </Empty>
        </Card>
      ) : (
        <div className="grid grid-2" style={{ marginBottom: 14, alignItems: "start" }}>
          <Card title="Plan" icon={Brain} actions={<button className="link" onClick={() => navigate("/planner")}>Change →</button>}>
            {cfg ? (
              <div className="stack" style={{ gap: 10 }}>
                <div className="chips">
                  {(cfg.stages || []).map((s) => (
                    <Badge key={s.name} tone={s.type === "copy" ? "accent" : "violet"}>{s.name}</Badge>
                  ))}
                </div>
                {(cfg.execution_groups?.length ?? 0) > 0 && (
                  <div className="mono muted" style={{ fontSize: 12, overflowWrap: "anywhere" }}>
                    {cfg.execution_groups.map((g) => `[${g.join(" ∥ ")}]`).join(" → ")}
                  </div>
                )}
                <div className="muted" style={{ fontSize: 12.5 }}>
                  Node <span className="mono">{rs.node_type || "auto"}</span> · workers {rs.num_workers ?? "auto"} · DIU {rs.diu ?? "auto"}
                  {savedPlan.used_fallback && <Badge tone="warn" style={{ marginLeft: 8 }}>fallback</Badge>}
                </div>
              </div>
            ) : <div className="muted">Following a run started elsewhere.</div>}
          </Card>

          <Card title="Data file" icon={FileText}
            actions={csvFile && !running ? <button className="link" onClick={() => fileRef.current.click()}>Change →</button> : null}>
            {csvRestoring ? (
              <div className="row muted" style={{ gap: 8, fontSize: 13 }}><Spinner size={12} /> Restoring your data file…</div>
            ) : csvFile ? (
              <div className="stack" style={{ gap: 6, fontSize: 13 }}>
                <div className="mono" style={{ color: "var(--text)" }}>{csvFile.name}</div>
                <div className="muted">{(csvFile.size / 1024).toFixed(1)} KB</div>
                {fileMismatch && (
                  <Alert tone="warn" style={{ padding: "8px 10px", fontSize: 12.5 }}>
                    The plan was designed for “{planFileName}”. Design it again if this file has different columns.
                  </Alert>
                )}
              </div>
            ) : (
              <div
                className={`dropzone${dragging ? " over" : ""}`}
                style={{ padding: "22px 16px" }}
                onClick={() => !running && fileRef.current.click()}
                onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
                onDragLeave={() => setDragging(false)}
                onDrop={onDrop}
                role="button" tabIndex={0}
              >
                <Upload size={18} style={{ color: "var(--text-3)" }} />
                <div className="muted" style={{ fontSize: 13, marginTop: 6 }}>Drop or choose a CSV / JSON file</div>
              </div>
            )}
          </Card>
        </div>
      )}

      {(running || jobState) && (
        <Card title="Execution" icon={Zap} style={{ marginBottom: 14 }}
          subtitle={jobId ? <>Run <span className="mono">{jobId.slice(0, 8)}</span></> : undefined}>
          {attachedFrom && jobId && (
            <Alert tone="accent" style={{ marginBottom: 14 }}
              action={<Button size="sm" onClick={() => navigate("/manager")}>Open in Manager</Button>}>
              Following run <span className="mono">{jobId.slice(0, 8)}</span>, started from the Central Manager.
            </Alert>
          )}

          {(rawRun?.plan?.stages?.length || cfg?.stages?.length) ? (
            <div style={{ marginBottom: 16 }}>
              <PipelineFlow
                plan={rawRun?.plan?.stages?.length ? rawRun.plan : cfg}
                runState={rawRun || { status: "validating", phase: "validating", decisions: [] }}
                inputLabel={csvFile?.name}
              />
            </div>
          ) : null}

          <div className="list">
            {EXEC_STEPS.map((label, i) => {
              const isLast = i === EXEC_STEPS.length - 1;
              let state;
              if (completed) state = "done";   // finished — every step is done, incl. "Complete"
              else if (failed) state = isLast ? "pending" : execStep > i ? "done" : execStep === i ? "failed" : "pending";
              else state = execStep > i ? "done" : execStep === i ? "running" : "pending";
              // Show the live backend step name on the running row
              const text = state === "running" && jobState?.step ? jobState.step : label;
              return (
                <div key={i} className="list-row" style={{ opacity: state === "pending" ? 0.6 : 1 }}>
                  <span style={{ width: 18, display: "inline-flex", justifyContent: "center" }}><StepIcon state={state} /></span>
                  <span className="grow" style={{
                    color: state === "failed" ? "var(--bad)" : state === "running" ? "var(--text)" : state === "done" ? "var(--text-2)" : "var(--text-3)",
                    fontWeight: state === "running" ? 600 : 400,
                  }}>{text}</span>
                </div>
              );
            })}
          </div>

          {jobState && jobState.status !== "running" && (
            <div className="big-status" style={{
              marginTop: 16,
              background: completed ? "var(--ok-soft)" : "var(--bad-soft)",
              borderColor: completed ? "var(--ok-line)" : "var(--bad-line)",
            }}>
              <div className="icon" style={{ background: "var(--surface)", color: completed ? "var(--ok)" : "var(--bad)" }}>
                {completed ? <CheckCircle2 size={20} /> : <XCircle size={20} />}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ color: "var(--text)", fontWeight: 600 }}>{completed ? "Pipeline completed" : "Pipeline failed"}</div>
                {jobState.error && <pre className="mono" style={{ fontSize: 12, color: "var(--bad)", whiteSpace: "pre-wrap", wordBreak: "break-word", marginTop: 4 }}>{jobState.error}</pre>}
                {jobState.result?.result?.message && <div className="muted" style={{ fontSize: 12.5, marginTop: 4 }}>ADF: {jobState.result.result.message}</div>}
                {jobState.result && (
                  <details style={{ marginTop: 6 }}>
                    <summary className="link" style={{ fontSize: 12.5 }}>Run details</summary>
                    <pre className="mono" style={{ marginTop: 8, fontSize: 11.5, color: "var(--text-2)", overflow: "auto", maxHeight: 200, background: "var(--surface)", padding: 10, borderRadius: 8, border: "1px solid var(--border)" }}>
                      {JSON.stringify(jobState.result, null, 2)}
                    </pre>
                  </details>
                )}
              </div>
              {completed && jobState.result?.sink_container && (
                <Button variant="primary" icon={Download}
                  onClick={() => executor.download(jobState.result.sink_container).catch((err) => setError(err.message))}>
                  Download output
                </Button>
              )}
            </div>
          )}
        </Card>
      )}

      {(running || jobState) && (
        <Card title="Monitor feed" icon={Activity} subtitle="ADF events over WebSocket — anomalies are flagged automatically"
          actions={<button className="link" onClick={() => navigate("/monitor")}>Open Monitor →</button>}>
          {monEvents.length === 0 ? (
            <div className="muted" style={{ fontSize: 13 }}>Waiting for ADF events… they appear once ADF picks up the run.</div>
          ) : (
            <div className="list" style={{ maxHeight: 240, overflowY: "auto" }}>
              {monEvents.map((ev, i) => (
                <div key={i} className="list-row" style={{ fontSize: 12.5 }}>
                  <span className="mono faint" style={{ width: 80, flexShrink: 0 }}>{ev.ts}</span>
                  {ev.event === "run_completed" ? (
                    <span style={{ color: "var(--ok)" }}>
                      <CheckCircle2 size={12} style={{ verticalAlign: -2, marginRight: 4 }} />
                      {ev.pipelineName} finished · severity {ev.severity}
                    </span>
                  ) : (
                    <span className="grow" style={{ color: "var(--text-2)" }}>
                      {(ev.runs || []).length} active in ADF
                      {(ev.runs || []).map((r) => (
                        <span key={r.runId} className="faint" style={{ marginLeft: 6 }}>
                          [{r.pipelineName}{r.anomaly ? <AlertTriangle size={10} style={{ color: "var(--warn)", marginLeft: 3, verticalAlign: -1 }} /> : ""}]
                        </span>
                      ))}
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}
        </Card>
      )}
    </div>
  );
}
