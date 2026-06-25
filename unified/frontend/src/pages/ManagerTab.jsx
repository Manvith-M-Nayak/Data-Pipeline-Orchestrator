import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { manager, executor } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import {
  Shield, Brain, Zap, ClipboardCheck, TrendingUp, CheckCircle,
  XCircle, AlertTriangle, Clock, Activity, RotateCcw, Download,
  ChevronRight, DollarSign, Cpu, GitBranch, RefreshCw,
} from "lucide-react";

// ── Phase metadata ────────────────────────────────────────────────────────────
const PHASES = [
  { key: "validating",  label: "Validate",  icon: Shield,         color: "#818cf8" },
  { key: "pre_checks",  label: "Pre-checks", icon: Cpu,           color: "#38bdf8" },
  { key: "executing",   label: "Execute",   icon: Zap,            color: "#f59e0b" },
  { key: "assurance",   label: "Assurance", icon: ClipboardCheck, color: "#4ade80" },
  { key: "feedback",    label: "Feedback",  icon: TrendingUp,     color: "#c084fc" },
  { key: "completed",   label: "Done",      icon: CheckCircle,    color: "#22c55e" },
];

const PHASE_ORDER = PHASES.map((p) => p.key);

const S = {
  page: { maxWidth: 900, margin: "0 auto" },
  header: { marginBottom: 28 },
  badge: {
    padding: "4px 12px", background: "#1a1a2e", border: "1px solid #4f46e5",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "#818cf8",
    display: "inline-flex", alignItems: "center", gap: 6, marginBottom: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:    { fontSize: 13, color: "#64748b" },
  card:   { background: "#1e293b", borderRadius: 14, padding: 20, border: "1px solid #334155", marginBottom: 14 },
  cardHdr:{ fontSize: 14, fontWeight: 700, color: "#f1f5f9", marginBottom: 12, display: "flex", alignItems: "center", gap: 8 },
  grid2:  { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 },
  btnPrimary: (disabled) => ({
    padding: "10px 22px", background: disabled ? "#1e293b" : "#818cf8",
    color: disabled ? "#475569" : "#0f172a", border: "none", borderRadius: 10,
    cursor: disabled ? "not-allowed" : "pointer", fontSize: 13, fontWeight: 700,
    display: "inline-flex", alignItems: "center", gap: 7,
  }),
  btnSecondary: {
    padding: "10px 18px", background: "transparent", color: "#64748b",
    border: "1px solid #334155", borderRadius: 10, cursor: "pointer",
    fontSize: 13, display: "inline-flex", alignItems: "center", gap: 6,
  },
  errBox: {
    background: "#450a0a", borderRadius: 8, padding: "10px 14px", marginBottom: 14,
    color: "#f87171", fontSize: 13, display: "flex", gap: 8,
  },
  decisionRow: (severity) => ({
    display: "flex", gap: 10, padding: "7px 10px",
    borderBottom: "1px solid #1e293b", alignItems: "flex-start",
    background: severity === "error" ? "rgba(127,29,29,0.15)" : severity === "warn" ? "rgba(120,53,15,0.1)" : "transparent",
  }),
  tag: (severity) => ({
    fontSize: 10, fontWeight: 700, borderRadius: 4, padding: "2px 6px", flexShrink: 0,
    background:
      severity === "ok"    ? "#14532d" :
      severity === "error" ? "#7f1d1d" :
      severity === "warn"  ? "#78350f" : "#1e293b",
    color:
      severity === "ok"    ? "#4ade80" :
      severity === "error" ? "#f87171" :
      severity === "warn"  ? "#fbbf24" : "#64748b",
  }),
  kv: { display: "flex", flexDirection: "column", gap: 6 },
  kvRow: { display: "flex", justifyContent: "space-between", fontSize: 12, color: "#94a3b8", borderBottom: "1px solid #1e293b", paddingBottom: 5 },
  kvVal: { color: "#f1f5f9", fontWeight: 600 },
  chip: (ok) => ({
    display: "inline-flex", alignItems: "center", gap: 4,
    padding: "2px 8px", borderRadius: 20, fontSize: 11, fontWeight: 600,
    background: ok ? "#14532d" : ok === false ? "#7f1d1d" : "#1e293b",
    color: ok ? "#4ade80" : ok === false ? "#f87171" : "#94a3b8",
  }),
};

function Spinner({ size = 14, color = "#818cf8" }) {
  return (
    <span style={{
      display: "inline-block", width: size, height: size,
      border: "2px solid #334155", borderTopColor: color,
      borderRadius: "50%", animation: "spin 0.7s linear infinite",
    }} />
  );
}

function PhaseBar({ currentStatus, currentPhase }) {
  const isTerminal = currentStatus === "completed" || currentStatus === "failed";
  const activeIdx = isTerminal
    ? (currentStatus === "completed" ? PHASES.length - 1 : PHASE_ORDER.indexOf(currentPhase))
    : PHASE_ORDER.indexOf(currentPhase);

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 20, overflowX: "auto" }}>
      {PHASES.map((p, i) => {
        const isDone    = currentStatus === "completed" ? true : i < activeIdx;
        const isActive  = !isTerminal && PHASE_ORDER[activeIdx] === p.key;
        const isFailed  = currentStatus === "failed" && isActive;
        const Icon = p.icon;
        return (
          <React.Fragment key={p.key}>
            <div style={{
              display: "flex", flexDirection: "column", alignItems: "center", gap: 4,
              minWidth: 72,
            }}>
              <div style={{
                width: 34, height: 34, borderRadius: "50%",
                display: "flex", alignItems: "center", justifyContent: "center",
                background:
                  isFailed  ? "#7f1d1d" :
                  isActive  ? p.color + "22" :
                  isDone    ? "#14532d" : "#1e293b",
                border: `2px solid ${
                  isFailed  ? "#f87171" :
                  isActive  ? p.color :
                  isDone    ? "#22c55e" : "#334155"
                }`,
                transition: "all 0.3s",
              }}>
                {isFailed ? (
                  <XCircle size={15} color="#f87171" />
                ) : isActive ? (
                  <Spinner size={13} color={p.color} />
                ) : isDone ? (
                  <CheckCircle size={15} color="#22c55e" />
                ) : (
                  <Icon size={14} color="#334155" />
                )}
              </div>
              <span style={{
                fontSize: 10, fontWeight: isActive ? 700 : 400,
                color: isActive ? p.color : isDone ? "#4ade80" : "#475569",
              }}>
                {p.label}
              </span>
            </div>
            {i < PHASES.length - 1 && (
              <div style={{
                flex: 1, height: 2, minWidth: 16,
                background: isDone ? "#22c55e" : "#1e293b",
                marginBottom: 18, transition: "background 0.3s",
              }} />
            )}
          </React.Fragment>
        );
      })}
    </div>
  );
}

function DecisionLog({ decisions }) {
  const endRef = useRef();
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [decisions.length]);

  if (!decisions.length) {
    return <div style={{ fontSize: 13, color: "#334155", padding: "10px 0" }}>Waiting for decisions…</div>;
  }
  return (
    <div style={{ maxHeight: 240, overflowY: "auto", borderRadius: 8, border: "1px solid #1e293b" }}>
      {decisions.map((d, i) => (
        <div key={i} style={S.decisionRow(d.severity)}>
          <span style={{ fontSize: 10, color: "#334155", flexShrink: 0, paddingTop: 2, minWidth: 72 }}>
            {d.ts?.slice(11, 19)}
          </span>
          <span style={S.tag(d.severity)}>{d.severity?.toUpperCase()}</span>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 12, color: "#f1f5f9", fontWeight: 600 }}>{d.action}</div>
            <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
              {d.reason}{d.outcome ? <span style={{ color: "#94a3b8" }}> → {d.outcome}</span> : null}
            </div>
          </div>
        </div>
      ))}
      <div ref={endRef} />
    </div>
  );
}

function PredictionsCard({ predictions, cost, resourcePlan }) {
  if (!predictions?.stage_count) return null;
  const allocs    = resourcePlan?.allocations || [];
  const feasible  = resourcePlan?.feasible ?? true;
  const violations = resourcePlan?.constraint_violations || [];
  const warnings   = resourcePlan?.warnings || [];
  const factors    = predictions.correction_factors || {};

  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <Cpu size={14} color="#38bdf8" />Resource Prediction
        {resourcePlan && (
          <span style={{
            marginLeft: "auto", padding: "2px 8px", borderRadius: 99, fontSize: 11, fontWeight: 700,
            background: feasible ? "#4ade8022" : "#f8717122", color: feasible ? "#4ade80" : "#f87171",
          }}>
            {feasible ? "Feasible" : "Infeasible"}
          </span>
        )}
      </div>

      {/* Constraint violations */}
      {violations.map((v, i) => (
        <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "#f87171", marginBottom: 4 }}>
          <AlertTriangle size={11} style={{ flexShrink: 0, marginTop: 1 }} />{v}
        </div>
      ))}
      {warnings.map((w, i) => (
        <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "#f59e0b", marginBottom: 4 }}>
          <AlertTriangle size={11} style={{ flexShrink: 0, marginTop: 1 }} />{w}
        </div>
      ))}

      <div style={S.kv}>
        <div style={S.kvRow}><span>File size</span><span style={S.kvVal}>{predictions.file_size_mb} MB</span></div>
        <div style={S.kvRow}><span>Stages</span><span style={S.kvVal}>{predictions.stage_count} ({predictions.copy_stages} copy + {predictions.notebook_stages} notebook)</span></div>
        <div style={S.kvRow}><span>Complexity</span><span style={S.kvVal}>{predictions.complexity}</span></div>
        <div style={S.kvRow}><span>Peak workers</span><span style={S.kvVal}>{predictions.suggested_workers}</span></div>
        <div style={S.kvRow}><span>Total memory</span><span style={S.kvVal}>{predictions.total_memory_gb ?? "—"} GB</span></div>
        <div style={S.kvRow}><span>Estimated duration</span><span style={S.kvVal}>~{predictions.estimated_duration_s}s</span></div>
        <div style={S.kvRow}><span>Node type</span><span style={S.kvVal}>{predictions.node_type}</span></div>
        {(factors.copy || factors.notebook) && (
          <div style={S.kvRow}>
            <span>Correction factors</span>
            <span style={S.kvVal}>{factors.copy}× copy · {factors.notebook}× notebook</span>
          </div>
        )}
      </div>

      {/* Per-stage allocations from Resource Agent */}
      {allocs.length > 0 && (
        <div style={{ marginTop: 12, borderTop: "1px solid #1e293b", paddingTop: 10 }}>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 8 }}>Stage allocations</div>
          {allocs.map((a) => (
            <div key={a.stage_name} style={{
              display: "flex", justifyContent: "space-between", alignItems: "center",
              fontSize: 11, color: "#94a3b8", paddingBottom: 4, marginBottom: 4,
              borderBottom: "1px solid #1e293b",
            }}>
              <span style={{ color: "#f1f5f9", fontWeight: 600, minWidth: 120 }}>{a.stage_name}</span>
              <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                {a.stage_type === "notebook"
                  ? <span>{a.workers}w · {a.memory_gb}GB · {a.cpu}vCPU</span>
                  : <span>{a.diu} DIU</span>}
                <span style={{ color: "#64748b" }}>~{a.duration_s}s</span>
                {a.right_sized && <span style={{ color: "#4ade80", fontSize: 10 }}>✔ right-sized</span>}
                {a.contention_adjusted && <span style={{ color: "#f59e0b", fontSize: 10 }}>⚠ adjusted</span>}
              </div>
            </div>
          ))}
        </div>
      )}

      {cost?.total_usd !== undefined && (
        <div style={{ marginTop: 12, padding: "8px 12px", borderRadius: 8, background: "#0f172a", border: "1px solid #1e293b" }}>
          <div style={{ fontSize: 12, color: "#64748b", marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
            <DollarSign size={12} /> Cost estimate
          </div>
          <div style={S.kv}>
            <div style={S.kvRow}><span>ADF activities</span><span style={S.kvVal}>${cost.adf_activity_usd}</span></div>
            <div style={S.kvRow}><span>Databricks</span><span style={S.kvVal}>${cost.databricks_usd}</span></div>
            <div style={S.kvRow}><span>Blob storage</span><span style={S.kvVal}>${cost.storage_usd}</span></div>
            <div style={{ ...S.kvRow, borderBottom: "none" }}>
              <span style={{ fontWeight: 700, color: "#f1f5f9" }}>Total</span>
              <span style={{ ...S.kvVal, color: cost.budget_ok ? "#4ade80" : "#f59e0b" }}>
                ${cost.total_usd} {cost.budget_ok ? "✔" : "⚠ >$1"}
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ParallelismCard({ parallelism }) {
  if (!parallelism?.execution_groups) return null;
  return (
    <div style={S.card}>
      <div style={S.cardHdr}><GitBranch size={14} color="#c084fc" />Parallelism Analysis</div>
      <div style={{ marginBottom: 10, fontSize: 12, color: "#94a3b8" }}>
        {parallelism.can_parallelize
          ? `${parallelism.parallel_groups} group(s) can run in parallel`
          : "All stages run sequentially (linear dependency chain)"}
      </div>
      {parallelism.execution_groups.map((group, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
          <span style={{ fontSize: 10, color: "#475569", minWidth: 48 }}>Group {i + 1}</span>
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {group.map((name) => (
              <span key={name} style={{
                padding: "2px 8px", borderRadius: 6, fontSize: 11,
                background: group.length > 1 ? "#2d1b69" : "#1e293b",
                color: group.length > 1 ? "#c084fc" : "#64748b",
                border: `1px solid ${group.length > 1 ? "#4c1d95" : "#334155"}`,
              }}>
                {name}
              </span>
            ))}
          </div>
          {group.length > 1 && (
            <span style={{ fontSize: 10, color: "#c084fc" }}>parallel</span>
          )}
        </div>
      ))}
    </div>
  );
}

function AssuranceCard({ assurance }) {
  if (!Object.keys(assurance || {}).length) return null;
  const rows = [
    { label: "All stages completed", val: assurance.all_stages_completed, bool: true },
    { label: "Output present",       val: assurance.has_output,           bool: true },
    { label: "Timing OK (< 4× est)", val: assurance.timing_ok,           bool: true },
    { label: "Actual duration",      val: `${assurance.actual_duration_s}s` },
    { label: "Predicted duration",   val: `${assurance.predicted_duration_s}s` },
    { label: "Timing ratio",         val: `${assurance.timing_ratio}×` },
    { label: "Retries used",         val: assurance.retries_used },
  ].filter((r) => r.val !== undefined && r.val !== null);

  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <ClipboardCheck size={14} color="#4ade80" />
        Assurance Checks
        <span style={S.chip(assurance.passed)}>
          {assurance.passed ? "PASSED" : "WARNINGS"}
        </span>
      </div>
      <div style={S.kv}>
        {rows.map(({ label, val, bool }) => (
          <div key={label} style={S.kvRow}>
            <span>{label}</span>
            <span style={S.kvVal}>
              {bool ? (
                <span style={S.chip(val)}>
                  {val ? "✔ Yes" : "✖ No"}
                </span>
              ) : val}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function ManagerTab() {
  const navigate = useNavigate();
  const {
    csvFile,
    planResult: savedPlan,
    managerRunId: runId, setManagerRunId: setRunId,
    managerState: mgrState, setManagerState: setMgrState,
  } = useAppContext();

  const savedSchema = (() => {
    try { return JSON.parse(localStorage.getItem("last_csv_schema") || "null"); } catch { return null; }
  })();

  const [running, setRunning] = useState(false);
  const [error,   setError]   = useState("");
  const pollRef = useRef();

  // ── Polling ────────────────────────────────────────────────────────────────
  const _startPolling = useCallback((rid) => {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const s = await manager.status(rid);
        setMgrState(s);
        if (s.status === "completed" || s.status === "failed") {
          clearInterval(pollRef.current);
          setRunning(false);
        }
      } catch (e) {
        const msg = e?.message || "";
        if (msg.startsWith("404")) {
          clearInterval(pollRef.current);
          setRunning(false);
          setError("Run session expired — click Run again.");
          setRunId(null);
          setMgrState(null);
        }
      }
    }, 2000);
  }, []); // eslint-disable-line

  // Resume polling on mount if run was in progress
  useEffect(() => {
    if (runId && mgrState?.status && !["completed", "failed"].includes(mgrState.status)) {
      setRunning(true);
      _startPolling(runId);
    }
    return () => clearInterval(pollRef.current);
  }, []); // eslint-disable-line

  async function handleRun() {
    if (!csvFile || !savedPlan) return;
    setError(""); setRunning(true); setMgrState(null);
    try {
      const res = await manager.run(csvFile, savedPlan.config, savedSchema || {});
      const rid = res.run_id;
      setRunId(rid);
      setMgrState({ status: "validating", phase: "validating", step: "Starting…", decisions: [] });
      _startPolling(rid);
    } catch (e) {
      setRunning(false);
      setError("Failed to start: " + e.message);
    }
  }

  function reset() {
    clearInterval(pollRef.current);
    setRunning(false);
    setRunId(null);
    setMgrState(null);
    setError("");
  }

  const status = mgrState?.status;
  const isTerminal = status === "completed" || status === "failed";
  const canRun = !!csvFile && !!savedPlan && !running;

  return (
    <div style={S.page}>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>

      {/* Header */}
      <div style={S.header}>
        <div style={S.badge}><Activity size={13} /> Central Manager</div>
        <h1 style={S.title}>Orchestrator</h1>
        <p style={S.sub}>
          Validates the plan, predicts resources and cost, orchestrates all agents in sequence,
          retries failures, verifies output, and records outcomes for future improvement.
        </p>
      </div>

      {error && (
        <div style={S.errBox}><XCircle size={14} style={{ flexShrink: 0 }} />{error}</div>
      )}

      {/* Plan + CSV status */}
      <div style={S.grid2}>
        <div style={{ ...S.card, marginBottom: 0 }}>
          <div style={S.cardHdr}><Brain size={14} color="#a78bfa" />Pipeline Plan</div>
          {savedPlan ? (
            <>
              <div style={{ fontSize: 12, color: "#4ade80", marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
                <CheckCircle size={11} /> Loaded from Planner Agent
              </div>
              <div style={{ fontSize: 12, color: "#64748b" }}>
                {savedPlan.config?.stages?.length ?? 0} stages ·{" "}
                {savedPlan.config?.execution_order?.join(" → ")}
              </div>
              {savedPlan.used_fallback && (
                <div style={{ fontSize: 11, color: "#f59e0b", marginTop: 4 }}>
                  ⚠ Fallback config used
                </div>
              )}
            </>
          ) : (
            <div style={{ fontSize: 13, color: "#64748b" }}>
              No plan — generate one in{" "}
              <button onClick={() => navigate("/planner")} style={{ color: "#818cf8", background: "none", border: "none", cursor: "pointer", fontSize: 13 }}>
                Planner Agent
              </button>
            </div>
          )}
        </div>

        <div style={{ ...S.card, marginBottom: 0 }}>
          <div style={S.cardHdr}><Zap size={14} color="#f59e0b" />Data File</div>
          {csvFile ? (
            <>
              <div style={{ fontSize: 12, color: "#4ade80", marginBottom: 4, display: "flex", alignItems: "center", gap: 6 }}>
                <CheckCircle size={11} /> {csvFile.name}
              </div>
              <div style={{ fontSize: 11, color: "#64748b" }}>
                {(csvFile.size / 1024).toFixed(1)} KB · ready to execute
              </div>
            </>
          ) : (
            <div style={{ fontSize: 13, color: "#64748b" }}>
              No CSV — upload one in{" "}
              <button onClick={() => navigate("/planner")} style={{ color: "#818cf8", background: "none", border: "none", cursor: "pointer", fontSize: 13 }}>
                Planner
              </button>{" "}or{" "}
              <button onClick={() => navigate("/executor")} style={{ color: "#818cf8", background: "none", border: "none", cursor: "pointer", fontSize: 13 }}>
                Executor
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Run controls */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20, alignItems: "center" }}>
        <button
          style={S.btnPrimary(!canRun)}
          disabled={!canRun}
          onClick={handleRun}
        >
          <Activity size={14} /> Run via Manager
        </button>
        {(mgrState || error) && (
          <button style={S.btnSecondary} onClick={reset}>
            <RotateCcw size={13} /> Reset
          </button>
        )}
      </div>

      {/* Active / completed run */}
      {mgrState && (
        <>
          {/* Phase bar */}
          <div style={S.card}>
            <div style={S.cardHdr}><Activity size={14} color="#818cf8" />Orchestration Pipeline</div>
            <PhaseBar currentStatus={status} currentPhase={mgrState.phase} />

            {/* Current step */}
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
              {running && !isTerminal && <Spinner color="#818cf8" />}
              {status === "completed" && <CheckCircle size={14} color="#22c55e" />}
              {status === "failed"    && <XCircle    size={14} color="#f87171" />}
              <span style={{ fontSize: 13, color: status === "failed" ? "#f87171" : status === "completed" ? "#4ade80" : "#94a3b8" }}>
                {mgrState.step}
              </span>
              {mgrState.retries > 0 && (
                <span style={{ fontSize: 11, color: "#f59e0b", padding: "2px 8px", background: "#451a03", borderRadius: 20 }}>
                  {mgrState.retries} retry/retries
                </span>
              )}
            </div>

            {/* Error detail */}
            {status === "failed" && mgrState.error && (
              <div style={{ background: "#450a0a", borderRadius: 8, padding: "10px 14px", fontSize: 12, color: "#f87171", marginBottom: 12, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
                {mgrState.error}
              </div>
            )}

            {/* Completed result */}
            {status === "completed" && mgrState.executor_result && (
              <div style={{ background: "#0d2b0d", borderRadius: 8, padding: "10px 14px", marginBottom: 12 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: "#4ade80", marginBottom: 6, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <span>Pipeline completed successfully</span>
                  {mgrState.executor_result.sink_container && (
                    <a
                      href={executor.downloadUrl(mgrState.executor_result.sink_container)}
                      download
                      style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 12px", background: "#0ea5e9", color: "#fff", borderRadius: 8, fontSize: 12, fontWeight: 600, textDecoration: "none" }}
                    >
                      <Download size={12} /> Download CSV
                    </a>
                  )}
                </div>
                <div style={{ fontSize: 12, color: "#64748b" }}>
                  Stages: {mgrState.executor_result.stages?.join(" → ")}
                </div>
              </div>
            )}
          </div>

          {/* Decision audit log */}
          <div style={S.card}>
            <div style={S.cardHdr}><Shield size={14} color="#818cf8" />Decision Audit Log</div>
            <DecisionLog decisions={mgrState.decisions || []} />
          </div>

          {/* Pre-check cards */}
          {(mgrState.predictions?.stage_count || mgrState.cost_estimate?.total_usd !== undefined) && (
            <div style={S.grid2}>
              <PredictionsCard
                predictions={mgrState.predictions}
                cost={mgrState.cost_estimate}
                resourcePlan={mgrState.resource_plan}
              />
              <ParallelismCard parallelism={mgrState.parallelism} />
            </div>
          )}

          {/* Assurance */}
          {mgrState.assurance && Object.keys(mgrState.assurance).length > 0 && (
            <AssuranceCard assurance={mgrState.assurance} />
          )}

          {/* Plan summary */}
          {mgrState.plan_summary && (
            <div style={S.card}>
              <div style={S.cardHdr}><GitBranch size={14} color="#64748b" />Plan Summary</div>
              <div style={{ fontSize: 12, color: "#64748b" }}>
                {mgrState.plan_summary.stage_count} stages ·{" "}
                {mgrState.plan_summary.execution_order?.join(" → ")}
              </div>
              {mgrState.validation?.warnings?.length > 0 && (
                <div style={{ marginTop: 10 }}>
                  {mgrState.validation.warnings.map((w, i) => (
                    <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "#f59e0b", marginTop: 4 }}>
                      <AlertTriangle size={11} style={{ flexShrink: 0, marginTop: 1 }} />{w}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </>
      )}

      {/* Empty state */}
      {!mgrState && !error && (
        <div style={{ ...S.card, textAlign: "center", padding: "40px 20px" }}>
          <Activity size={40} style={{ marginBottom: 12, color: "#334155" }} />
          <div style={{ fontSize: 14, color: "#475569", marginBottom: 8 }}>
            Central Manager ready
          </div>
          <div style={{ fontSize: 12, color: "#334155" }}>
            Requires a plan from Planner Agent and a CSV file.
            {!savedPlan && (
              <> <button onClick={() => navigate("/planner")} style={{ color: "#818cf8", background: "none", border: "none", cursor: "pointer", fontSize: 12 }}>Generate a plan →</button></>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
