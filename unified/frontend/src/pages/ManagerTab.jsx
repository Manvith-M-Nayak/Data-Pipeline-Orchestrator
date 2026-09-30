import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { manager, executor } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import StreamingConsole from "./StreamingConsole.jsx";
import {
  Shield, ShieldCheck, Brain, Zap, ClipboardCheck, TrendingUp, CheckCircle,
  XCircle, AlertTriangle, Clock, Activity, RotateCcw, Download,
  DollarSign, Cpu, GitBranch,
} from "lucide-react";

// ── Phase metadata ────────────────────────────────────────────────────────────
const PHASES = [
  { key: "validating",   label: "Validate", icon: Shield,         color: "var(--accent)" },
  { key: "assuring_plan", label: "Verify",  icon: ShieldCheck,    color: "var(--ok)" },
  { key: "pre_checks",  label: "Pre-checks", icon: Cpu,           color: "var(--accent)" },
  { key: "executing",   label: "Execute",   icon: Zap,            color: "var(--warn)" },
  { key: "assurance",   label: "Assurance", icon: ClipboardCheck, color: "var(--ok)" },
  { key: "feedback",    label: "Feedback",  icon: TrendingUp,     color: "var(--violet)" },
  { key: "completed",   label: "Done",      icon: CheckCircle,    color: "var(--ok)" },
];

const PHASE_ORDER = PHASES.map((p) => p.key);

// record_feedback() enters the "feedback" phase even for failed runs, so a
// failed run's `phase` is "feedback". The phase it actually failed in is the
// last one entered before that (decision log entries "PHASE:<NAME>").
function failedPhase(state) {
  const entered = (state?.decisions || [])
    .filter((d) => d.action?.startsWith("PHASE:"))
    .map((d) => d.action.slice(6).toLowerCase())
    .filter((p) => p !== "feedback");
  return entered.length ? entered[entered.length - 1] : state?.phase;
}

const S = {
  page: { maxWidth: 900, margin: "0 auto" },
  header: { marginBottom: 28 },
  badge: {
    padding: "4px 12px", background: "var(--accent-soft)", border: "1px solid var(--accent-line)",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "var(--accent)",
    display: "inline-flex", alignItems: "center", gap: 6, marginBottom: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "var(--text)", marginBottom: 4 },
  sub:    { fontSize: 13, color: "var(--text-3)" },
  card:   { background: "var(--surface)", borderRadius: 14, padding: 20, border: "1px solid var(--border)", marginBottom: 14 },
  cardHdr:{ fontSize: 14, fontWeight: 700, color: "var(--text)", marginBottom: 12, display: "flex", alignItems: "center", gap: 8 },
  grid2:  { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 },
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
  errBox: {
    background: "var(--bad-soft)", borderRadius: 8, padding: "10px 14px", marginBottom: 14,
    color: "var(--bad)", fontSize: 13, display: "flex", gap: 8,
  },
  decisionRow: (severity) => ({
    display: "flex", gap: 10, padding: "7px 10px",
    borderBottom: "1px solid var(--divider)", alignItems: "flex-start",
    background: severity === "error" ? "var(--bad-soft)" : severity === "warn" ? "var(--warn-soft)" : "transparent",
  }),
  tag: (severity) => ({
    fontSize: 11, fontWeight: 700, borderRadius: 4, padding: "2px 6px", flexShrink: 0,
    background:
      severity === "ok"    ? "var(--ok-soft)" :
      severity === "error" ? "var(--bad-soft)" :
      severity === "warn"  ? "var(--warn-soft)" : "var(--surface)",
    color:
      severity === "ok"    ? "var(--ok)" :
      severity === "error" ? "var(--bad)" :
      severity === "warn"  ? "var(--warn)" : "var(--text-3)",
  }),
  kv: { display: "flex", flexDirection: "column", gap: 6 },
  kvRow: { display: "flex", justifyContent: "space-between", fontSize: 12, color: "var(--text-2)", borderBottom: "1px solid var(--divider)", paddingBottom: 5 },
  kvVal: { color: "var(--text)", fontWeight: 600 },
  chip: (ok) => ({
    display: "inline-flex", alignItems: "center", gap: 4,
    padding: "2px 8px", borderRadius: 20, fontSize: 11, fontWeight: 600,
    background: ok ? "var(--ok-soft)" : ok === false ? "var(--bad-soft)" : "var(--surface)",
    color: ok ? "var(--ok)" : ok === false ? "var(--bad)" : "var(--text-2)",
  }),
};

function Spinner({ size = 14, color = "var(--accent)" }) {
  return (
    <span style={{
      display: "inline-block", width: size, height: size,
      border: "2px solid var(--border)", borderTopColor: color,
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
        // A failed run keeps the phase it failed in — mark that one red.
        const isFailed  = currentStatus === "failed" && i === activeIdx;
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
                  isFailed  ? "var(--bad-soft)" :
                  isActive  ? `color-mix(in srgb, ${p.color} 13%, transparent)` :
                  isDone    ? "var(--ok-soft)" : "var(--surface)",
                border: `2px solid ${
                  isFailed  ? "var(--bad)" :
                  isActive  ? p.color :
                  isDone    ? "var(--ok)" : "var(--border-strong)"
                }`,
                transition: "all 0.3s",
              }}>
                {isFailed ? (
                  <XCircle size={15} color="var(--bad)" />
                ) : isActive ? (
                  <Spinner size={13} color={p.color} />
                ) : isDone ? (
                  <CheckCircle size={15} color="var(--ok)" />
                ) : (
                  <Icon size={14} color="var(--text-4)" />
                )}
              </div>
              <span style={{
                fontSize: 11, fontWeight: isActive ? 700 : 400,
                color: isActive ? p.color : isDone ? "var(--ok)" : "var(--text-4)",
              }}>
                {p.label}
              </span>
            </div>
            {i < PHASES.length - 1 && (
              <div style={{
                flex: 1, height: 2, minWidth: 16,
                background: isDone ? "var(--ok)" : "var(--surface)",
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
    return <div style={{ fontSize: 13, color: "var(--text-4)", padding: "10px 0" }}>Waiting for decisions…</div>;
  }
  return (
    <div style={{ maxHeight: 240, overflowY: "auto", borderRadius: 8, border: "1px solid var(--divider)" }}>
      {decisions.map((d, i) => (
        <div key={i} style={S.decisionRow(d.severity)}>
          <span style={{ fontSize: 11, color: "var(--text-4)", flexShrink: 0, paddingTop: 2, minWidth: 72 }}>
            {d.ts?.slice(11, 19)}
          </span>
          <span style={S.tag(d.severity)}>{d.severity?.toUpperCase()}</span>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 12, color: "var(--text)", fontWeight: 600 }}>{d.action}</div>
            <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 2 }}>
              {d.reason}{d.outcome ? <span style={{ color: "var(--text-2)" }}> → {d.outcome}</span> : null}
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
        <Cpu size={14} color="var(--accent)" />Resource Prediction
        {resourcePlan && (
          <span style={{
            marginLeft: "auto", padding: "2px 8px", borderRadius: 99, fontSize: 11, fontWeight: 700,
            background: feasible ? "#4ade8022" : "#f8717122", color: feasible ? "var(--ok)" : "var(--bad)",
          }}>
            {feasible ? "Feasible" : "Infeasible"}
          </span>
        )}
      </div>

      {/* Constraint violations */}
      {violations.map((v, i) => (
        <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "var(--bad)", marginBottom: 4 }}>
          <AlertTriangle size={11} style={{ flexShrink: 0, marginTop: 1 }} />{v}
        </div>
      ))}
      {warnings.map((w, i) => (
        <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "var(--warn)", marginBottom: 4 }}>
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
        <div style={{ marginTop: 12, borderTop: "1px solid var(--divider)", paddingTop: 10 }}>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 8 }}>Stage allocations</div>
          {allocs.map((a) => (
            <div key={a.stage_name} style={{
              display: "flex", justifyContent: "space-between", alignItems: "center",
              fontSize: 11, color: "var(--text-2)", paddingBottom: 4, marginBottom: 4,
              borderBottom: "1px solid var(--divider)",
            }}>
              <span style={{ color: "var(--text)", fontWeight: 600, minWidth: 120 }}>{a.stage_name}</span>
              <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                {a.stage_type === "notebook"
                  ? <span>{a.workers}w · {a.memory_gb}GB · {a.cpu}vCPU</span>
                  : <span>{a.diu} DIU</span>}
                <span style={{ color: "var(--text-3)" }}>~{a.duration_s}s</span>
                {a.right_sized && <span style={{ color: "var(--ok)", fontSize: 11 }}>✔ right-sized</span>}
                {a.contention_adjusted && <span style={{ color: "var(--warn)", fontSize: 11 }}>⚠ adjusted</span>}
              </div>
            </div>
          ))}
        </div>
      )}

      {cost?.total_usd !== undefined && (
        <div style={{ marginTop: 12, padding: "8px 12px", borderRadius: 8, background: "var(--surface-2)", border: "1px solid var(--divider)" }}>
          <div style={{ fontSize: 12, color: "var(--text-3)", marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
            <DollarSign size={12} /> Cost estimate
          </div>
          <div style={S.kv}>
            <div style={S.kvRow}><span>ADF activities</span><span style={S.kvVal}>${cost.adf_activity_usd}</span></div>
            <div style={S.kvRow}><span>Databricks</span><span style={S.kvVal}>${cost.databricks_usd}</span></div>
            <div style={S.kvRow}><span>Blob storage</span><span style={S.kvVal}>${cost.storage_usd}</span></div>
            <div style={{ ...S.kvRow, borderBottom: "none" }}>
              <span style={{ fontWeight: 700, color: "var(--text)" }}>Total</span>
              <span style={{ ...S.kvVal, color: cost.budget_ok ? "var(--ok)" : "var(--warn)" }}>
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
      <div style={S.cardHdr}><GitBranch size={14} color="var(--violet)" />Parallelism Analysis</div>
      <div style={{ marginBottom: 10, fontSize: 12, color: "var(--text-2)" }}>
        {parallelism.can_parallelize
          ? `${parallelism.parallel_groups} group(s) can run in parallel`
          : "All stages run sequentially (linear dependency chain)"}
      </div>
      {parallelism.execution_groups.map((group, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
          <span style={{ fontSize: 11, color: "var(--text-4)", minWidth: 48 }}>Group {i + 1}</span>
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {group.map((name) => (
              <span key={name} style={{
                padding: "2px 8px", borderRadius: 6, fontSize: 11,
                background: group.length > 1 ? "var(--violet-soft)" : "var(--surface)",
                color: group.length > 1 ? "var(--violet)" : "var(--text-3)",
                border: `1px solid ${group.length > 1 ? "var(--violet-soft)" : "var(--border-strong)"}`,
              }}>
                {name}
              </span>
            ))}
          </div>
          {group.length > 1 && (
            <span style={{ fontSize: 11, color: "var(--violet)" }}>parallel</span>
          )}
        </div>
      ))}
    </div>
  );
}

// Shared context surfaced from the Planner into the Manager (hub view):
// the original request (editable), the detected schema, and the plan's
// per-stage transformations/filters/aggregations.
function ContextCard({ request, setRequest, disabled, detectedSchema, savedPlan }) {
  const cols = detectedSchema?.columns || {};            // {col: type}
  const colEntries = Object.entries(cols);
  const stages = savedPlan?.config?.stages || [];
  const transformStages = stages.filter(
    (s) => (s.transformations && s.transformations.length) || s.filter_condition || s.aggregation
  );

  return (
    <div style={S.card}>
      <div style={S.cardHdr}><Brain size={14} color="var(--violet)" />Request &amp; Context</div>

      {/* Editable user request */}
      <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 6 }}>
        User request (drives semantic intent check — edit before running)
      </div>
      <textarea
        value={request}
        disabled={disabled}
        onChange={(e) => setRequest(e.target.value)}
        placeholder="e.g. Ingest orders, then total revenue per category and region"
        style={{
          width: "100%", minHeight: 60, resize: "vertical", boxSizing: "border-box",
          background: "var(--surface-2)", color: "var(--text)", border: "1px solid var(--border)",
          borderRadius: 8, padding: "8px 10px", fontSize: 12, fontFamily: "inherit",
          opacity: disabled ? 0.6 : 1,
        }}
      />

      {/* Detected schema */}
      {colEntries.length > 0 && (
        <div style={{ marginTop: 12 }}>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 6 }}>
            Detected schema · {colEntries.length} columns
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6, maxHeight: 96, overflowY: "auto" }}>
            {colEntries.map(([col, type]) => (
              <span key={col} style={{
                padding: "2px 8px", borderRadius: 6, fontSize: 11,
                background: "var(--surface)", border: "1px solid var(--border)", color: "var(--text-2)",
              }}>
                {col}<span style={{ color: "var(--text-3)" }}> · {String(type)}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Plan transformations / filters / aggregations */}
      {transformStages.length > 0 && (
        <div style={{ marginTop: 12, borderTop: "1px solid var(--divider)", paddingTop: 10 }}>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 6 }}>Transformations &amp; filters</div>
          {transformStages.map((s) => (
            <div key={s.name} style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 12, color: "var(--text)", fontWeight: 600 }}>{s.name}</div>
              {(s.transformations || []).map((t, i) => (
                <div key={i} style={{ fontSize: 11, color: "var(--text-2)", fontFamily: "monospace", marginLeft: 8 }}>• {t}</div>
              ))}
              {s.filter_condition && (
                <div style={{ fontSize: 11, color: "var(--warn)", fontFamily: "monospace", marginLeft: 8 }}>filter: {s.filter_condition}</div>
              )}
              {s.aggregation && (
                <div style={{ fontSize: 11, color: "var(--accent)", fontFamily: "monospace", marginLeft: 8 }}>
                  group by [{(s.aggregation.group_by || []).join(", ")}] →{" "}
                  {(s.aggregation.aggregations || []).map((a) => `${a.op}(${a.column})`).join(", ")}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// How this run compares with the pipeline's own history (learned per pipeline,
// no fixed time limit). Older runs saved before this existed show "—".
function usualDuration(p) {
  if (!p || p.expected_duration_basis === undefined) return { label: "—", ok: true };
  if (p.expected_duration_basis !== "history") {
    return { label: `still learning (${p.expected_duration_runs || 0}/3 runs)`, ok: true };
  }
  return p.slower_than_usual
    ? { label: `⚠ slower (usually ≤${Math.round(p.expected_duration_s)}s)`, ok: false }
    : { label: `✔ within usual (≤${Math.round(p.expected_duration_s)}s)`, ok: true };
}

// Performance prediction (gap in the hub view — computed by the manager but
// not previously surfaced).
function PerformancePredictionCard({ perf }) {
  if (!perf || !perf.outcome) return null;
  const usual = usualDuration(perf);
  const ok = perf.outcome === "success" && usual.ok;
  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <TrendingUp size={14} color="var(--violet)" />Performance Prediction
        <span style={S.chip(ok)}>{perf.outcome}</span>
      </div>
      <div style={S.kv}>
        <div style={S.kvRow}><span>Predicted total</span><span style={S.kvVal}>~{perf.predicted_total_s}s</span></div>
        <div style={S.kvRow}><span>Bottleneck stage</span><span style={S.kvVal}>{perf.bottleneck_stage || "—"}</span></div>
        <div style={S.kvRow}><span>Confidence</span><span style={S.kvVal}>{Math.round((perf.confidence || 0) * 100)}%</span></div>
        <div style={S.kvRow}>
          <span>vs usual duration</span>
          <span style={S.kvVal}><span style={S.chip(usual.ok)}>{usual.label}</span></span>
        </div>
        {perf.history_runs_used !== undefined && (
          <div style={S.kvRow}><span>History runs used</span><span style={S.kvVal}>{perf.history_runs_used}</span></div>
        )}
      </div>
    </div>
  );
}

// Pre-execution plan verification (Assurance Agent: structural + semantic)
function PlanAssuranceCard({ planAssurance }) {
  if (!planAssurance || !planAssurance.summary) return null;
  const structural = planAssurance.structural_results || [];
  const sem = planAssurance.semantic_result;
  const passed = planAssurance.overall_status === "pass";

  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <ShieldCheck size={14} color="var(--ok)" />
        Plan Assurance
        <span style={S.chip(passed)}>{passed ? "PASSED" : "REJECTED"}</span>
      </div>

      <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 10 }}>
        {planAssurance.summary}
      </div>

      {/* Structural checks (deterministic) */}
      <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 6 }}>
        Structural checks (deterministic)
      </div>
      <div style={S.kv}>
        {structural.map((c) => (
          <div key={c.check} style={S.kvRow}>
            <span>{c.label}</span>
            <span style={S.kvVal}>
              <span style={S.chip(c.passed)}>{c.passed ? "✔ pass" : "✖ fail"}</span>
            </span>
          </div>
        ))}
      </div>

      {/* Per-check violation messages on failure */}
      {structural.filter((c) => !c.passed).map((c) => (
        <div key={c.check + "-msg"} style={{ display: "flex", gap: 6, fontSize: 11, color: "var(--bad)", marginTop: 6 }}>
          <AlertTriangle size={11} style={{ flexShrink: 0, marginTop: 1 }} />
          <span><b>{c.label}:</b> {c.message}</span>
        </div>
      ))}

      {/* Semantic check (advisory) */}
      {sem && (
        <div style={{ marginTop: 12, padding: "8px 12px", borderRadius: 8, background: "var(--surface-2)", border: "1px solid var(--divider)" }}>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 4, display: "flex", alignItems: "center", gap: 6 }}>
            <Brain size={12} /> Semantic intent check {sem.model ? `· ${sem.model}` : ""} (advisory)
          </div>
          {!sem.available ? (
            <div style={{ fontSize: 12, color: "var(--text-3)" }}>{sem.reasoning}</div>
          ) : (
            <>
              <span style={S.chip(!sem.flagged)}>
                {sem.flagged ? "⚠ possible mismatch" : "✔ matches request"}
              </span>
              <div style={{ fontSize: 12, color: "var(--text-2)", marginTop: 6 }}>{sem.reasoning}</div>
            </>
          )}
        </div>
      )}
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
        <ClipboardCheck size={14} color="var(--ok)" />
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
    plannerPrompt, setPlannerPrompt,
    detectedSchema,
    managerRunId: runId, setManagerRunId: setRunId,
    managerState: mgrState, setManagerState: setMgrState,
  } = useAppContext();

  // Editable user request — prefilled from the Planner prompt, can be edited
  // here and is sent to the Manager (drives the semantic assurance layer).
  const [request, setRequest] = useState(plannerPrompt || "");
  useEffect(() => { setRequest(plannerPrompt || ""); }, [plannerPrompt]);

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
        if (e?.status === 404) {
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

  // ── All runs (managed + mirrored executor runs) ────────────────────────────
  const [allRuns, setAllRuns] = useState([]);
  const refreshRuns = useCallback(async () => {
    try { setAllRuns(await manager.listRuns()); } catch { /* backend down — keep last */ }
  }, []);
  useEffect(() => {
    refreshRuns();
    const iv = setInterval(refreshRuns, 5000);
    return () => clearInterval(iv);
  }, [refreshRuns]);

  const isLive = (s) => !["completed", "failed"].includes(s);

  // Attach this tab to a run started elsewhere (Executor tab, another window)
  // so opening the Manager always shows what is currently executing.
  useEffect(() => {
    if (running || (mgrState && isLive(mgrState.status))) return;
    const live = allRuns.find((r) => isLive(r.status));
    if (live && live.run_id !== runId) {
      attachToRun(live.run_id);
    }
  }, [allRuns]); // eslint-disable-line

  function attachToRun(rid) {
    clearInterval(pollRef.current);
    setError("");
    setRunId(rid);
    manager.status(rid).then((s) => {
      setMgrState(s);
      if (isLive(s.status)) {
        setRunning(true);
        _startPolling(rid);
      } else {
        setRunning(false);
      }
    }).catch(() => {});
  }

  async function handleRun() {
    if (!csvFile || !savedPlan) return;
    setError(""); setRunning(true); setMgrState(null);
    try {
      setPlannerPrompt(request);   // persist any edits so other tabs stay in sync
      const res = await manager.run(csvFile, savedPlan.config, savedSchema || {}, request || "");
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
          <div style={S.cardHdr}><Brain size={14} color="var(--violet)" />Pipeline Plan</div>
          {savedPlan ? (
            <>
              <div style={{ fontSize: 12, color: "var(--ok)", marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
                <CheckCircle size={11} /> Loaded from Planner Agent
              </div>
              <div style={{ fontSize: 12, color: "var(--text-3)" }}>
                {savedPlan.config?.stages?.length ?? 0} stages ·{" "}
                {savedPlan.config?.execution_order?.join(" → ")}
              </div>
              {savedPlan.used_fallback && (
                <div style={{ fontSize: 11, color: "var(--warn)", marginTop: 4 }}>
                  ⚠ Fallback config used
                </div>
              )}
            </>
          ) : (
            <div style={{ fontSize: 13, color: "var(--text-3)" }}>
              No plan — generate one in{" "}
              <button onClick={() => navigate("/planner")} style={{ color: "var(--accent)", background: "none", border: "none", cursor: "pointer", fontSize: 13 }}>
                Planner Agent
              </button>
            </div>
          )}
        </div>

        <div style={{ ...S.card, marginBottom: 0 }}>
          <div style={S.cardHdr}><Zap size={14} color="var(--warn)" />Data File</div>
          {csvFile ? (
            <>
              <div style={{ fontSize: 12, color: "var(--ok)", marginBottom: 4, display: "flex", alignItems: "center", gap: 6 }}>
                <CheckCircle size={11} /> {csvFile.name}
              </div>
              <div style={{ fontSize: 11, color: "var(--text-3)" }}>
                {(csvFile.size / 1024).toFixed(1)} KB · ready to execute
              </div>
            </>
          ) : (
            <div style={{ fontSize: 13, color: "var(--text-3)" }}>
              No data file — upload one in{" "}
              <button onClick={() => navigate("/planner")} style={{ color: "var(--accent)", background: "none", border: "none", cursor: "pointer", fontSize: 13 }}>
                Planner
              </button>{" "}or{" "}
              <button onClick={() => navigate("/executor")} style={{ color: "var(--accent)", background: "none", border: "none", cursor: "pointer", fontSize: 13 }}>
                Executor
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Shared context from Planner (request + schema + transformations) */}
      {savedPlan && (
        <ContextCard
          request={request}
          setRequest={setRequest}
          disabled={running}
          detectedSchema={detectedSchema}
          savedPlan={savedPlan}
        />
      )}

      {/* Run controls — batch runs once; streaming runs live via the console below */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20, alignItems: "center" }}>
        <button
          style={S.btnPrimary(!canRun)}
          disabled={!canRun}
          onClick={handleRun}
        >
          <Activity size={14} /> {savedPlan?.config?.mode === "streaming" ? "Run once (seed data)" : "Run via Manager"}
        </button>
        {(mgrState || error) && (
          <button style={S.btnSecondary} onClick={reset}>
            <RotateCcw size={13} /> Reset
          </button>
        )}
      </div>

      {/* Streaming console — live incremental processing for streaming-mode plans */}
      {savedPlan?.config?.mode === "streaming" && (
        <StreamingConsole
          config={savedPlan.config}
          schema={savedSchema || (detectedSchema?.columns ? { columns: detectedSchema.columns } : {})}
          fileFormat={detectedSchema?.file_format || "csv"}
        />
      )}

      {/* All runs — managed here or mirrored from the Executor Agent */}
      {allRuns.length > 0 && (
        <div style={S.card}>
          <div style={S.cardHdr}><Clock size={14} color="var(--text-2)" />Recent Runs</div>
          {allRuns.slice(0, 8).map((r) => {
            const live = isLive(r.status);
            const selected = r.run_id === runId;
            const color = r.status === "completed" ? "var(--ok)" : r.status === "failed" ? "var(--bad)" : "var(--warn)";
            return (
              <div
                key={r.run_id}
                onClick={() => attachToRun(r.run_id)}
                style={{
                  display: "flex", alignItems: "center", gap: 10, padding: "8px 10px",
                  borderRadius: 8, cursor: "pointer", marginBottom: 4,
                  background: selected ? "var(--surface-2)" : "transparent",
                  border: `1px solid ${selected ? "var(--border-strong)" : "transparent"}`,
                }}
              >
                <span style={{
                  width: 8, height: 8, borderRadius: "50%", flexShrink: 0, background: color,
                  boxShadow: live ? `0 0 0 3px color-mix(in srgb, ${color} 20%, transparent)` : "none",
                }} />
                <span style={{ fontSize: 12, color: "var(--text-2)", fontFamily: "monospace" }}>
                  {r.run_id.slice(0, 8)}
                </span>
                <span style={{ fontSize: 12, color, fontWeight: 600 }}>{r.status}</span>
                <span style={{ fontSize: 12, color: "var(--text-3)", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {r.step}
                </span>
                <span style={{ fontSize: 11, color: "var(--text-4)", flexShrink: 0 }}>
                  {r.stage_count} stage(s) · {(r.started_at || "").slice(11, 19)}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* Active / completed run */}
      {mgrState && (
        <>
          {/* Phase bar */}
          <div style={S.card}>
            <div style={S.cardHdr}><Activity size={14} color="var(--accent)" />Orchestration Pipeline</div>
            <PhaseBar currentStatus={status} currentPhase={status === "failed" ? failedPhase(mgrState) : mgrState.phase} />

            {/* Current step */}
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
              {running && !isTerminal && <Spinner color="var(--accent)" />}
              {status === "completed" && <CheckCircle size={14} color="var(--ok)" />}
              {status === "failed"    && <XCircle    size={14} color="var(--bad)" />}
              <span style={{ fontSize: 13, color: status === "failed" ? "var(--bad)" : status === "completed" ? "var(--ok)" : "var(--text-2)" }}>
                {mgrState.step}
              </span>
              {mgrState.retries > 0 && (
                <span style={{ fontSize: 11, color: "var(--warn)", padding: "2px 8px", background: "var(--warn-soft)", borderRadius: 20 }}>
                  {mgrState.retries} {mgrState.retries === 1 ? "retry" : "retries"}
                </span>
              )}
            </div>

            {/* Error detail */}
            {status === "failed" && mgrState.error && (
              <div style={{ background: "var(--bad-soft)", borderRadius: 8, padding: "10px 14px", fontSize: 12, color: "var(--bad)", marginBottom: 12, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
                {mgrState.error}
              </div>
            )}

            {/* Completed result */}
            {status === "completed" && mgrState.executor_result && (
              <div style={{ background: "var(--ok-soft)", borderRadius: 8, padding: "10px 14px", marginBottom: 12 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: "var(--ok)", marginBottom: 6, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <span>Pipeline completed successfully</span>
                  {mgrState.executor_result.sink_container && (
                    <a
                      href="#"
                      onClick={(e) => { e.preventDefault(); executor.download(mgrState.executor_result.sink_container).catch((err) => setError(err.message)); }}
                      style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 12px", background: "var(--accent)", color: "var(--accent-fg)", borderRadius: 8, fontSize: 12, fontWeight: 600, textDecoration: "none" }}
                    >
                      <Download size={12} /> Download output
                    </a>
                  )}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-3)" }}>
                  Stages: {mgrState.executor_result.stages?.join(" → ")}
                </div>
              </div>
            )}
          </div>

          {/* Decision audit log */}
          <div style={S.card}>
            <div style={S.cardHdr}><Shield size={14} color="var(--accent)" />Decision Audit Log</div>
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

          {/* Performance prediction */}
          {mgrState.performance_prediction && (
            <PerformancePredictionCard perf={mgrState.performance_prediction} />
          )}

          {/* Plan assurance (pre-execution verification) */}
          {mgrState.plan_assurance && mgrState.plan_assurance.summary && (
            <PlanAssuranceCard planAssurance={mgrState.plan_assurance} />
          )}

          {/* Assurance (post-execution runtime checks) */}
          {mgrState.assurance && Object.keys(mgrState.assurance).length > 0 && (
            <AssuranceCard assurance={mgrState.assurance} />
          )}

          {/* Plan summary */}
          {mgrState.plan_summary && (
            <div style={S.card}>
              <div style={S.cardHdr}><GitBranch size={14} color="var(--text-3)" />Plan Summary</div>
              <div style={{ fontSize: 12, color: "var(--text-3)" }}>
                {mgrState.plan_summary.stage_count} stages ·{" "}
                {mgrState.plan_summary.execution_order?.join(" → ")}
              </div>
              {mgrState.validation?.warnings?.length > 0 && (
                <div style={{ marginTop: 10 }}>
                  {mgrState.validation.warnings.map((w, i) => (
                    <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "var(--warn)", marginTop: 4 }}>
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
          <Activity size={40} style={{ marginBottom: 12, color: "var(--text-4)" }} />
          <div style={{ fontSize: 14, color: "var(--text-4)", marginBottom: 8 }}>
            Central Manager ready
          </div>
          <div style={{ fontSize: 12, color: "var(--text-4)" }}>
            Requires a plan from Planner Agent and a data file (CSV or JSON).
            {!savedPlan && (
              <> <button onClick={() => navigate("/planner")} style={{ color: "var(--accent)", background: "none", border: "none", cursor: "pointer", fontSize: 12 }}>Generate a plan →</button></>
            )}
          </div>
        </div>
      )}
    </div>
  );
}