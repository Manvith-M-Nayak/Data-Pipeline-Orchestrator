import React, { useEffect, useState, useCallback } from "react";
import { manager } from "../api.js";
import {
  BarChart3, Activity, Brain, Zap, Shield, ShieldCheck, Cpu, TrendingUp,
  DollarSign, ClipboardCheck, Clock, ChevronDown, ChevronRight, AlertTriangle,
  CheckCircle, XCircle, GitBranch, RefreshCw, Download,
} from "lucide-react";

const S = {
  page:   { maxWidth: 1100, margin: "0 auto" },
  title:  { fontSize: 22, fontWeight: 700, marginBottom: 4, color: "#f1f5f9" },
  sub:    { fontSize: 13, color: "#64748b", marginBottom: 24 },
  grid4:  { display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, marginBottom: 20 },
  grid3:  { display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 14, marginBottom: 20 },
  grid2:  { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 20 },
  card:   { background: "#1e293b", borderRadius: 14, padding: 20, border: "1px solid #334155", marginBottom: 14 },
  cardHdr:{ fontSize: 13, fontWeight: 700, color: "#f1f5f9", marginBottom: 14, display: "flex", alignItems: "center", gap: 8 },
  statVal:{ fontSize: 30, fontWeight: 800, color: "#f1f5f9", lineHeight: 1, marginBottom: 3 },
  statSub:{ fontSize: 11, color: "#64748b" },
  kv:     { display: "flex", flexDirection: "column", gap: 6 },
  kvRow:  { display: "flex", justifyContent: "space-between", fontSize: 12, color: "#94a3b8", borderBottom: "1px solid #1e293b", paddingBottom: 5 },
  kvVal:  { color: "#f1f5f9", fontWeight: 600 },
  th:     { textAlign: "left", padding: "8px 10px", fontSize: 10, color: "#64748b", borderBottom: "1px solid #334155", textTransform: "uppercase", letterSpacing: 0.5 },
  td:     { padding: "8px 10px", fontSize: 12, borderBottom: "1px solid #1e293b", verticalAlign: "top" },
  decisionRow: (sev) => ({
    display: "flex", gap: 8, padding: "6px 8px", borderBottom: "1px solid #1e293b", alignItems: "flex-start",
    background: sev === "error" ? "rgba(127,29,29,0.12)" : sev === "warn" ? "rgba(120,53,15,0.08)" : "transparent",
  }),
  tag: (sev) => ({
    fontSize: 9, fontWeight: 700, borderRadius: 4, padding: "2px 5px", flexShrink: 0,
    background: sev === "ok" ? "#14532d" : sev === "error" ? "#7f1d1d" : sev === "warn" ? "#78350f" : "#1e293b",
    color: sev === "ok" ? "#4ade80" : sev === "error" ? "#f87171" : sev === "warn" ? "#fbbf24" : "#64748b",
  }),
  chip: (ok) => ({
    display: "inline-flex", alignItems: "center", gap: 4,
    padding: "2px 8px", borderRadius: 20, fontSize: 11, fontWeight: 600,
    background: ok ? "#14532d" : ok === false ? "#7f1d1d" : "#1e293b",
    color: ok ? "#4ade80" : ok === false ? "#f87171" : "#94a3b8",
  }),
  phaseRow: { display: "flex", alignItems: "center", gap: 6, marginBottom: 4, fontSize: 12 },
  phaseDot: (color) => ({ width: 6, height: 6, borderRadius: "50%", background: color, flexShrink: 0 }),
  sectionHdr: { fontSize: 11, fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 8, marginTop: 16 },
  detailCard: { background: "#0f172a", borderRadius: 10, padding: 14, border: "1px solid #1e293b", marginBottom: 10 },
  btnPrimary: {
    padding: "8px 16px", background: "#818cf8", color: "#0f172a", border: "none",
    borderRadius: 8, cursor: "pointer", fontSize: 12, fontWeight: 700,
    display: "inline-flex", alignItems: "center", gap: 6,
  },
};

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

function fmtSec(s) {
  if (!s) return "0s";
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`;
}

function StatCard({ icon: Icon, color, label, value, sub }) {
  return (
    <div style={S.card}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
        <Icon size={14} color={color} />
        <span style={{ fontSize: 11, fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</span>
      </div>
      <div style={{ ...S.statVal, color: color || "#f1f5f9" }}>{value}</div>
      {sub && <div style={S.statSub}>{sub}</div>}
    </div>
  );
}

function AgentHealthCard({ title, icon: Icon, color, metrics }) {
  const entries = Object.entries(metrics).filter(([, v]) => v !== null && v !== undefined);
  if (entries.length === 0) return null;
  return (
    <div style={S.detailCard}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
        <Icon size={13} color={color} />
        <span style={{ fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{title}</span>
      </div>
      <div style={S.kv}>
        {entries.map(([k, v]) => (
          <div key={k} style={S.kvRow}>
            <span>{k.replace(/_/g, " ")}</span>
            <span style={S.kvVal}>
              {typeof v === "number" && k.includes("usd") ? `$${v.toFixed(5)}` :
               typeof v === "number" && k.includes("rate") ? `${v}%` :
               typeof v === "number" && k.includes("pct") ? `${v}%` :
               String(v)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DecisionLog({ decisions }) {
  const [expanded, setExpanded] = useState(false);
  const shown = expanded ? decisions : decisions.slice(0, 15);

  if (!decisions.length) {
    return <div style={{ fontSize: 12, color: "#475569", padding: "8px 0" }}>No decisions recorded.</div>;
  }

  return (
    <div>
      <div style={{ borderRadius: 8, border: "1px solid #1e293b", overflow: "hidden" }}>
        {shown.map((d, i) => (
          <div key={i} style={S.decisionRow(d.severity)}>
            <span style={{ fontSize: 9, color: "#334155", flexShrink: 0, paddingTop: 2, minWidth: 64 }}>
              {d.ts?.slice(11, 19)}
            </span>
            <span style={S.tag(d.severity)}>{d.severity?.toUpperCase()}</span>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 11, color: "#f1f5f9", fontWeight: 600 }}>{d.action}</div>
              <div style={{ fontSize: 10, color: "#64748b", marginTop: 1 }}>
                {d.reason}{d.outcome ? <span style={{ color: "#94a3b8" }}> → {d.outcome}</span> : null}
              </div>
            </div>
          </div>
        ))}
      </div>
      {decisions.length > 15 && (
        <button
          onClick={() => setExpanded(!expanded)}
          style={{ background: "none", border: "none", color: "#818cf8", cursor: "pointer", fontSize: 11, padding: "6px 0", fontWeight: 600 }}
        >
          {expanded ? "Show less" : `Show all ${decisions.length} entries`}
        </button>
      )}
    </div>
  );
}

function RunDetail({ runId, onBack }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    setError("");
    manager.combinedRun(runId)
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [runId]);

  if (loading) return <div style={{ color: "#64748b", textAlign: "center", padding: 40 }}>Loading run details…</div>;
  if (error) return <div style={{ color: "#f87171", textAlign: "center", padding: 40 }}>Error: {error}</div>;
  if (!data) return null;

  const statusColor = data.status === "completed" ? "#4ade80" : data.status === "failed" ? "#f87171" : "#fbbf24";
  const fb = data.feedback || {};
  const ma = data.monitor_analysis || {};

  // Phase timeline from decisions
  const phases = data.decisions
    ? data.decisions.filter((d) => d.action?.startsWith("PHASE:"))
    : [];

  return (
    <div>
      {/* Back button + header */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
        <button onClick={onBack} style={{ ...S.btnPrimary, background: "transparent", color: "#64748b", border: "1px solid #334155" }}>
          ← Back to overview
        </button>
        <div>
          <div style={{ fontSize: 14, fontWeight: 700, color: "#f1f5f9" }}>
            Run <span style={{ fontFamily: "monospace" }}>{runId.slice(0, 8)}</span>
            <span style={{ ...S.chip(data.status === "completed"), marginLeft: 10 }}>{data.status}</span>
          </div>
          <div style={{ fontSize: 11, color: "#64748b" }}>
            {data.started_at} {data.completed_at ? `→ ${data.completed_at}` : ""}
          </div>
        </div>
      </div>

      {data.error && (
        <div style={{ background: "#450a0a", borderRadius: 8, padding: "10px 14px", marginBottom: 14, color: "#f87171", fontSize: 12, whiteSpace: "pre-wrap" }}>
          {data.error}
        </div>
      )}

      {/* Phase timeline */}
      {phases.length > 0 && (
        <div style={S.card}>
          <div style={S.cardHdr}><Activity size={13} color="#818cf8" />Phase Timeline</div>
          <div style={{ display: "flex", gap: 0, flexWrap: "wrap" }}>
            {phases.map((p, i) => {
              const label = p.action.replace("PHASE:", "").toLowerCase();
              const ok = p.outcome !== "abort";
              return (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 10px" }}>
                  <div style={S.phaseDot(ok ? "#22c55e" : "#f87171")} />
                  <span style={{ fontSize: 11, color: ok ? "#94a3b8" : "#f87171", fontWeight: i === phases.length - 1 ? 700 : 400 }}>{label}</span>
                  {i < phases.length - 1 && <span style={{ color: "#334155", margin: "0 2px" }}>→</span>}
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div style={S.grid2}>
        {/* Left column: agent results */}
        <div>
          {/* Validation */}
          {data.validation && (
            <div style={S.detailCard}>
              <div style={{ ...S.sectionHdr, marginTop: 0 }}>Plan Validation</div>
              <span style={S.chip(data.validation.ok)}>{data.validation.ok ? "PASSED" : "FAILED"}</span>
              {data.validation.issues?.length > 0 && (
                <div style={{ marginTop: 8 }}>
                  {data.validation.issues.map((issue, i) => (
                    <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "#f87171", marginBottom: 3 }}>
                      <XCircle size={10} style={{ flexShrink: 0, marginTop: 2 }} />{issue}
                    </div>
                  ))}
                </div>
              )}
              {data.validation.warnings?.length > 0 && (
                <div style={{ marginTop: 6 }}>
                  {data.validation.warnings.map((w, i) => (
                    <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "#fbbf24", marginBottom: 3 }}>
                      <AlertTriangle size={10} style={{ flexShrink: 0, marginTop: 2 }} />{w}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Plan Assurance */}
          {data.plan_assurance?.summary && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Plan Assurance (Pre-execution)</div>
              <span style={S.chip(data.plan_assurance.overall_status === "pass")}>
                {data.plan_assurance.overall_status === "pass" ? "PASSED" : "REJECTED"}
              </span>
              <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 6 }}>{data.plan_assurance.summary}</div>
              {(data.plan_assurance.structural_results || []).map((c, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
                  <span>{c.label}</span>
                  <span style={S.chip(c.passed)}>{c.passed ? "✔" : "✖"}</span>
                </div>
              ))}
              {data.plan_assurance.semantic_result && (
                <div style={{ marginTop: 8, padding: "6px 10px", background: "#0f172a", borderRadius: 6, fontSize: 11, color: "#94a3b8" }}>
                  Semantic: {data.plan_assurance.semantic_result.flagged ? "⚠ flagged" : "✔ matches"} — {data.plan_assurance.semantic_result.reasoning}
                </div>
              )}
            </div>
          )}

          {/* Resource Prediction */}
          {data.predictions?.stage_count && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Resource Prediction</div>
              <div style={S.kv}>
                <div style={S.kvRow}><span>File size</span><span style={S.kvVal}>{data.predictions.file_size_mb} MB</span></div>
                <div style={S.kvRow}><span>Stages</span><span style={S.kvVal}>{data.predictions.stage_count}</span></div>
                <div style={S.kvRow}><span>Complexity</span><span style={S.kvVal}>{data.predictions.complexity}</span></div>
                <div style={S.kvRow}><span>Peak workers</span><span style={S.kvVal}>{data.predictions.suggested_workers}</span></div>
                <div style={S.kvRow}><span>Est. duration</span><span style={S.kvVal}>~{data.predictions.estimated_duration_s}s</span></div>
                <div style={S.kvRow}><span>Feasible</span><span style={S.kvVal}><span style={S.chip(data.resource_plan?.feasible ?? true)}>{data.resource_plan?.feasible !== false ? "Yes" : "No"}</span></span></div>
              </div>
              {/* Per-stage allocations */}
              {(data.resource_plan?.allocations || []).length > 0 && (
                <div style={{ marginTop: 8, borderTop: "1px solid #1e293b", paddingTop: 8 }}>
                  {data.resource_plan.allocations.map((a) => (
                    <div key={a.stage_name} style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#94a3b8", padding: "3px 0" }}>
                      <span style={{ color: "#f1f5f9", fontWeight: 600 }}>{a.stage_name}</span>
                      <span>{a.stage_type === "notebook" ? `${a.workers}w · ${a.memory_gb}GB` : `${a.diu} DIU`} · ~{a.duration_s}s</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Performance Prediction */}
          {data.performance_prediction?.outcome && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Performance Prediction</div>
              <span style={S.chip(data.performance_prediction.outcome === "success")}>
                {data.performance_prediction.outcome}
              </span>
              <div style={S.kv}>
                <div style={S.kvRow}><span>Predicted total</span><span style={S.kvVal}>~{data.performance_prediction.predicted_total_s}s</span></div>
                <div style={S.kvRow}><span>Bottleneck</span><span style={S.kvVal}>{data.performance_prediction.bottleneck_stage || "—"}</span></div>
                <div style={S.kvRow}><span>Confidence</span><span style={S.kvVal}>{Math.round((data.performance_prediction.confidence || 0) * 100)}%</span></div>
                <div style={S.kvRow}><span>vs usual duration</span><span style={S.kvVal}><span style={S.chip(usualDuration(data.performance_prediction).ok)}>{usualDuration(data.performance_prediction).label}</span></span></div>
                {data.performance_prediction.prediction_source && (
                  <div style={S.kvRow}><span>Source</span><span style={S.kvVal}>{data.performance_prediction.prediction_source}</span></div>
                )}
                {data.performance_prediction.learning_correction_applied && (
                  <div style={S.kvRow}><span>Learning correction</span><span style={S.kvVal}>×{data.performance_prediction.learning_correction_applied}</span></div>
                )}
              </div>
            </div>
          )}

          {/* Cost Estimation */}
          {data.cost_estimate?.total_usd !== undefined && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Cost Estimation</div>
              <div style={S.kv}>
                <div style={S.kvRow}><span>ADF activities</span><span style={S.kvVal}>${data.cost_estimate.adf_activity_usd}</span></div>
                <div style={S.kvRow}><span>Databricks</span><span style={S.kvVal}>${data.cost_estimate.databricks_usd}</span></div>
                <div style={S.kvRow}><span>Storage</span><span style={S.kvVal}>${data.cost_estimate.storage_usd}</span></div>
                <div style={{ ...S.kvRow, borderBottom: "none" }}>
                  <span style={{ fontWeight: 700, color: "#f1f5f9" }}>Total</span>
                  <span style={{ ...S.kvVal, color: data.cost_estimate.budget_ok ? "#4ade80" : "#f59e0b" }}>${data.cost_estimate.total_usd}</span>
                </div>
              </div>
            </div>
          )}

          {/* Cost Optimization */}
          {data.cost_optimization?.estimated_cost && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Cost Optimization</div>
              <div style={S.kv}>
                <div style={S.kvRow}>
                  <span>Optimized cost</span>
                  <span style={S.kvVal}>${data.cost_optimization.estimated_cost.total_usd}</span>
                </div>
                {data.cost_optimization.cost_correction_applied && (
                  <div style={S.kvRow}>
                    <span>Learning correction</span>
                    <span style={S.kvVal}>×{data.cost_optimization.cost_correction_applied}</span>
                  </div>
                )}
              </div>
              {(data.cost_optimization.recommendations || []).length > 0 && (
                <div style={{ marginTop: 8 }}>
                  {data.cost_optimization.recommendations.map((r, i) => (
                    <div key={i} style={{ display: "flex", gap: 6, fontSize: 11, color: "#94a3b8", marginBottom: 3 }}>
                      <DollarSign size={10} style={{ flexShrink: 0, marginTop: 2 }} />
                      <span><b>{r.action}</b> — {r.estimated_saving}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Right column: assurance + feedback + logs */}
        <div>
          {/* Post-execution Assurance */}
          {data.assurance && Object.keys(data.assurance).length > 0 && (
            <div style={S.detailCard}>
              <div style={{ ...S.sectionHdr, marginTop: 0 }}>Post-execution Assurance</div>
              <span style={S.chip(data.assurance.passed)}>{data.assurance.passed ? "PASSED" : "WARNINGS"}</span>
              <div style={S.kv}>
                {data.assurance.actual_duration_s !== undefined && (
                  <div style={S.kvRow}><span>Actual duration</span><span style={S.kvVal}>{data.assurance.actual_duration_s}s</span></div>
                )}
                {data.assurance.predicted_duration_s !== undefined && (
                  <div style={S.kvRow}><span>Predicted duration</span><span style={S.kvVal}>{data.assurance.predicted_duration_s}s</span></div>
                )}
                {data.assurance.timing_ratio !== undefined && (
                  <div style={S.kvRow}><span>Timing ratio</span><span style={S.kvVal}>{data.assurance.timing_ratio}×</span></div>
                )}
                {data.assurance.retries_used !== undefined && (
                  <div style={S.kvRow}><span>Retries</span><span style={S.kvVal}>{data.assurance.retries_used}</span></div>
                )}
              </div>
            </div>
          )}

          {/* Feedback Record */}
          {fb.run_id && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Feedback Record</div>
              <div style={S.kv}>
                {fb.actual_duration_s !== undefined && (
                  <div style={S.kvRow}><span>Actual duration</span><span style={S.kvVal}>{fb.actual_duration_s}s</span></div>
                )}
                {fb.predicted_duration_s !== undefined && (
                  <div style={S.kvRow}><span>Predicted duration</span><span style={S.kvVal}>{fb.predicted_duration_s}s</span></div>
                )}
                {fb.perf_predicted_total_s !== undefined && (
                  <div style={S.kvRow}><span>Perf predicted total</span><span style={S.kvVal}>{fb.perf_predicted_total_s}s</span></div>
                )}
                {fb.estimated_cost_usd !== undefined && fb.estimated_cost_usd !== null && (
                  <div style={S.kvRow}><span>Estimated cost</span><span style={S.kvVal}>${fb.estimated_cost_usd}</span></div>
                )}
                {fb.actual_cost_usd !== undefined && fb.actual_cost_usd !== null && (
                  <div style={S.kvRow}><span>Actual cost</span><span style={S.kvVal}>${fb.actual_cost_usd}</span></div>
                )}
                {fb.prediction_source && (
                  <div style={S.kvRow}><span>Prediction source</span><span style={S.kvVal}>{fb.prediction_source}</span></div>
                )}
                {fb.complexity && (
                  <div style={S.kvRow}><span>Complexity</span><span style={S.kvVal}>{fb.complexity}</span></div>
                )}
              </div>
            </div>
          )}

          {/* Monitor Analysis */}
          {ma.status_summary && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Monitor Analysis</div>
              <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 6 }}>{ma.status_summary}</div>
              {ma.explanation && <div style={{ fontSize: 11, color: "#64748b", marginBottom: 6 }}>{ma.explanation}</div>}
              {ma.root_cause && (
                <div style={{ fontSize: 11, color: "#fbbf24", marginBottom: 4 }}>Root cause: {ma.root_cause}</div>
              )}
              {ma.severity && (
                <span style={{ fontSize: 11, color: ma.severity === "high" ? "#f97316" : ma.severity === "medium" ? "#f59e0b" : "#22c55e", fontWeight: 600 }}>
                  Severity: {ma.severity}
                </span>
              )}
            </div>
          )}

          {/* Parallelism */}
          {data.parallelism?.execution_groups && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>Parallelism</div>
              <div style={{ fontSize: 11, color: "#94a3b8", marginBottom: 6 }}>
                {data.parallelism.can_parallelize
                  ? `${data.parallelism.parallel_groups} parallel group(s)`
                  : "All sequential"}
              </div>
              {data.parallelism.execution_groups.map((group, i) => (
                <div key={i} style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 4 }}>
                  <span style={{ fontSize: 9, color: "#475569", minWidth: 40 }}>G{i + 1}</span>
                  {group.map((name) => (
                    <span key={name} style={{
                      padding: "1px 6px", borderRadius: 4, fontSize: 10,
                      background: group.length > 1 ? "#2d1b69" : "#1e293b",
                      color: group.length > 1 ? "#c084fc" : "#64748b",
                    }}>{name}</span>
                  ))}
                </div>
              ))}
            </div>
          )}

          {/* User Request */}
          {data.user_request && (
            <div style={S.detailCard}>
              <div style={S.sectionHdr}>User Request</div>
              <div style={{ fontSize: 12, color: "#94a3b8", fontStyle: "italic" }}>"{data.user_request}"</div>
            </div>
          )}
        </div>
      </div>

      {/* Full Decision Log */}
      <div style={S.card}>
        <div style={S.cardHdr}><Shield size={13} color="#818cf8" />Decision Audit Log ({data.decisions?.length || 0} entries)</div>
        <DecisionLog decisions={data.decisions || []} />
      </div>

      {/* Executor Result */}
      {data.executor_result && (
        <div style={S.card}>
          <div style={S.cardHdr}><Zap size={13} color="#f59e0b" />Executor Result</div>
          <div style={{ fontSize: 12, color: "#94a3b8" }}>
            Status: <span style={{ color: data.executor_result.status === "ok" ? "#4ade80" : "#f87171" }}>{data.executor_result.status}</span>
            {data.executor_result.stages?.length > 0 && (
              <> · Stages: {data.executor_result.stages.join(" → ")}</>
            )}
            {data.executor_result.sink_container && (
              <> · Output: {data.executor_result.sink_container}</>
            )}
          </div>
          {data.executor_result.sink_container && (
            <a
              href={`/api/executor/download/${encodeURIComponent(data.executor_result.sink_container)}`}
              download
              style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 12px", background: "#0ea5e9", color: "#fff", borderRadius: 8, fontSize: 12, fontWeight: 600, textDecoration: "none", marginTop: 8 }}
            >
              <Download size={12} /> Download output
            </a>
          )}
        </div>
      )}
    </div>
  );
}

export default function RunInsights() {
  const [analytics, setAnalytics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedRun, setSelectedRun] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setAnalytics(await manager.analytics());
    } catch {}
    finally { setLoading(false); }
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading && !analytics) {
    return <div style={{ color: "#64748b", textAlign: "center", padding: 60 }}>Loading analytics…</div>;
  }

  if (selectedRun) {
    return <RunDetail runId={selectedRun} onBack={() => setSelectedRun(null)} />;
  }

  const s = analytics?.summary || {};
  const da = analytics?.duration_accuracy || {};
  const ca = analytics?.cost_accuracy || {};
  const ah = analytics?.agent_health || {};
  const runs = analytics?.runs || [];

  return (
    <div style={S.page}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
        <div>
          <h1 style={S.title}>Run Insights</h1>
          <p style={S.sub}>Combined results and logs from all agents across every pipeline run.</p>
        </div>
        <button onClick={load} style={{ padding: "6px 12px", background: "transparent", color: "#475569", border: "1px solid #334155", borderRadius: 8, cursor: "pointer", fontSize: 12, display: "flex", alignItems: "center", gap: 5 }}>
          <RefreshCw size={12} /> Refresh
        </button>
      </div>

      {/* Summary stats */}
      <div style={S.grid4}>
        <StatCard icon={Activity} color="#38bdf8" label="Total Runs" value={s.total_runs ?? 0} sub={`${s.completed ?? 0} completed · ${s.failed ?? 0} failed`} />
        <StatCard icon={CheckCircle} color="#22c55e" label="Success Rate" value={`${s.success_rate_pct ?? 0}%`} sub={`${s.in_progress ?? 0} in progress`} />
        <StatCard icon={TrendingUp} color="#c084fc" label="Duration Accuracy" value={da.avg_predicted_vs_actual_ratio ? `${da.avg_predicted_vs_actual_ratio}×` : "—"} sub={`${da.samples || 0} samples`} />
        <StatCard icon={DollarSign} color="#fbbf24" label="Cost Accuracy" value={ca.avg_error_pct !== null ? `${ca.avg_error_pct}%` : "—"} sub={`$${ca.total_estimated_usd?.toFixed(4) || 0} est. · $${ca.total_actual_usd?.toFixed(4) || 0} actual`} />
      </div>

      {/* Agent health */}
      <div style={S.sectionHdr}>Agent Health</div>
      <div style={S.grid3}>
        <AgentHealthCard title="Planner Agent" icon={Brain} color="#a78bfa" metrics={ah.planner || {}} />
        <AgentHealthCard title="Assurance Agent" icon={ShieldCheck} color="#34d399" metrics={ah.assurance || {}} />
        <AgentHealthCard title="Resource Agent" icon={Cpu} color="#38bdf8" metrics={ah.resource || {}} />
        <AgentHealthCard title="Performance Prediction" icon={TrendingUp} color="#c084fc" metrics={ah.performance_prediction || {}} />
        <AgentHealthCard title="Cost Optimization" icon={DollarSign} color="#fbbf24" metrics={ah.cost_optimization || {}} />
        <AgentHealthCard title="Executor Agent" icon={Zap} color="#f59e0b" metrics={ah.executor || {}} />
      </div>

      {/* Run list */}
      <div style={S.card}>
        <div style={S.cardHdr}><Clock size={13} color="#94a3b8" />All Runs ({runs.length})</div>
        {runs.length === 0 ? (
          <div style={{ color: "#475569", textAlign: "center", padding: 20, fontSize: 12 }}>No runs recorded yet.</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={S.th} />
                  <th style={S.th}>Run ID</th>
                  <th style={S.th}>Status</th>
                  <th style={S.th}>Stages</th>
                  <th style={S.th}>Duration</th>
                  <th style={S.th}>Cost Est.</th>
                  <th style={S.th}>Cost Actual</th>
                  <th style={S.th}>Assurance</th>
                  <th style={S.th}>Source</th>
                  <th style={S.th}>Started</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((r) => (
                  <tr
                    key={r.run_id}
                    onClick={() => setSelectedRun(r.run_id)}
                    style={{ cursor: "pointer", background: "transparent" }}
                    onMouseEnter={(e) => e.currentTarget.style.background = "#0f172a"}
                    onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}
                  >
                    <td style={S.td}><ChevronRight size={12} color="#334155" /></td>
                    <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{r.run_id?.slice(0, 8)}…</td>
                    <td style={S.td}>
                      <span style={{
                        padding: "2px 6px", borderRadius: 8, fontSize: 10, fontWeight: 700,
                        background: r.status === "completed" ? "#14532d" : r.status === "failed" ? "#7f1d1d" : "#1e293b",
                        color: r.status === "completed" ? "#4ade80" : r.status === "failed" ? "#f87171" : "#94a3b8",
                      }}>{r.status}</span>
                    </td>
                    <td style={S.td}>{r.stage_count}</td>
                    <td style={S.td}>
                      {r.actual_duration_s ? `${r.actual_duration_s}s` : "—"}
                      {r.predicted_duration_s && <span style={{ color: "#64748b", fontSize: 10 }}> (pred: {r.predicted_duration_s}s)</span>}
                    </td>
                    <td style={S.td}>{r.cost_estimate_usd != null ? `$${r.cost_estimate_usd}` : "—"}</td>
                    <td style={S.td}>{r.actual_cost_usd != null ? `$${r.actual_cost_usd}` : "—"}</td>
                    <td style={S.td}>
                      {r.assurance_passed != null ? (
                        <span style={S.chip(r.assurance_passed)}>{r.assurance_passed ? "✔" : "✖"}</span>
                      ) : "—"}
                    </td>
                    <td style={S.td}>{r.prediction_source || "—"}</td>
                    <td style={{ ...S.td, fontSize: 10, color: "#475569" }}>{(r.started_at || "").slice(0, 16)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
