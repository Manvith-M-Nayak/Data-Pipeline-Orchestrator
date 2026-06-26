import React, { useState, useEffect, useCallback } from "react";
import {
  TrendingUp, AlertTriangle, CheckCircle, RefreshCw,
  Clock, Zap, Target, BarChart3, Activity, GitBranch,
} from "lucide-react";
import { perfPrediction, manager } from "../api.js";

// ── Styles (mirrors ResourceTab exactly) ─────────────────────────────────────
const S = {
  page:    { maxWidth: 960, margin: "0 auto" },
  heading: { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:     { fontSize: 13, color: "#64748b", marginBottom: 28 },
  grid2:   { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  grid4:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  card:    {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 12,
    padding: "16px 20px", marginBottom: 16,
  },
  cardHdr: {
    display: "flex", alignItems: "center", gap: 8,
    fontSize: 13, fontWeight: 700, color: "#f1f5f9", marginBottom: 14,
  },
  kv:      { display: "flex", flexDirection: "column", gap: 6 },
  kvRow:   {
    display: "flex", justifyContent: "space-between", alignItems: "center",
    fontSize: 12, color: "#94a3b8", paddingBottom: 6,
    borderBottom: "1px solid #0f172a",
  },
  kvVal:   { color: "#f1f5f9", fontWeight: 600 },
  badge:   (color) => ({
    display: "inline-block", padding: "2px 8px", borderRadius: 99,
    fontSize: 11, fontWeight: 700,
    background: color + "22", color: color,
  }),
  btn:     {
    padding: "8px 16px", background: "#0ea5e9", color: "#fff",
    border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
  },
  tag:     (color) => ({
    padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 700,
    background: color + "22", color: color, marginRight: 4,
  }),
  bar:     (pct, color) => ({
    height: 6, width: `${Math.min(pct, 100)}%`, background: color,
    borderRadius: 3, transition: "width 0.4s ease",
  }),
  barBg:   { height: 6, background: "#0f172a", borderRadius: 3, marginTop: 4, overflow: "hidden" },
  stageRow: {
    padding: "10px 0", borderBottom: "1px solid #0f172a",
    display: "flex", flexDirection: "column", gap: 4,
  },
  warn:    { display: "flex", gap: 6, fontSize: 11, color: "#f59e0b", marginTop: 6 },
  error:   { display: "flex", gap: 6, fontSize: 11, color: "#f87171", marginTop: 6 },
  empty:   {
    textAlign: "center", color: "#475569", fontSize: 13,
    padding: "32px 20px",
  },
};

// ── Colour helpers ────────────────────────────────────────────────────────────
const OUTCOME_COLOR = {
  success: "#4ade80",
  slowdown: "#f59e0b",
  failure: "#f87171",
  unknown: "#64748b",
};

const RISK_COLOR = {
  ok: "#4ade80",
  warning: "#f59e0b",
  high: "#f87171",
};

function fmtSeconds(s) {
  if (!s) return "—";
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return rem > 0 ? `${m}m ${rem}s` : `${m}m`;
}

// ── Shared mini-components ────────────────────────────────────────────────────
function Badge({ text, color = "#38bdf8" }) {
  return <span style={S.badge(color)}>{text}</span>;
}

function StatCard({ label, value, sub, color = "#38bdf8", Icon }) {
  return (
    <div style={S.card}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        {Icon && <Icon size={14} color={color} />}
        <span style={{ fontSize: 12, color: "#64748b" }}>{label}</span>
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#475569", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

// ── Outcome banner ────────────────────────────────────────────────────────────
function OutcomeBanner({ pred }) {
  if (!pred) return null;
  const outcome = pred.outcome || "unknown";
  const color   = OUTCOME_COLOR[outcome] || "#64748b";
  const icons   = { success: CheckCircle, slowdown: AlertTriangle, failure: AlertTriangle, unknown: Activity };
  const Icon    = icons[outcome] || Activity;
  const labels  = {
    success:  "Run predicted to succeed",
    slowdown: "Slowdown risk — run may take significantly longer than estimated",
    failure:  "Failure predicted — run was aborted before execution",
    unknown:  "Prediction unavailable",
  };

  return (
    <div style={{
      ...S.card,
      border: `1px solid ${color}44`,
      background: color + "11",
      display: "flex", alignItems: "center", gap: 12, marginBottom: 16,
    }}>
      <Icon size={20} color={color} />
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color }}>{labels[outcome]}</div>
        {pred.rationale && (
          <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4, lineHeight: 1.5 }}>
            {pred.rationale}
          </div>
        )}
      </div>
      <Badge text={outcome.toUpperCase()} color={color} />
    </div>
  );
}

// ── Summary stat cards ────────────────────────────────────────────────────────
function PredictionStats({ pred }) {
  if (!pred) return null;
  const outcomeColor = OUTCOME_COLOR[pred.outcome] || "#64748b";
  const confPct      = Math.round((pred.confidence || 0) * 100);
  const slaColor     = pred.sla_breach_risk ? "#f87171" : "#4ade80";

  const throughputVal = pred.throughput_mb_per_s != null
    ? `${pred.throughput_mb_per_s} MB/s`
    : pred.throughput_rows_per_s != null
    ? `${pred.throughput_rows_per_s} rows/s`
    : "—";

  const throughputSub = pred.throughput_mb_per_s != null && pred.throughput_rows_per_s != null
    ? `${pred.throughput_rows_per_s} rows/s`
    : pred.throughput_mb_per_s == null && pred.throughput_rows_per_s == null
    ? "file size or row count unknown"
    : null;

  return (
    <>
      <div style={S.grid4}>
        <StatCard
          label="Predicted total runtime"
          value={fmtSeconds(pred.predicted_total_s)}
          sub={`SLA target: ${fmtSeconds(pred.sla_target_s)}`}
          Icon={Clock}
          color="#38bdf8"
        />
        <StatCard
          label="Outcome"
          value={pred.outcome ? pred.outcome.charAt(0).toUpperCase() + pred.outcome.slice(1) : "—"}
          sub={`Confidence: ${confPct}%`}
          Icon={Activity}
          color={outcomeColor}
        />
        <StatCard
          label="SLA breach risk"
          value={pred.sla_breach_risk ? "Yes" : "No"}
          sub={pred.sla_breach_risk ? "Predicted to exceed target time" : "Within target time"}
          Icon={Target}
          color={slaColor}
        />
        <StatCard
          label="Adjustment factor"
          value={pred.adjustment_factor != null ? `${pred.adjustment_factor}×` : "1.0×"}
          sub={`From ${pred.history_runs_used || 0} historical run(s)`}
          Icon={TrendingUp}
          color="#c084fc"
        />
      </div>
      {/* Throughput row */}
      <div style={{ ...S.grid2, marginBottom: 16 }}>
        <StatCard
          label="Throughput (data volume)"
          value={throughputVal}
          sub={throughputSub}
          Icon={Zap}
          color="#f59e0b"
        />
        <StatCard
          label="Data processed"
          value={pred.throughput_mb_per_s != null
            ? `${(pred.throughput_mb_per_s * pred.predicted_total_s).toFixed(1)} MB`
            : "—"}
          sub="estimated total volume through pipeline"
          Icon={BarChart3}
          color="#38bdf8"
        />
      </div>
    </>
  );
}

// ── Per-stage forecast table ──────────────────────────────────────────────────
function StageForecasts({ forecasts }) {
  if (!forecasts || forecasts.length === 0) return null;

  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <BarChart3 size={14} color="#38bdf8" />
        Stage Forecasts
      </div>
      {forecasts.map((f, i) => {
        const riskColor = RISK_COLOR[f.risk_level] || "#64748b";
        const pct       = Math.min((f.predicted_s / 600) * 100, 100); // 600s = 100%
        return (
          <div key={f.name} style={{
            ...S.stageRow,
            ...(i === forecasts.length - 1 ? { borderBottom: "none" } : {}),
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 13, fontWeight: 600, color: "#f1f5f9" }}>{f.name}</span>
                {f.is_bottleneck && (
                  <span style={S.tag("#f59e0b")}>bottleneck</span>
                )}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Badge text={f.risk_level} color={riskColor} />
                <span style={{ fontSize: 12, color: "#94a3b8" }}>
                  <Clock size={10} style={{ marginRight: 3, verticalAlign: "middle" }} />
                  {fmtSeconds(f.predicted_s)}
                </span>
              </div>
            </div>
            <div style={S.barBg}>
              <div style={S.bar(pct, riskColor)} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── History accuracy section ──────────────────────────────────────────────────
function HistorySection({ history }) {
  if (!history || history.length === 0) {
    return (
      <div style={{ ...S.card, ...S.empty }}>
        No prediction history yet — run a pipeline through Central Manager to start collecting data.
      </div>
    );
  }

  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <TrendingUp size={14} color="#4ade80" />
        Recent Runs — Actual vs Predicted
        <span style={{ marginLeft: "auto", fontSize: 11, color: "#475569", fontWeight: 400 }}>
          last {history.length} run(s)
        </span>
      </div>
      <div style={S.kv}>
        {history.slice().reverse().map((r, i) => {
          const ratio      = r.predicted_duration_s > 0
            ? (r.actual_duration_s / r.predicted_duration_s).toFixed(2)
            : "—";
          const ratioNum   = parseFloat(ratio);
          const ratioColor = ratioNum <= 1.2 ? "#4ade80" : ratioNum <= 2.0 ? "#f59e0b" : "#f87171";
          const passed     = r.assurance_passed;

          return (
            <div key={i} style={{
              ...S.kvRow,
              ...(i === history.length - 1 ? { borderBottom: "none" } : {}),
            }}>
              <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                <span style={{ fontSize: 12, color: "#f1f5f9", fontWeight: 600 }}>
                  {r.run_id ? r.run_id.slice(0, 8) : `Run ${i + 1}`}
                </span>
                <span style={{ fontSize: 10, color: "#475569" }}>
                  {r.complexity || "—"} · {r.stage_count || "?"} stage(s)
                  {r.ts ? ` · ${r.ts.slice(0, 16).replace("T", " ")}` : ""}
                </span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <span style={{ fontSize: 11, color: "#64748b" }}>
                  actual {fmtSeconds(Math.round(r.actual_duration_s))} /
                  predicted {fmtSeconds(r.predicted_duration_s)}
                </span>
                <span style={{ ...S.kvVal, color: ratioColor }}>{ratio}×</span>
                <Badge
                  text={passed ? "passed" : passed === false ? "failed" : "?"}
                  color={passed ? "#4ade80" : passed === false ? "#f87171" : "#64748b"}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Empty state ───────────────────────────────────────────────────────────────
function EmptyState() {
  return (
    <div style={{ ...S.card, ...S.empty, padding: "48px 20px" }}>
      <Activity size={32} color="#334155" style={{ marginBottom: 12 }} />
      <div style={{ color: "#64748b", marginBottom: 6 }}>No prediction data yet</div>
      <div style={{ fontSize: 12, color: "#475569" }}>
        Run a pipeline through the Central Manager tab — the Performance Prediction Agent
        runs automatically during pre-checks and its output will appear here.
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function PerformancePredictionTab() {
  const [latestPred, setLatestPred]   = useState(null);   // from last manager run
  const [history, setHistory]         = useState([]);
  const [loading, setLoading]         = useState(false);
  const [err, setErr]                 = useState("");

  const fetchData = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      // 1. Pull history from the performance prediction agent
      const histRes = await perfPrediction.history();
      setHistory(histRes.records || []);

      // 2. Pull latest manager run and grab its performance_prediction block
      const runs = await manager.listRuns();
      if (runs && runs.length > 0) {
        // Runs are sorted newest-first by the backend
        const latestRun = runs[0];
        if (latestRun.run_id) {
          const state = await manager.status(latestRun.run_id);
          if (state && state.performance_prediction && state.performance_prediction.outcome) {
            setLatestPred(state.performance_prediction);
          }
        }
      }
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const hasPred = latestPred && latestPred.outcome;

  return (
    <div style={S.page}>
      <div style={S.heading}>Performance Prediction Agent</div>
      <div style={S.sub}>
        Forecasts total pipeline runtime, identifies the bottleneck stage, and predicts
        whether a run will succeed, slow down, or fail — all before execution starts.
      </div>

      {/* Refresh */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
        <button style={S.btn} onClick={fetchData} disabled={loading}>
          <RefreshCw size={13} />
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>

      {err && (
        <div style={{ ...S.card, border: "1px solid #f87171", color: "#f87171", fontSize: 12 }}>
          {err}
        </div>
      )}

      {/* Latest run prediction */}
      {hasPred ? (
        <>
          <div style={{ fontSize: 11, color: "#475569", marginBottom: 10 }}>
            LATEST RUN PREDICTION
          </div>

          {/* Outcome banner */}
          <OutcomeBanner pred={latestPred} />

          {/* Four stat cards */}
          <PredictionStats pred={latestPred} />

          {/* Per-stage breakdown */}
          <StageForecasts forecasts={latestPred.stage_forecasts} />

          {/* Bottleneck + execution groups */}
          {latestPred.bottleneck_stage && (
            <div style={S.card}>
              <div style={S.cardHdr}>
                <GitBranch size={14} color="#f59e0b" />
                Key Findings
              </div>
              <div style={S.kv}>
                <div style={S.kvRow}>
                  <span>Bottleneck stage</span>
                  <span style={{ ...S.kvVal, color: "#f59e0b" }}>{latestPred.bottleneck_stage}</span>
                </div>
                <div style={S.kvRow}>
                  <span>Confidence</span>
                  <span style={S.kvVal}>{Math.round((latestPred.confidence || 0) * 100)}%</span>
                </div>
                <div style={S.kvRow}>
                  <span>Historical runs informing this prediction</span>
                  <span style={S.kvVal}>{latestPred.history_runs_used ?? 0}</span>
                </div>
                <div style={{ ...S.kvRow, borderBottom: "none" }}>
                  <span>Adjustment factor applied</span>
                  <span style={S.kvVal}>{latestPred.adjustment_factor ?? 1.0}×</span>
                </div>
              </div>
            </div>
          )}
        </>
      ) : (
        !loading && <EmptyState />
      )}

      {/* History section — always shown once we have data */}
      <div style={S.card}>
        <div style={S.cardHdr}>
          <TrendingUp size={14} color="#4ade80" />
          Prediction History
          {history.length === 0 && (
            <span style={{ marginLeft: 8, fontSize: 11, color: "#475569", fontWeight: 400 }}>
              — no data yet
            </span>
          )}
        </div>
        <HistorySection history={history} />
      </div>

      {/* How it works reference card */}
      <div style={S.card}>
        <div style={S.cardHdr}>
          <CheckCircle size={14} color="#64748b" />
          How Predictions Are Made
        </div>
        <div style={S.kv}>
          <div style={S.kvRow}>
            <span>Baseline source</span>
            <span style={S.kvVal}>Resource Agent per-stage duration estimates</span>
          </div>
          <div style={S.kvRow}>
            <span>Critical path method</span>
            <span style={S.kvVal}>Slowest stage per parallel group, summed</span>
          </div>
          <div style={S.kvRow}>
            <span>History adjustment</span>
            <span style={S.kvVal}>Damped 40% toward mean(actual / predicted)</span>
          </div>
          <div style={S.kvRow}>
            <span>Minimum runs for history correction</span>
            <span style={S.kvVal}>5 runs</span>
          </div>
          <div style={S.kvRow}>
            <span>Slowdown threshold</span>
            <span style={S.kvVal}>≥ 1.6× resource estimate</span>
          </div>
          <div style={S.kvRow}>
            <span>Failure threshold</span>
            <span style={S.kvVal}>≥ 3.0× resource estimate or &gt;50% historical failure rate</span>
          </div>
          <div style={{ ...S.kvRow, borderBottom: "none" }}>
            <span>SLA target</span>
            <span style={S.kvVal}>900s (15 min) — student tier default</span>
          </div>
        </div>
      </div>
    </div>
  );
}
