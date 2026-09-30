import React, { useState, useEffect, useCallback } from "react";
import {
  TrendingUp, AlertTriangle, CheckCircle, RefreshCw,
  Clock, Zap, Target, BarChart3, Activity, GitBranch,
} from "lucide-react";
import { perfPrediction, manager } from "../api.js";

// ── Styles (mirrors ResourceTab exactly) ─────────────────────────────────────
const S = {
  page:    { maxWidth: 960, margin: "0 auto" },
  heading: { fontSize: 22, fontWeight: 700, color: "var(--text)", marginBottom: 4 },
  sub:     { fontSize: 13, color: "var(--text-3)", marginBottom: 28 },
  grid2:   { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  grid4:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  card:    {
    background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 12,
    padding: "16px 20px", marginBottom: 16,
  },
  cardHdr: {
    display: "flex", alignItems: "center", gap: 8,
    fontSize: 13, fontWeight: 700, color: "var(--text)", marginBottom: 14,
  },
  kv:      { display: "flex", flexDirection: "column", gap: 6 },
  kvRow:   {
    display: "flex", justifyContent: "space-between", alignItems: "center",
    fontSize: 12, color: "var(--text-2)", paddingBottom: 6,
    borderBottom: "1px solid var(--divider)",
  },
  kvVal:   { color: "var(--text)", fontWeight: 600 },
  badge:   (color) => ({
    display: "inline-block", padding: "2px 8px", borderRadius: 99,
    fontSize: 11, fontWeight: 700,
    background: `color-mix(in srgb, ${color} 13%, transparent)`, color: color,
  }),
  btn:     {
    padding: "8px 16px", background: "var(--accent)", color: "var(--accent-fg)",
    border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
  },
  tag:     (color) => ({
    padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 700,
    background: `color-mix(in srgb, ${color} 13%, transparent)`, color: color, marginRight: 4,
  }),
  bar:     (pct, color) => ({
    height: 6, width: `${Math.min(pct, 100)}%`, background: color,
    borderRadius: 3, transition: "width 0.4s ease",
  }),
  barBg:   { height: 6, background: "var(--surface-2)", borderRadius: 3, marginTop: 4, overflow: "hidden" },
  stageRow: {
    padding: "10px 0", borderBottom: "1px solid var(--divider)",
    display: "flex", flexDirection: "column", gap: 4,
  },
  warn:    { display: "flex", gap: 6, fontSize: 11, color: "var(--warn)", marginTop: 6 },
  error:   { display: "flex", gap: 6, fontSize: 11, color: "var(--bad)", marginTop: 6 },
  empty:   {
    textAlign: "center", color: "var(--text-4)", fontSize: 13,
    padding: "32px 20px",
  },
};

// ── Colour helpers ────────────────────────────────────────────────────────────
const OUTCOME_COLOR = {
  success: "var(--ok)",
  slowdown: "var(--warn)",
  failure: "var(--bad)",
  unknown: "var(--text-3)",
};

const RISK_COLOR = {
  ok: "var(--ok)",
  warning: "var(--warn)",
  high: "var(--bad)",
};

function fmtSeconds(v) {
  if (!v) return "—";
  const s = Math.round(v);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return rem > 0 ? `${m}m ${rem}s` : `${m}m`;
}

// ── Shared mini-components ────────────────────────────────────────────────────
function Badge({ text, color = "var(--accent)" }) {
  return <span style={S.badge(color)}>{text}</span>;
}

function StatCard({ label, value, sub, color = "var(--accent)", Icon }) {
  return (
    <div style={S.card}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        {Icon && <Icon size={14} color={color} />}
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>{label}</span>
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "var(--text-4)", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

// ── Outcome banner ────────────────────────────────────────────────────────────
function OutcomeBanner({ pred }) {
  if (!pred) return null;
  const outcome = pred.outcome || "unknown";
  const color   = OUTCOME_COLOR[outcome] || "var(--text-3)";
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
      border: `1px solid color-mix(in srgb, ${color} 27%, transparent)`,
      background: `color-mix(in srgb, ${color} 7%, transparent)`,
      display: "flex", alignItems: "center", gap: 12, marginBottom: 16,
    }}>
      <Icon size={20} color={color} />
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color }}>{labels[outcome]}</div>
        {pred.rationale && (
          <div style={{ fontSize: 11, color: "var(--text-2)", marginTop: 4, lineHeight: 1.5 }}>
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
  const outcomeColor = OUTCOME_COLOR[pred.outcome] || "var(--text-3)";
  const confPct      = Math.round((pred.confidence || 0) * 100);
  const learned      = pred.expected_duration_basis === "history";
  const usualColor   = !learned ? "var(--text-2)" : pred.slower_than_usual ? "var(--bad)" : "var(--ok)";

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
          sub={learned ? `Usually ≤ ${fmtSeconds(pred.expected_duration_s)}` : "Usual duration: still learning"}
          Icon={Clock}
          color="var(--accent)"
        />
        <StatCard
          label="Outcome"
          value={pred.outcome ? pred.outcome.charAt(0).toUpperCase() + pred.outcome.slice(1) : "—"}
          sub={`Confidence: ${confPct}%`}
          Icon={Activity}
          color={outcomeColor}
        />
        <StatCard
          label="Slower than usual"
          value={!learned ? "—" : pred.slower_than_usual ? "Yes" : "No"}
          sub={!learned
            ? `Learning from this pipeline's runs (${pred.expected_duration_runs || 0}/3)`
            : pred.slower_than_usual
            ? "Predicted slower than this pipeline normally runs"
            : "Within this pipeline's normal duration"}
          Icon={Target}
          color={usualColor}
        />
        <StatCard
          label="Adjustment factor"
          value={pred.adjustment_factor != null ? `${pred.adjustment_factor}×` : "1.0×"}
          sub={`From ${pred.history_runs_used || 0} historical run(s)`}
          Icon={TrendingUp}
          color="var(--violet)"
        />
      </div>
      {/* Throughput row */}
      <div style={{ ...S.grid2, marginBottom: 16 }}>
        <StatCard
          label="Throughput (data volume)"
          value={throughputVal}
          sub={throughputSub}
          Icon={Zap}
          color="var(--warn)"
        />
        <StatCard
          label="Data processed"
          value={pred.throughput_mb_per_s != null
            ? `${(pred.throughput_mb_per_s * pred.predicted_total_s).toFixed(1)} MB`
            : "—"}
          sub="estimated total volume through pipeline"
          Icon={BarChart3}
          color="var(--accent)"
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
        <BarChart3 size={14} color="var(--accent)" />
        Stage Forecasts
      </div>
      {forecasts.map((f, i) => {
        const riskColor = RISK_COLOR[f.risk_level] || "var(--text-3)";
        const pct       = Math.min((f.predicted_s / 600) * 100, 100); // 600s = 100%
        return (
          <div key={f.name} style={{
            ...S.stageRow,
            ...(i === forecasts.length - 1 ? { borderBottom: "none" } : {}),
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 13, fontWeight: 600, color: "var(--text)" }}>{f.name}</span>
                {f.is_bottleneck && (
                  <span style={S.tag("var(--warn)")}>bottleneck</span>
                )}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Badge text={f.risk_level} color={riskColor} />
                <span style={{ fontSize: 12, color: "var(--text-2)" }}>
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
        <TrendingUp size={14} color="var(--ok)" />
        Recent Runs — Actual vs Predicted
        <span style={{ marginLeft: "auto", fontSize: 11, color: "var(--text-4)", fontWeight: 400 }}>
          last {history.length} run(s)
        </span>
      </div>
      <div style={S.kv}>
        {history.slice().reverse().map((r, i) => {
          const ratio      = r.predicted_duration_s > 0
            ? (r.actual_duration_s / r.predicted_duration_s).toFixed(2)
            : "—";
          const ratioNum   = parseFloat(ratio);
          const ratioColor = ratioNum <= 1.2 ? "var(--ok)" : ratioNum <= 2.0 ? "var(--warn)" : "var(--bad)";
          const passed     = r.assurance_passed;

          return (
            <div key={i} style={{
              ...S.kvRow,
              ...(i === history.length - 1 ? { borderBottom: "none" } : {}),
            }}>
              <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                <span style={{ fontSize: 12, color: "var(--text)", fontWeight: 600 }}>
                  {r.run_id ? r.run_id.slice(0, 8) : `Run ${i + 1}`}
                </span>
                <span style={{ fontSize: 10, color: "var(--text-4)" }}>
                  {r.complexity || "—"} · {r.stage_count || "?"} stage(s)
                  {r.ts ? ` · ${r.ts.slice(0, 16).replace("T", " ")}` : ""}
                </span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <span style={{ fontSize: 11, color: "var(--text-3)" }}>
                  actual {fmtSeconds(Math.round(r.actual_duration_s))} /
                  predicted {fmtSeconds(r.predicted_duration_s)}
                </span>
                <span style={{ ...S.kvVal, color: ratioColor }}>{ratio}×</span>
                <Badge
                  text={passed ? "passed" : passed === false ? "failed" : "?"}
                  color={passed ? "var(--ok)" : passed === false ? "var(--bad)" : "var(--text-3)"}
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
      <Activity size={32} color="var(--text-4)" style={{ marginBottom: 12 }} />
      <div style={{ color: "var(--text-3)", marginBottom: 6 }}>No prediction data yet</div>
      <div style={{ fontSize: 12, color: "var(--text-4)" }}>
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

      // 2. Newest manager run that got as far as a performance prediction
      //    (runs are newest-first; one that failed validation has none).
      const runs = await manager.listRuns();
      let pred = null;
      for (const r of (runs || []).slice(0, 10)) {
        const state = await manager.status(r.run_id).catch(() => null);
        if (state?.performance_prediction?.outcome) { pred = state.performance_prediction; break; }
      }
      setLatestPred(pred);
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
        <div style={{ ...S.card, border: "1px solid var(--bad)", color: "var(--bad)", fontSize: 12 }}>
          {err}
        </div>
      )}

      {/* Latest run prediction */}
      {hasPred ? (
        <>
          <div style={{ fontSize: 11, color: "var(--text-4)", marginBottom: 10 }}>
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
                <GitBranch size={14} color="var(--warn)" />
                Key Findings
              </div>
              <div style={S.kv}>
                <div style={S.kvRow}>
                  <span>Bottleneck stage</span>
                  <span style={{ ...S.kvVal, color: "var(--warn)" }}>{latestPred.bottleneck_stage}</span>
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
          <TrendingUp size={14} color="var(--ok)" />
          Prediction History
          {history.length === 0 && (
            <span style={{ marginLeft: 8, fontSize: 11, color: "var(--text-4)", fontWeight: 400 }}>
              — no data yet
            </span>
          )}
        </div>
        <HistorySection history={history} />
      </div>

      {/* How it works reference card */}
      <div style={S.card}>
        <div style={S.cardHdr}>
          <CheckCircle size={14} color="var(--text-3)" />
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
            <span>Usual duration</span>
            <span style={S.kvVal}>Learned per pipeline — p95 of its last 20 comparable runs (similar input size); cost savings may add at most 20%</span>
          </div>
        </div>
      </div>
    </div>
  );
}
