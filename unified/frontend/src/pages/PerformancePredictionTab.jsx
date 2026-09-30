import React, { useState, useEffect, useCallback } from "react";
import {
  Activity, AlertTriangle, BarChart3, CheckCircle2, Clock, Gauge, RefreshCw, Target, TrendingUp, Zap,
} from "lucide-react";
import { perfPrediction, manager } from "../api.js";
import { Alert, Badge, Button, Card, Empty, KV, PageHeader, Stat } from "../ui/components.jsx";

const OUTCOME = {
  success:  { tone: "ok",      icon: CheckCircle2,  text: "Run predicted to succeed" },
  slowdown: { tone: "warn",    icon: AlertTriangle, text: "Slowdown risk — may take much longer than estimated" },
  failure:  { tone: "bad",     icon: AlertTriangle, text: "Failure predicted — the run was stopped before execution" },
  unknown:  { tone: "neutral", icon: Activity,      text: "Prediction unavailable" },
};
const RISK_TONE = { ok: "ok", warning: "warn", high: "bad" };

function fmtSeconds(v) {
  if (!v) return "—";
  const s = Math.round(v);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), rem = s % 60;
  return rem > 0 ? `${m}m ${rem}s` : `${m}m`;
}

function LatestPrediction({ pred }) {
  const o = OUTCOME[pred.outcome] || OUTCOME.unknown;
  const Icon = o.icon;
  const learned = pred.expected_duration_basis === "history";
  const forecasts = pred.stage_forecasts || [];
  const maxS = Math.max(600, ...forecasts.map((f) => f.predicted_s || 0));

  return (
    <div className="stack" style={{ gap: 14 }}>
      <div className="big-status" style={{ background: `var(--${o.tone === "neutral" ? "surface" : `${o.tone}-soft`})`, borderColor: `var(--${o.tone === "neutral" ? "border" : `${o.tone}-line`})` }}>
        <div className="icon" style={{ background: "var(--surface)", color: `var(--${o.tone === "neutral" ? "text-3" : o.tone})` }}><Icon size={20} /></div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontWeight: 600, color: "var(--text)" }}>{o.text}</div>
          {pred.rationale && <div className="muted" style={{ fontSize: 12.5, marginTop: 2, lineHeight: 1.5 }}>{pred.rationale}</div>}
        </div>
        <Badge tone={o.tone}>{Math.round((pred.confidence || 0) * 100)}% confidence</Badge>
      </div>

      <div className="grid grid-4">
        <Stat icon={Clock} label="Predicted runtime" value={fmtSeconds(pred.predicted_total_s)}
          sub={learned ? `usually ≤ ${fmtSeconds(pred.expected_duration_s)}` : "usual duration: still learning"} />
        <Stat icon={Target} label="Slower than usual"
          value={!learned ? "—" : pred.slower_than_usual ? "Yes" : "No"}
          tone={!learned ? undefined : pred.slower_than_usual ? "bad" : "ok"}
          sub={!learned ? `learning from this pipeline (${pred.expected_duration_runs || 0}/3 runs)` : "vs this pipeline's own history"} />
        <Stat icon={TrendingUp} label="Adjustment factor" value={`${pred.adjustment_factor ?? 1}×`}
          sub={`from ${pred.history_runs_used || 0} past run(s)`} />
        <Stat icon={Zap} label="Throughput"
          value={pred.throughput_mb_per_s != null ? `${pred.throughput_mb_per_s} MB/s` : pred.throughput_rows_per_s != null ? `${pred.throughput_rows_per_s} rows/s` : "—"}
          sub={pred.throughput_mb_per_s != null && pred.throughput_rows_per_s != null ? `${pred.throughput_rows_per_s} rows/s` : pred.throughput_mb_per_s == null && pred.throughput_rows_per_s == null ? "file size unknown" : undefined} />
      </div>

      {forecasts.length > 0 && (
        <Card title="Stage forecasts" icon={BarChart3}
          subtitle={pred.bottleneck_stage ? <>Bottleneck: <span className="mono">{pred.bottleneck_stage}</span></> : undefined}>
          <div className="stack" style={{ gap: 12 }}>
            {forecasts.map((f) => (
              <div key={f.name}>
                <div className="row" style={{ gap: 8, marginBottom: 5, fontSize: 13 }}>
                  <span className="mono" style={{ color: "var(--text)" }}>{f.name}</span>
                  {f.is_bottleneck && <Badge tone="warn">bottleneck</Badge>}
                  <span className="muted" style={{ marginLeft: "auto" }}>{fmtSeconds(f.predicted_s)}</span>
                  <Badge tone={RISK_TONE[f.risk_level] || "neutral"}>{f.risk_level}</Badge>
                </div>
                <div style={{ height: 6, background: "var(--surface-2)", borderRadius: 3, overflow: "hidden" }}>
                  <div style={{ height: "100%", width: `${Math.min(100, ((f.predicted_s || 0) / maxS) * 100)}%`, background: `var(--${RISK_TONE[f.risk_level] || "accent"})` }} />
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}

function History({ history }) {
  if (!history.length) {
    return <Empty icon={TrendingUp} title="No history yet">Each Central Manager run records predicted vs actual runtime here.</Empty>;
  }
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="table">
        <thead><tr><th>Run</th><th>When</th><th>Shape</th><th>Actual</th><th>Predicted</th><th>Ratio</th><th>Checks</th></tr></thead>
        <tbody>
          {history.slice().reverse().map((r, i) => {
            const ratio = r.predicted_duration_s > 0 ? r.actual_duration_s / r.predicted_duration_s : null;
            return (
              <tr key={r.run_id || i}>
                <td className="mono" style={{ color: "var(--text)" }}>{r.run_id ? r.run_id.slice(0, 8) : `#${i + 1}`}</td>
                <td className="muted">{r.ts ? r.ts.slice(0, 16).replace("T", " ") : "—"}</td>
                <td className="muted">{r.complexity || "—"} · {r.stage_count || "?"} stage(s)</td>
                <td>{fmtSeconds(r.actual_duration_s)}</td>
                <td>{fmtSeconds(r.predicted_duration_s)}</td>
                <td>{ratio == null ? "—" : <Badge tone={ratio <= 1.2 ? "ok" : ratio <= 2 ? "warn" : "bad"}>{ratio.toFixed(2)}×</Badge>}</td>
                <td>{r.assurance_passed == null ? <span className="faint">—</span> : <Badge tone={r.assurance_passed ? "ok" : "bad"}>{r.assurance_passed ? "passed" : "failed"}</Badge>}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function PerformancePredictionTab() {
  const [latestPred, setLatestPred] = useState(null);
  const [history, setHistory]       = useState([]);
  const [loading, setLoading]       = useState(false);
  const [err, setErr]               = useState("");

  const fetchData = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const histRes = await perfPrediction.history();
      setHistory(histRes.records || []);
      // Newest manager run that got as far as a performance prediction
      // (runs are newest-first; one that failed validation has none).
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

  return (
    <div>
      <PageHeader
        eyebrow="Agents" icon={Gauge}
        title="Performance agent"
        description="Forecasts total runtime, finds the bottleneck stage, and predicts whether a run will succeed, slow down or fail — before it starts."
        actions={<Button size="sm" icon={RefreshCw} loading={loading} onClick={fetchData}>Refresh</Button>}
      />
      {err && <Alert tone="bad" style={{ marginBottom: 14 }}>{err}</Alert>}

      <div className="list-title" style={{ marginBottom: 10 }}>Latest run</div>
      {latestPred ? <LatestPrediction pred={latestPred} /> : !loading && (
        <Card><Empty icon={Activity} title="No prediction yet">Run a pipeline through the Central Manager — the prediction is made during its pre-checks.</Empty></Card>
      )}

      <Card title="Prediction history" icon={TrendingUp} subtitle={history.length ? `last ${history.length} run(s)` : undefined} pad={!history.length} style={{ marginTop: 14 }}>
        <History history={history} />
      </Card>

      <Card title="How predictions are made" style={{ marginTop: 14 }}>
        <KV items={[
          ["Baseline", "Resource agent's per-stage duration estimates"],
          ["Critical path", "Slowest stage per parallel group, summed"],
          ["History adjustment", "Damped 40% toward mean(actual ÷ predicted), after 5 runs"],
          ["Slowdown", "≥ 1.6× the resource estimate"],
          ["Failure", "≥ 3.0× the estimate, or > 50% recent failures"],
          ["Usual duration", "p95 of the pipeline's last 20 comparable runs; cost savings may add at most 20%"],
        ]} />
      </Card>
    </div>
  );
}
