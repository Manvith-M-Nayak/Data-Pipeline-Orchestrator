import React, { useState, useEffect, useCallback } from "react";
import { BarChart3, Clock, Cpu, GitBranch, RefreshCw, ShieldCheck, TrendingUp, Zap } from "lucide-react";
import { resource, monitor } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import { Alert, Badge, Button, Card, Empty, KV, PageHeader, Stat } from "../ui/components.jsx";

const ratioTone = (r) => (Math.abs(r - 1) < 0.2 ? "ok" : Math.abs(r - 1) < 0.5 ? "warn" : "bad");
const REC_TONE = { ok: "ok", scale_up: "warn", reclaim: "accent", investigate: "bad" };

// ── Accuracy per stage type ───────────────────────────────────────────────────
function AccuracySection({ report }) {
  if (!report || report.total_records === 0) {
    return (
      <Empty icon={BarChart3} title="No prediction history yet">
        Run a pipeline through the Central Manager; each run records predicted vs actual duration.
      </Empty>
    );
  }
  return (
    <div className="grid grid-2">
      {Object.entries(report.by_type || {}).map(([stype, st]) => {
        const acc = st.accuracy_pct || 0;
        return (
          <div key={stype} style={{ border: "1px solid var(--border)", borderRadius: "var(--radius)", padding: 16 }}>
            <div className="row" style={{ gap: 8, marginBottom: 12 }}>
              <span style={{ color: "var(--text)", fontWeight: 600, textTransform: "capitalize" }}>{stype} stages</span>
              <Badge tone={acc > 80 ? "ok" : "warn"} style={{ marginLeft: "auto" }}>{acc}% accurate</Badge>
            </div>
            <div style={{ height: 6, background: "var(--surface-2)", borderRadius: 3, overflow: "hidden", marginBottom: 14 }}>
              <div style={{ height: "100%", width: `${Math.min(acc, 100)}%`, background: acc > 80 ? "var(--ok)" : "var(--warn)" }} />
            </div>
            <KV items={[
              ["Runs recorded", st.count],
              ["Mean actual ÷ predicted", <span key="r" style={{ color: `var(--${ratioTone(st.mean_ratio)})` }}>{st.mean_ratio}×</span>],
              ["Correction applied", `${st.correction_factor}×`],
            ]} />
            {(st.recent_ratios || []).length > 0 && (
              <>
                <div className="list-title">Recent ratios</div>
                <div className="chips">
                  {st.recent_ratios.map((r, i) => <Badge key={i} tone={ratioTone(r)}>{r}×</Badge>)}
                </div>
              </>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── Allocations of the current run ────────────────────────────────────────────
function Allocations({ rp }) {
  const allocs = rp.allocations || [];
  return (
    <>
      {(rp.constraint_violations || []).map((v, i) => <Alert key={`v${i}`} tone="bad" style={{ marginBottom: 8 }}>{v}</Alert>)}
      {(rp.warnings || []).map((w, i) => <Alert key={`w${i}`} tone="warn" style={{ marginBottom: 8 }}>{w}</Alert>)}
      <div style={{ overflowX: "auto" }}>
        <table className="table">
          <thead><tr><th>Stage</th><th>Type</th><th>Compute</th><th>Memory</th><th>Node</th><th>Est. time</th><th /></tr></thead>
          <tbody>
            {allocs.map((a) => (
              <tr key={a.stage_name}>
                <td className="mono" style={{ color: "var(--text)" }}>{a.stage_name}</td>
                <td><Badge tone={a.stage_type === "notebook" ? "violet" : "accent"}>{a.stage_type}</Badge></td>
                <td>{a.stage_type === "notebook"
                  ? `${a.workers} worker${a.workers !== 1 ? "s" : ""} · ${a.cpu} vCPU${a.shuffle_partitions != null ? ` · ${a.shuffle_partitions} shuffle` : ""}`
                  : `${a.diu} DIU`}</td>
                <td>{a.memory_gb} GB</td>
                <td className="muted">{a.node_type || "—"}</td>
                <td><Clock size={11} style={{ verticalAlign: -1 }} /> ~{a.duration_s}s</td>
                <td>
                  <div className="row" style={{ gap: 4, justifyContent: "flex-end" }}>
                    {a.ml_sized && <Badge tone="violet">ML-sized</Badge>}
                    {a.right_sized && <Badge tone="ok">right-sized</Badge>}
                    {a.contention_adjusted && <Badge tone="warn">contention-adjusted</Badge>}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {(rp.execution_groups || []).length > 0 && (
        <>
          <div className="list-title"><GitBranch size={11} style={{ verticalAlign: -1 }} /> Execution groups after contention resolution</div>
          <div className="stack" style={{ gap: 6 }}>
            {rp.execution_groups.map((g, i) => (
              <div key={i} className="row" style={{ gap: 6, flexWrap: "wrap" }}>
                <span className="faint" style={{ fontSize: 12, width: 60 }}>Group {i + 1}</span>
                {g.map((n) => <Badge key={n} tone={g.length > 1 ? "accent" : "neutral"}>{n}</Badge>)}
                {g.length > 1 && <span className="faint" style={{ fontSize: 12 }}>parallel</span>}
              </div>
            ))}
          </div>
        </>
      )}
    </>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function ResourceTab() {
  const [accuracy, setAccuracy]   = useState(null);
  const [factors, setFactors]     = useState(null);
  const [modelInfo, setModelInfo] = useState(null);
  const [recs, setRecs]           = useState(null);
  const [limits, setLimits]       = useState(null);
  // Resource plan of the current run (shared with the Manager and Executor
  // pages) — the allocations live re-allocation compares against.
  const { run } = useAppContext();
  const liveRp = run?.resource_plan?.allocations?.length ? run.resource_plan : null;
  const [loading, setLoading]     = useState(false);
  const [rlLoading, setRlLoading] = useState(false);
  const [err, setErr]             = useState("");

  const fetchAccuracy = useCallback(async () => {
    setLoading(true);
    try {
      const [acc, cf, mi, lim] = await Promise.all([
        resource.accuracy(),
        resource.correctionFactors(),
        resource.modelInfo().catch(() => null),
        resource.limits().catch(() => null),
      ]);
      setAccuracy(acc); setFactors(cf); setModelInfo(mi); setLimits(lim);
      setErr("");
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchAccuracy(); }, [fetchAccuracy]);

  async function checkReallocate() {
    setRlLoading(true);
    setErr("");
    try {
      const live = await monitor.getLiveRuns();
      if (!live || live.length === 0) {
        setRecs([{ stage: "—", action: "ok", reason: "No ADF runs are live right now — nothing to re-allocate." }]);
        return;
      }
      const allocs = liveRp?.allocations || [];
      if (allocs.length === 0) {
        setRecs([{ stage: "—", action: "ok", reason: "No resource plan for the current run — run a pipeline through the Central Manager first." }]);
        return;
      }
      const result = await resource.reallocate(live, allocs, 0);
      setRecs(result.recommendations || []);
    } catch (e) {
      setErr(e.message);
    } finally {
      setRlLoading(false);
    }
  }

  const totalRecords = accuracy?.total_records || 0;
  const types = Object.values(accuracy?.by_type || {});
  const avgAccuracy = types.length ? types.reduce((s, t) => s + (t.accuracy_pct || 0), 0) / types.length : null;
  const spec = limits?.node_specs?.[limits?.default_node];

  return (
    <div>
      <PageHeader
        eyebrow="Agents" icon={Cpu}
        title="Resource agent"
        description="Sizes each stage (workers, DIU, memory, shuffle, node) with a trained model, resolves contention between parallel stages, enforces the subscription limits, and corrects itself from past runs."
        actions={<>
          <Button size="sm" icon={RefreshCw} loading={loading} onClick={fetchAccuracy}>Refresh</Button>
          <Button size="sm" variant="primary" icon={Zap} loading={rlLoading} onClick={checkReallocate}>Check live re-allocation</Button>
        </>}
      />

      {err && <Alert tone="bad" style={{ marginBottom: 14 }}>{err}</Alert>}

      <div className="grid grid-4" style={{ marginBottom: 14 }}>
        <Stat icon={Cpu} label="Sizing engine"
          value={modelInfo ? (modelInfo.ml_available ? "ML model" : "Heuristic") : "—"}
          tone={modelInfo?.ml_available ? "violet" : undefined}
          sub={modelInfo?.metrics?.rows ? `trained on ${Number(modelInfo.metrics.rows).toLocaleString()} stages` : modelInfo && !modelInfo.ml_available ? "model file missing" : undefined} />
        <Stat icon={BarChart3} label="Runs recorded" value={totalRecords} sub="used for self-correction" />
        <Stat icon={TrendingUp} label="Duration accuracy" value={avgAccuracy != null ? `${avgAccuracy.toFixed(1)}%` : "—"}
          tone={avgAccuracy == null ? undefined : avgAccuracy > 80 ? "ok" : "warn"} sub="actual vs predicted" />
        <Stat icon={Zap} label="Correction factors" value={factors ? `${factors.copy}× / ${factors.notebook}×` : "—"}
          sub="copy / notebook (damped)" />
      </div>

      {recs && (
        <Card title="Re-allocation recommendations" icon={Zap} style={{ marginBottom: 14 }}
          actions={<button className="link" onClick={() => setRecs(null)}>Dismiss</button>}>
          {recs.length === 0 ? (
            <div className="muted">Every live stage is sized correctly.</div>
          ) : (
            <div className="list">
              {recs.map((r, i) => (
                <div key={i} className="list-row">
                  <Badge tone={REC_TONE[r.action] || "neutral"}>{(r.action || "").replace("_", " ")}</Badge>
                  <span className="mono" style={{ color: "var(--text)", flexShrink: 0 }}>{r.stage}</span>
                  <span className="muted" style={{ flex: 1, minWidth: 0 }}>{r.reason}</span>
                </div>
              ))}
            </div>
          )}
        </Card>
      )}

      <Card title="Current run's allocations" icon={Cpu} style={{ marginBottom: 14 }}
        subtitle={liveRp ? <>Run <span className="mono">{run?.run_id?.slice(0, 8)}</span> · {liveRp.feasible === false ? "infeasible" : "within limits"}</> : undefined}
        actions={liveRp ? <Badge tone={liveRp.feasible === false ? "bad" : "ok"} dot>{liveRp.feasible === false ? "Infeasible" : "Feasible"}</Badge> : null}>
        {liveRp ? <Allocations rp={liveRp} /> : (
          <Empty icon={Cpu} title="No run selected">Start or open a run in the Central Manager to see how its stages were sized.</Empty>
        )}
      </Card>

      <Card title="Prediction accuracy" icon={TrendingUp} style={{ marginBottom: 14 }}>
        <AccuracySection report={accuracy} />
      </Card>

      <Card title="Subscription limits" icon={ShieldCheck} subtitle="Hard limits the agent enforces (from the backend)">
        {limits ? (
          <KV items={[
            ["Max Databricks workers", limits.max_workers],
            ["Max ADF DIU", limits.max_diu],
            ["Max parallel stages per group", limits.max_concurrent],
            ["Max memory per parallel group", `${limits.max_total_mem_gb} GB`],
            ["Default node", spec ? `${limits.default_node} · ${spec.cpu} vCPU / ${spec.memory_gb} GB` : limits.default_node],
            ["ADF throughput per DIU", `~${limits.adf_mb_per_diu_per_s} MB/s`],
          ]} />
        ) : <div className="muted">Limits unavailable — backend not reachable.</div>}
      </Card>
    </div>
  );
}
