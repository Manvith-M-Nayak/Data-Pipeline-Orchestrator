import React, { useEffect, useState, useCallback, useRef } from "react";
import {
  Activity, ArrowLeft, BarChart3, Brain, CheckCircle2, ChevronRight, CircleDollarSign, Clock, Cpu,
  Download, GitBranch, RefreshCw, Shield, ShieldCheck, TrendingUp, XCircle, Zap,
} from "lucide-react";
import { useAppContext, isLive } from "../AppContext.jsx";
import { manager, executor } from "../api.js";
import { formatWhen } from "../formatTime.js";
import AgentFlow from "../flows/AgentFlow.jsx";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import { Alert, Badge, Button, Card, Empty, KV, PageHeader, Spinner, Stat } from "../ui/components.jsx";

const SEV_TONE = { ok: "ok", error: "bad", warn: "warn", info: "neutral" };
const RUN_TONE = (s) => (s === "completed" ? "ok" : s === "failed" ? "bad" : "warn");

// How this run compares with the pipeline's own history (learned per pipeline,
// no fixed time limit). Older runs saved before this existed show "—".
function usualDuration(p) {
  if (!p || p.expected_duration_basis === undefined) return { label: "—", ok: true };
  if (p.expected_duration_basis !== "history") {
    return { label: `still learning (${p.expected_duration_runs || 0}/3 runs)`, ok: true };
  }
  return p.slower_than_usual
    ? { label: `slower (usually ≤${Math.round(p.expected_duration_s)}s)`, ok: false }
    : { label: `within usual (≤${Math.round(p.expected_duration_s)}s)`, ok: true };
}

function metricValue(k, v) {
  if (typeof v === "number" && k.includes("usd")) return `$${v.toFixed(5)}`;
  if (typeof v === "number" && (k.includes("rate") || k.includes("pct"))) return `${v}%`;
  return String(v);
}

function AgentHealthCard({ title, icon, metrics }) {
  const entries = Object.entries(metrics).filter(([, v]) => v !== null && v !== undefined);
  if (!entries.length) return null;
  return (
    <Card title={title} icon={icon}>
      <KV items={entries.map(([k, v]) => [k.replace(/_/g, " "), metricValue(k, v)])} />
    </Card>
  );
}

function DecisionLog({ decisions }) {
  const [expanded, setExpanded] = useState(false);
  if (!decisions.length) return <div className="muted" style={{ fontSize: 13 }}>No decisions recorded.</div>;
  const shown = expanded ? decisions : decisions.slice(0, 15);
  return (
    <>
      <div className="list">
        {shown.map((d, i) => (
          <div key={i} className="list-row" style={{ alignItems: "flex-start", padding: "8px 0" }}>
            <span className="mono faint" style={{ fontSize: 11.5, flexShrink: 0, paddingTop: 2, whiteSpace: "nowrap" }}>{formatWhen(d.ts)}</span>
            <Badge tone={SEV_TONE[d.severity] || "neutral"} style={{ flexShrink: 0 }}>{d.severity}</Badge>
            <div style={{ minWidth: 0 }}>
              <div style={{ color: "var(--text)", fontWeight: 500, fontSize: 12.5 }}>{d.action}</div>
              <div className="muted" style={{ fontSize: 12 }}>{d.reason}{d.outcome ? <span style={{ color: "var(--text-2)" }}> → {d.outcome}</span> : null}</div>
            </div>
          </div>
        ))}
      </div>
      {decisions.length > 15 && (
        <button className="link" style={{ fontSize: 12.5, marginTop: 8 }} onClick={() => setExpanded(!expanded)}>
          {expanded ? "Show less" : `Show all ${decisions.length} entries`}
        </button>
      )}
    </>
  );
}

// ── One run ───────────────────────────────────────────────────────────────────
function RunDetail({ runId, onBack }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [downloadError, setDownloadError] = useState("");

  // Loaded once; re-fetched every few seconds while the run is still in
  // progress, so this view never lags behind the Central Manager page.
  const live = !!data && isLive(data.status);
  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");
    manager.combinedRun(runId)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(e.message))
      .finally(() => alive && setLoading(false));
    return () => { alive = false; };
  }, [runId]);
  useEffect(() => {
    if (!live) return undefined;
    const t = setInterval(() => {
      manager.combinedRun(runId).then(setData).catch(() => {});
    }, 3000);
    return () => clearInterval(t);
  }, [live, runId]);

  const back = <Button size="sm" variant="ghost" icon={ArrowLeft} onClick={onBack}>All runs</Button>;
  if (loading) return <div>{back}<div className="row muted" style={{ gap: 8, marginTop: 16 }}><Spinner /> Loading run…</div></div>;
  if (error) return <div>{back}<Alert tone="bad" style={{ marginTop: 16 }}>{error}</Alert></div>;
  if (!data) return null;

  const fb = data.feedback || {};
  const ma = data.monitor_analysis || {};
  const perf = data.performance_prediction;
  const usual = usualDuration(perf);
  const sink = data.executor_result?.sink_container;

  return (
    <div>
      <div style={{ marginBottom: 14 }}>{back}</div>
      <PageHeader
        eyebrow="Run insights" icon={BarChart3}
        title={<>Run <span className="mono">{runId.slice(0, 8)}</span> <Badge tone={RUN_TONE(data.status)} dot style={{ verticalAlign: 4, marginLeft: 6 }}>{data.status}</Badge></>}
        description={`${formatWhen(data.started_at)}${data.completed_at ? ` → ${formatWhen(data.completed_at)}` : ""}${data.started_at && data.completed_at ? ` · ${Math.round((new Date(data.completed_at) - new Date(data.started_at)) / 1000)}s` : ""}`}
        actions={sink ? (
          <Button variant="primary" icon={Download} onClick={() => {
            setDownloadError("");
            // via fetch so the x-api-key header is sent when auth is on
            executor.download(sink).catch((err) => setDownloadError(err.message));
          }}>Download output</Button>
        ) : null}
      />

      {downloadError && <Alert tone="bad" style={{ marginBottom: 14 }}>{downloadError}</Alert>}
      {data.error && <Alert tone="bad" title="The run failed" style={{ marginBottom: 14 }}><span style={{ whiteSpace: "pre-wrap" }}>{data.error}</span></Alert>}
      {data.user_request && <Alert tone="neutral" title="Request" style={{ marginBottom: 14 }}>“{data.user_request}”</Alert>}

      <div className="stack" style={{ gap: 14 }}>
        <Card title="Orchestration" icon={Activity} subtitle="How the run moved through the agents, and where it stopped">
          <AgentFlow runState={data} hasPlan />
        </Card>
        {data.plan?.stages?.length > 0 && (
          <Card title="Pipeline" icon={GitBranch}>
            <PipelineFlow plan={data.plan} runState={data} />
          </Card>
        )}

        <div className="grid grid-2" style={{ alignItems: "start" }}>
          <div className="stack" style={{ gap: 14 }}>
            {data.validation && (
              <Card title="Plan validation" icon={Shield}
                actions={<Badge tone={data.validation.ok ? "ok" : "bad"} dot>{data.validation.ok ? "Passed" : "Failed"}</Badge>}>
                {(data.validation.issues || []).map((x, i) => <div key={`i${i}`} style={{ color: "var(--bad)", fontSize: 12.5 }}>✗ {x}</div>)}
                {(data.validation.warnings || []).map((w, i) => <div key={`w${i}`} style={{ color: "var(--warn)", fontSize: 12.5 }}>! {w}</div>)}
                {!(data.validation.issues || []).length && !(data.validation.warnings || []).length && <div className="muted" style={{ fontSize: 13 }}>No issues or warnings.</div>}
              </Card>
            )}

            {data.plan_assurance?.summary && (
              <Card title="Plan checks" icon={ShieldCheck} subtitle={data.plan_assurance.summary}
                actions={<Badge tone={data.plan_assurance.overall_status === "pass" ? "ok" : "bad"} dot>{data.plan_assurance.overall_status === "pass" ? "Passed" : "Rejected"}</Badge>}>
                <div className="list">
                  {(data.plan_assurance.structural_results || []).map((c, i) => (
                    <div key={i} className="list-row">
                      {c.passed ? <CheckCircle2 size={14} style={{ color: "var(--ok)" }} /> : <XCircle size={14} style={{ color: "var(--bad)" }} />}
                      <span className="grow" style={{ color: "var(--text-2)" }}>{c.label}</span>
                    </div>
                  ))}
                </div>
                {data.plan_assurance.semantic_result && (
                  <div className="muted" style={{ fontSize: 12.5, marginTop: 8 }}>
                    Intent: {data.plan_assurance.semantic_result.flagged ? "flagged" : "matches"} — {data.plan_assurance.semantic_result.reasoning}
                  </div>
                )}
              </Card>
            )}

            {data.predictions?.stage_count && (
              <Card title="Resource prediction" icon={Cpu}
                actions={<Badge tone={data.resource_plan?.feasible === false ? "bad" : "ok"}>{data.resource_plan?.feasible === false ? "infeasible" : "feasible"}</Badge>}>
                <KV items={[
                  ["File size", `${data.predictions.file_size_mb} MB`],
                  ["Stages", data.predictions.stage_count],
                  ["Complexity", data.predictions.complexity],
                  ["Peak workers", data.predictions.suggested_workers],
                  ["Estimated duration", `~${data.predictions.estimated_duration_s}s`],
                ]} />
                {(data.resource_plan?.allocations || []).length > 0 && (
                  <div className="list" style={{ marginTop: 10 }}>
                    {data.resource_plan.allocations.map((a) => (
                      <div key={a.stage_name} className="list-row" style={{ fontSize: 12.5 }}>
                        <span className="grow mono">{a.stage_name}</span>
                        <span className="meta">{a.stage_type === "notebook" ? `${a.workers}w · ${a.memory_gb}GB` : `${a.diu} DIU`} · ~{a.duration_s}s</span>
                      </div>
                    ))}
                  </div>
                )}
              </Card>
            )}

            {perf?.outcome && (
              <Card title="Performance prediction" icon={TrendingUp}
                actions={<Badge tone={perf.outcome === "success" ? "ok" : perf.outcome === "failure" ? "bad" : "warn"}>{perf.outcome}</Badge>}>
                <KV items={[
                  ["Predicted total", `~${perf.predicted_total_s}s`],
                  ["Bottleneck", perf.bottleneck_stage ? <span key="b" className="mono">{perf.bottleneck_stage}</span> : "—"],
                  ["Confidence", `${Math.round((perf.confidence || 0) * 100)}%`],
                  ["vs usual duration", <Badge key="u" tone={usual.ok ? "neutral" : "warn"}>{usual.label}</Badge>],
                  perf.prediction_source ? ["Source", perf.prediction_source] : null,
                  perf.learning_correction_applied ? ["Learning correction", `×${perf.learning_correction_applied}`] : null,
                ]} />
              </Card>
            )}

            {(data.cost_estimate?.total_usd !== undefined || data.cost_optimization?.estimated_cost) && (
              <Card title="Cost" icon={CircleDollarSign}>
                <KV items={[
                  data.cost_estimate?.total_usd !== undefined ? ["Estimate (pre-checks)", `$${data.cost_estimate.total_usd}`] : null,
                  data.cost_optimization?.estimated_cost ? ["Cost agent estimate", `$${data.cost_optimization.estimated_cost.total_usd}`] : null,
                  data.cost_optimization?.cost_correction_applied ? ["Learned correction", `×${data.cost_optimization.cost_correction_applied}`] : null,
                ]} />
                {(data.cost_optimization?.recommendations || []).map((r, i) => (
                  <div key={i} className="muted" style={{ fontSize: 12.5, marginTop: 6 }}>
                    <CircleDollarSign size={11} style={{ verticalAlign: -1 }} /> <b style={{ color: "var(--text-2)" }}>{r.change}</b> — {r.estimated_saving}
                  </div>
                ))}
              </Card>
            )}
          </div>

          <div className="stack" style={{ gap: 14 }}>
            {data.assurance && Object.keys(data.assurance).length > 0 && (
              <Card title="Output checks" icon={ShieldCheck}
                actions={<Badge tone={data.assurance.passed ? "ok" : "warn"} dot>{data.assurance.passed ? "Passed" : "Warnings"}</Badge>}>
                <KV items={[
                  data.assurance.actual_duration_s !== undefined ? ["Actual duration", `${data.assurance.actual_duration_s}s`] : null,
                  data.assurance.predicted_duration_s !== undefined ? ["Predicted duration", `${data.assurance.predicted_duration_s}s`] : null,
                  data.assurance.timing_ratio !== undefined ? ["Timing ratio", `${data.assurance.timing_ratio}×`] : null,
                  data.assurance.retries_used !== undefined ? ["Retries", data.assurance.retries_used] : null,
                ]} />
              </Card>
            )}

            {fb.run_id && (
              <Card title="Feedback record" icon={Brain} subtitle="What the learning loop recorded">
                <KV items={[
                  fb.actual_duration_s !== undefined ? ["Actual duration", `${fb.actual_duration_s}s`] : null,
                  fb.predicted_duration_s !== undefined ? ["Predicted duration", `${fb.predicted_duration_s}s`] : null,
                  fb.perf_predicted_total_s !== undefined ? ["Performance prediction", `${fb.perf_predicted_total_s}s`] : null,
                  fb.estimated_cost_usd != null ? ["Estimated cost", `$${fb.estimated_cost_usd}`] : null,
                  fb.actual_cost_usd != null ? ["Actual cost", `$${fb.actual_cost_usd}`] : null,
                  fb.prediction_source ? ["Prediction source", fb.prediction_source] : null,
                  fb.complexity ? ["Complexity", fb.complexity] : null,
                ]} />
              </Card>
            )}

            {ma.status_summary && (
              <Card title="Monitor analysis" icon={Activity}
                actions={ma.severity ? <Badge tone={ma.severity === "high" ? "bad" : ma.severity === "medium" ? "warn" : "ok"}>{ma.severity}</Badge> : null}>
                <div style={{ fontSize: 13, color: "var(--text-2)" }}>{ma.status_summary}</div>
                {ma.explanation && <div className="muted" style={{ fontSize: 12.5, marginTop: 6 }}>{ma.explanation}</div>}
                {ma.root_cause && <div style={{ fontSize: 12.5, marginTop: 6, color: "var(--warn)" }}>Root cause: {ma.root_cause}</div>}
              </Card>
            )}

            {data.parallelism?.execution_groups && (
              <Card title="Parallelism" icon={GitBranch}
                subtitle={data.parallelism.can_parallelize ? `${data.parallelism.parallel_groups} parallel group(s)` : "All sequential"}>
                <div className="stack" style={{ gap: 6 }}>
                  {data.parallelism.execution_groups.map((g, i) => (
                    <div key={i} className="row" style={{ gap: 6, flexWrap: "wrap" }}>
                      <span className="faint" style={{ fontSize: 12, width: 28 }}>G{i + 1}</span>
                      {g.map((n) => <Badge key={n} tone={g.length > 1 ? "violet" : "neutral"}>{n}</Badge>)}
                    </div>
                  ))}
                </div>
              </Card>
            )}

            {data.executor_result && (
              <Card title="Executor result" icon={Zap}>
                <KV items={[
                  ["Status", <Badge key="s" tone={data.executor_result.status === "ok" ? "ok" : "bad"}>{data.executor_result.status}</Badge>],
                  data.executor_result.stages?.length ? ["Stages", data.executor_result.stages.join(" → ")] : null,
                  sink ? ["Output container", <span key="o" className="mono">{sink}</span>] : null,
                ]} />
              </Card>
            )}
          </div>
        </div>

        <Card title="Decision log" icon={Shield} subtitle={`${data.decisions?.length || 0} entries`}>
          <DecisionLog decisions={data.decisions || []} />
        </Card>
      </div>
    </div>
  );
}

// ── All runs ──────────────────────────────────────────────────────────────────
export default function RunInsights() {
  const [analytics, setAnalytics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedRun, setSelectedRun] = useState(null);
  const [error, setError] = useState("");

  const load = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true);
    setError("");
    try {
      setAnalytics(await manager.analytics());
    } catch (e) {
      setError(`Could not load analytics: ${e.message}`);
    } finally { if (!quiet) setLoading(false); }
  }, []);

  useEffect(() => { load(); }, [load]);

  // Keep the table in step with the shared run list: refresh while any run is
  // in progress, and once more when the last one finishes.
  const { runs: sharedRuns } = useAppContext();
  const liveKey = sharedRuns.filter((r) => isLive(r.status)).map((r) => `${r.run_id}:${r.status}`).join(",");
  const firstKey = useRef(true);
  useEffect(() => {
    if (firstKey.current) { firstKey.current = false; return undefined; }
    load(true);
    if (!liveKey) return undefined;
    const t = setInterval(() => load(true), 5000);
    return () => clearInterval(t);
  }, [liveKey, load]);

  if (selectedRun) return <RunDetail runId={selectedRun} onBack={() => setSelectedRun(null)} />;

  const s = analytics?.summary || {};
  const da = analytics?.duration_accuracy || {};
  const ca = analytics?.cost_accuracy || {};
  const ah = analytics?.agent_health || {};
  const runs = analytics?.runs || [];

  return (
    <div>
      <PageHeader
        eyebrow="Observe" icon={BarChart3}
        title="Run insights"
        description="Every managed run with what each agent predicted, decided and measured — and how accurate the predictions were."
        actions={<Button size="sm" icon={RefreshCw} loading={loading} onClick={() => load()}>Refresh</Button>}
      />

      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}

      <div className="grid grid-4" style={{ marginBottom: 14 }}>
        <Stat icon={Activity} label="Runs" value={s.total_runs ?? 0} sub={`${s.completed ?? 0} completed · ${s.failed ?? 0} failed`} />
        <Stat icon={CheckCircle2} label="Success rate" value={`${s.success_rate_pct ?? 0}%`} tone="ok" sub={`${s.in_progress ?? 0} in progress`} />
        <Stat icon={TrendingUp} label="Actual ÷ predicted" value={da.avg_predicted_vs_actual_ratio ? `${da.avg_predicted_vs_actual_ratio}×` : "—"} sub={`${da.samples || 0} samples`} />
        <Stat icon={CircleDollarSign} label="Cost estimate error" value={ca.avg_error_pct != null ? `${ca.avg_error_pct}%` : "—"}
          sub={`$${ca.total_estimated_usd?.toFixed(4) || 0} est. · $${ca.total_actual_usd?.toFixed(4) || 0} actual`} />
      </div>

      <div className="list-title" style={{ marginBottom: 10 }}>Agent health</div>
      <div className="grid grid-3" style={{ marginBottom: 14, alignItems: "start" }}>
        <AgentHealthCard title="Planner" icon={Brain} metrics={ah.planner || {}} />
        <AgentHealthCard title="Assurance" icon={ShieldCheck} metrics={ah.assurance || {}} />
        <AgentHealthCard title="Resource" icon={Cpu} metrics={ah.resource || {}} />
        <AgentHealthCard title="Performance" icon={TrendingUp} metrics={ah.performance_prediction || {}} />
        <AgentHealthCard title="Cost" icon={CircleDollarSign} metrics={ah.cost_optimization || {}} />
        <AgentHealthCard title="Executor" icon={Zap} metrics={ah.executor || {}} />
      </div>

      <Card title="All runs" icon={Clock} subtitle={`${runs.length} run(s) — select one for the full breakdown`} pad={false}>
        {runs.length === 0 ? (
          <Empty icon={Clock} title="No runs recorded yet">Runs started through the Central Manager appear here.</Empty>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table className="table">
              <thead><tr>
                <th /><th>Run</th><th>Status</th><th>Stages</th><th>Duration</th><th>Cost est.</th><th>Cost actual</th><th>Checks</th><th>Source</th><th>Started</th>
              </tr></thead>
              <tbody>
                {runs.map((r) => (
                  <tr key={r.run_id} className="clickable" onClick={() => setSelectedRun(r.run_id)}>
                    <td style={{ width: 24, color: "var(--text-4)" }}><ChevronRight size={14} /></td>
                    <td className="mono" style={{ color: "var(--text)" }}>{r.run_id?.slice(0, 8)}</td>
                    <td><Badge tone={RUN_TONE(r.status)}>{r.status?.replace("_", " ")}</Badge></td>
                    <td>{r.stage_count}</td>
                    <td>
                      {r.actual_duration_s ? `${r.actual_duration_s}s` : "—"}
                      {r.predicted_duration_s != null && <span className="faint" style={{ fontSize: 12 }}> / {r.predicted_duration_s}s</span>}
                    </td>
                    <td>{r.cost_estimate_usd != null ? `$${r.cost_estimate_usd}` : "—"}</td>
                    <td>{r.actual_cost_usd != null ? `$${r.actual_cost_usd}` : "—"}</td>
                    <td>{r.assurance_passed != null ? <Badge tone={r.assurance_passed ? "ok" : "bad"}>{r.assurance_passed ? "passed" : "failed"}</Badge> : <span className="faint">—</span>}</td>
                    <td className="muted">{r.prediction_source || "—"}</td>
                    <td className="muted mono" style={{ fontSize: 12, whiteSpace: "nowrap" }}>{formatWhen(r.started_at) || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
