import React, { useState, useRef, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Activity, Brain, CheckCircle2, CircleDollarSign, ClipboardCheck, Clock, Cpu, Download,
  FileText, GitBranch, Play, RotateCcw, Shield, ShieldCheck, TrendingUp, XCircle,
} from "lucide-react";
import { executor } from "../api.js";
import { useAppContext, isLive } from "../AppContext.jsx";
import { formatWhen } from "../formatTime.js";
import StreamingConsole from "./StreamingConsole.jsx";
import AgentFlow from "../flows/AgentFlow.jsx";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import { Alert, Badge, Button, Card, Dot, KV, PageHeader, Spinner } from "../ui/components.jsx";

const SEV_TONE = { ok: "ok", error: "bad", warn: "warn", info: "neutral" };
const RUN_TONE = (s) => (s === "completed" ? "ok" : s === "failed" ? "bad" : "warn");
const yesNo = (v) => <Badge tone={v ? "ok" : "bad"}>{v ? "yes" : "no"}</Badge>;

// ── Decision audit log ────────────────────────────────────────────────────────
function DecisionLog({ decisions }) {
  const endRef = useRef();
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" }); }, [decisions.length]);
  if (!decisions.length) return <div className="muted" style={{ fontSize: 13 }}>Waiting for the first decision…</div>;
  return (
    <div style={{ maxHeight: 320, overflowY: "auto", margin: "-4px -4px 0", padding: "0 4px" }}>
      <div className="list">
        {decisions.map((d, i) => (
          <div key={i} className="list-row" style={{ alignItems: "flex-start", gap: 10, padding: "8px 0" }}>
            <span className="mono faint" style={{ fontSize: 11.5, flexShrink: 0, paddingTop: 2, whiteSpace: "nowrap" }}>{formatWhen(d.ts)}</span>
            <Badge tone={SEV_TONE[d.severity] || "neutral"} style={{ flexShrink: 0 }}>{d.severity}</Badge>
            <div style={{ minWidth: 0 }}>
              <div style={{ color: "var(--text)", fontWeight: 500, fontSize: 12.5 }}>{d.action}</div>
              <div className="muted" style={{ fontSize: 12 }}>
                {d.reason}{d.outcome ? <span style={{ color: "var(--text-2)" }}> → {d.outcome}</span> : null}
              </div>
            </div>
          </div>
        ))}
      </div>
      <div ref={endRef} />
    </div>
  );
}

// ── Pre-check results ─────────────────────────────────────────────────────────
function ResourceCard({ predictions, cost, resourcePlan }) {
  if (!predictions?.stage_count) return null;
  const allocs = resourcePlan?.allocations || [];
  const feasible = resourcePlan?.feasible ?? true;
  const factors = predictions.correction_factors || {};
  return (
    <Card title="Resources & cost" icon={Cpu}
      actions={resourcePlan ? <Badge tone={feasible ? "ok" : "bad"} dot>{feasible ? "Feasible" : "Infeasible"}</Badge> : null}>
      {(resourcePlan?.constraint_violations || []).map((v, i) => <Alert key={`v${i}`} tone="bad" style={{ marginBottom: 8 }}>{v}</Alert>)}
      {(resourcePlan?.warnings || []).map((w, i) => <Alert key={`w${i}`} tone="warn" style={{ marginBottom: 8 }}>{w}</Alert>)}
      <KV items={[
        ["File size", `${predictions.file_size_mb} MB`],
        ["Stages", `${predictions.stage_count} (${predictions.copy_stages} copy + ${predictions.notebook_stages} notebook)`],
        ["Complexity", predictions.complexity],
        ["Peak workers", predictions.suggested_workers],
        ["Total memory", `${predictions.total_memory_gb ?? "—"} GB`],
        ["Estimated duration", `~${predictions.estimated_duration_s}s`],
        ["Node type", <span key="n" className="mono">{predictions.node_type}</span>],
        (factors.copy || factors.notebook) ? ["Correction factors", `${factors.copy}× copy · ${factors.notebook}× notebook`] : null,
      ]} />
      {allocs.length > 0 && (
        <>
          <div className="list-title">Stage allocations</div>
          <div className="list">
            {allocs.map((a) => (
              <div key={a.stage_name} className="list-row" style={{ fontSize: 12.5 }}>
                <span className="grow mono">{a.stage_name}</span>
                <span className="meta">{a.stage_type === "notebook" ? `${a.workers}w · ${a.memory_gb}GB · ${a.cpu}vCPU` : `${a.diu} DIU`} · ~{a.duration_s}s</span>
                {a.right_sized && <Badge tone="ok">right-sized</Badge>}
                {a.contention_adjusted && <Badge tone="warn">adjusted</Badge>}
              </div>
            ))}
          </div>
        </>
      )}
      {cost?.total_usd !== undefined && (
        <>
          <div className="list-title"><CircleDollarSign size={11} style={{ verticalAlign: -1 }} /> Cost estimate</div>
          <KV items={[
            ["ADF activities", `$${cost.adf_activity_usd}`],
            ["Databricks", `$${cost.databricks_usd}`],
            ["Blob storage", `$${cost.storage_usd}`],
            ["Total", <span key="t" style={{ color: cost.budget_ok ? "var(--ok)" : "var(--warn)", fontWeight: 600 }}>${cost.total_usd} {cost.budget_ok ? "" : "· over $1"}</span>],
          ]} />
        </>
      )}
    </Card>
  );
}

function ParallelismCard({ parallelism }) {
  if (!parallelism?.execution_groups) return null;
  return (
    <Card title="Parallelism" icon={GitBranch}
      subtitle={parallelism.can_parallelize ? `${parallelism.parallel_groups} group(s) can run in parallel` : "All stages run in sequence"}>
      <div className="stack" style={{ gap: 8 }}>
        {parallelism.execution_groups.map((group, i) => (
          <div key={i} className="row" style={{ gap: 6, flexWrap: "wrap" }}>
            <span className="faint" style={{ fontSize: 12, width: 60 }}>Group {i + 1}</span>
            {group.map((name) => <Badge key={name} tone={group.length > 1 ? "violet" : "neutral"}>{name}</Badge>)}
            {group.length > 1 && <span style={{ fontSize: 12, color: "var(--violet)" }}>parallel</span>}
          </div>
        ))}
      </div>
    </Card>
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
    ? { label: `slower (usually ≤${Math.round(p.expected_duration_s)}s)`, ok: false }
    : { label: `within usual (≤${Math.round(p.expected_duration_s)}s)`, ok: true };
}

function PerformanceCard({ perf }) {
  if (!perf?.outcome) return null;
  const usual = usualDuration(perf);
  return (
    <Card title="Performance prediction" icon={TrendingUp}
      actions={<Badge tone={perf.outcome === "success" ? "ok" : perf.outcome === "failure" ? "bad" : "warn"}>{perf.outcome}</Badge>}>
      <KV items={[
        ["Predicted total", `~${perf.predicted_total_s}s`],
        ["Bottleneck stage", perf.bottleneck_stage ? <span key="b" className="mono">{perf.bottleneck_stage}</span> : "—"],
        ["Confidence", `${Math.round((perf.confidence || 0) * 100)}%`],
        ["vs usual duration", <Badge key="u" tone={usual.ok ? "neutral" : "warn"}>{usual.label}</Badge>],
        perf.history_runs_used !== undefined ? ["History runs used", perf.history_runs_used] : null,
      ]} />
    </Card>
  );
}

function PlanAssuranceCard({ planAssurance }) {
  if (!planAssurance?.summary) return null;
  const structural = planAssurance.structural_results || [];
  const sem = planAssurance.semantic_result;
  const passed = planAssurance.overall_status === "pass";
  return (
    <Card title="Plan checks (before running)" icon={ShieldCheck} subtitle={planAssurance.summary}
      actions={<Badge tone={passed ? "ok" : "bad"} dot>{passed ? "Passed" : "Rejected"}</Badge>}>
      <div className="list">
        {structural.map((c) => (
          <div key={c.check} className="list-row" style={{ alignItems: "flex-start" }}>
            {c.passed
              ? <CheckCircle2 size={14} style={{ color: "var(--ok)", marginTop: 2, flexShrink: 0 }} />
              : <XCircle size={14} style={{ color: "var(--bad)", marginTop: 2, flexShrink: 0 }} />}
            <div style={{ minWidth: 0 }}>
              <div style={{ color: "var(--text)" }}>{c.label}</div>
              {!c.passed && <div style={{ color: "var(--bad)", fontSize: 12.5 }}>{c.message}</div>}
            </div>
          </div>
        ))}
      </div>
      {sem && (
        <Alert tone={!sem.available ? "neutral" : sem.flagged ? "warn" : "ok"} style={{ marginTop: 10 }}
          title={`Intent check${sem.model ? ` · ${sem.model}` : ""} (advisory)${sem.available ? (sem.flagged ? " — possible mismatch" : " — matches the request") : ""}`}>
          {sem.reasoning}
        </Alert>
      )}
    </Card>
  );
}

function AssuranceCard({ assurance }) {
  if (!Object.keys(assurance || {}).length) return null;
  return (
    <Card title="Output checks (after running)" icon={ClipboardCheck}
      actions={<Badge tone={assurance.passed ? "ok" : "warn"} dot>{assurance.passed ? "Passed" : "Warnings"}</Badge>}>
      <KV items={[
        assurance.all_stages_completed !== undefined ? ["All stages completed", yesNo(assurance.all_stages_completed)] : null,
        assurance.has_output !== undefined ? ["Output present", yesNo(assurance.has_output)] : null,
        assurance.timing_ok !== undefined ? ["Timing within 4× estimate", yesNo(assurance.timing_ok)] : null,
        assurance.actual_duration_s !== undefined ? ["Actual duration", `${assurance.actual_duration_s}s`] : null,
        assurance.predicted_duration_s !== undefined ? ["Predicted duration", `${assurance.predicted_duration_s}s`] : null,
        assurance.timing_ratio !== undefined ? ["Timing ratio", `${assurance.timing_ratio}×`] : null,
        assurance.retries_used !== undefined ? ["Retries used", assurance.retries_used] : null,
      ]} />
    </Card>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function ManagerTab() {
  const navigate = useNavigate();
  const {
    csvFile, csvRestoring,
    planResult: savedPlan,
    plannerPrompt, setPlannerPrompt,
    detectedSchema, runSchema,
    runId, run: mgrState, runError, runs: allRuns,
    startRun, followRun, clearRun,
  } = useAppContext();

  // Editable user request — prefilled from the Planner prompt, can be edited
  // here and is sent to the Manager (drives the semantic assurance layer).
  const [request, setRequest] = useState(plannerPrompt || "");
  useEffect(() => { setRequest(plannerPrompt || ""); }, [plannerPrompt]);

  const [starting, setStarting] = useState(false);
  const [localError, setLocalError] = useState("");
  const error = localError || runError;

  // The run itself (polling, following runs started elsewhere, reload) lives
  // in AppContext — one copy shared with the Executor and every other page.
  const running = starting || isLive(mgrState?.status) || (!!runId && !mgrState);

  async function handleRun() {
    if (!csvFile || !savedPlan) return;
    setLocalError(""); setStarting(true);
    try {
      setPlannerPrompt(request);   // persist any edits so other tabs stay in sync
      await startRun({ file: csvFile, config: savedPlan.config, schema: runSchema, request, origin: "manager" });
    } catch (e) {
      setLocalError("Failed to start: " + e.message);
    } finally {
      setStarting(false);
    }
  }

  function attachToRun(rid) {
    setLocalError("");
    followRun(rid);
  }

  function reset() {
    clearRun();
    setLocalError("");
  }

  const status = mgrState?.status;
  const isTerminal = status === "completed" || status === "failed";
  const canRun = !!csvFile && !!savedPlan && !running;
  const cfg = savedPlan?.config;
  const streaming = cfg?.mode === "streaming";
  const transformStages = (cfg?.stages || []).filter(
    (s) => (s.transformations && s.transformations.length) || s.filter_condition || s.aggregation);

  return (
    <div>
      <PageHeader
        eyebrow="Run" icon={GitBranch}
        title="Central Manager"
        description="Validates the plan, sizes resources, forecasts runtime and cost, hands off to the Executor, retries failures, checks the output and records the outcome."
        actions={<>
          {(mgrState || error) && <Button size="sm" icon={RotateCcw} onClick={reset} disabled={running && !isTerminal}>Clear</Button>}
          <Button variant="primary" icon={Play} loading={starting} disabled={!canRun} onClick={handleRun}>
            {streaming ? "Run once (seed data)" : "Run pipeline"}
          </Button>
        </>}
      />

      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}

      {/* ── What will run ── */}
      <div className="grid grid-2" style={{ marginBottom: 14, alignItems: "start" }}>
        <Card title="Plan" icon={Brain}
          actions={cfg ? <button className="link" onClick={() => navigate("/planner")}>Edit →</button> : null}>
          {cfg ? (
            <div className="stack" style={{ gap: 6, fontSize: 13 }}>
              <div style={{ color: "var(--text)" }}>
                {cfg.stages?.length ?? 0} stage(s) · {streaming ? "streaming" : "batch"}
                {savedPlan.used_fallback && <Badge tone="warn" style={{ marginLeft: 8 }}>fallback</Badge>}
                {savedPlan.verification?.verified && <Badge tone="ok" style={{ marginLeft: 8 }}>self-checked</Badge>}
              </div>
              <div className="mono muted" style={{ fontSize: 12, overflowWrap: "anywhere" }}>{cfg.execution_order?.join(" → ")}</div>
            </div>
          ) : (
            <div className="muted" style={{ fontSize: 13 }}>
              No plan yet — <button className="link" onClick={() => navigate("/planner")}>design one in the Planner</button>
              {" "}or <button className="link" onClick={() => navigate("/new")}>use guided mode</button>.
            </div>
          )}
        </Card>

        <Card title="Data file" icon={FileText}>
          {csvRestoring ? (
            <div className="row muted" style={{ gap: 8, fontSize: 13 }}><Spinner size={12} /> Restoring your data file…</div>
          ) : csvFile ? (
            <div className="stack" style={{ gap: 4, fontSize: 13 }}>
              <div className="mono" style={{ color: "var(--text)" }}>{csvFile.name}</div>
              <div className="muted">{(csvFile.size / 1024).toFixed(1)} KB · ready</div>
            </div>
          ) : (
            <div className="muted" style={{ fontSize: 13 }}>
              No data file — choose one in the <button className="link" onClick={() => navigate("/planner")}>Planner</button>
              {" "}or <button className="link" onClick={() => navigate("/executor")}>Executor</button>.
            </div>
          )}
        </Card>
      </div>

      {cfg && (
        <Card title="Request & context" icon={Activity} style={{ marginBottom: 14 }}
          subtitle="The request drives the intent check — edit it before running">
          <textarea className="input" rows={2} value={request} disabled={running}
            onChange={(e) => setRequest(e.target.value)}
            placeholder="e.g. Ingest orders, then total revenue per category and region" />
          {(Object.keys(detectedSchema?.columns || {}).length > 0 || transformStages.length > 0) && (
            <details style={{ marginTop: 10 }}>
              <summary className="link" style={{ fontSize: 13 }}>Schema and transformations</summary>
              {Object.keys(detectedSchema?.columns || {}).length > 0 && (
                <>
                  <div className="list-title">Columns</div>
                  <div className="chips">
                    {Object.entries(detectedSchema.columns).map(([c, t]) => (
                      <span key={c} className="chip"><span className="mono">{c}</span><small>{String(t)}</small></span>
                    ))}
                  </div>
                </>
              )}
              {transformStages.map((s) => (
                <div key={s.name} style={{ marginTop: 10 }}>
                  <div className="mono" style={{ color: "var(--text)", fontSize: 12.5 }}>{s.name}</div>
                  <div className="mono muted" style={{ fontSize: 12, paddingLeft: 10 }}>
                    {(s.transformations || []).map((t, i) => <div key={i}>• {t}</div>)}
                    {s.filter_condition && <div style={{ color: "var(--warn)" }}>filter: {s.filter_condition}</div>}
                    {s.aggregation && (
                      <div style={{ color: "var(--accent)" }}>
                        group by [{(s.aggregation.group_by || []).join(", ")}] → {(s.aggregation.aggregations || []).map((a) => `${a.op}(${a.column})`).join(", ")}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </details>
          )}
        </Card>
      )}

      {streaming && (
        <div style={{ marginBottom: 14 }}>
          <StreamingConsole config={cfg} schema={runSchema} fileFormat={detectedSchema?.file_format || "csv"} />
        </div>
      )}

      {/* ── The run ── */}
      {runId && !mgrState && !error && (
        <Card style={{ marginBottom: 14 }}>
          <div className="row muted" style={{ gap: 10 }}><Spinner size={13} /> Loading run {runId.slice(0, 8)}…</div>
        </Card>
      )}

      {mgrState && (
        <div className="stack" style={{ gap: 14, marginBottom: 14 }}>
          <div className="big-status" style={{
            background: status === "completed" ? "var(--ok-soft)" : status === "failed" ? "var(--bad-soft)" : "var(--surface)",
            borderColor: status === "completed" ? "var(--ok-line)" : status === "failed" ? "var(--bad-line)" : "var(--border)",
          }}>
            <div className="icon" style={{ background: status === "completed" || status === "failed" ? "var(--surface)" : "var(--surface-2)" }}>
              {status === "completed" ? <CheckCircle2 size={20} style={{ color: "var(--ok)" }} />
                : status === "failed" ? <XCircle size={20} style={{ color: "var(--bad)" }} />
                : <Spinner size={18} />}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ color: "var(--text)", fontWeight: 600 }}>
                {status === "completed" ? "Pipeline completed" : status === "failed" ? "Run failed" : mgrState.step}
              </div>
              <div className="muted" style={{ fontSize: 12.5, wordBreak: "break-word", whiteSpace: "pre-wrap" }}>
                Run <span className="mono">{mgrState.run_id?.slice(0, 8)}</span>
                {mgrState.retries > 0 ? ` · ${mgrState.retries} ${mgrState.retries === 1 ? "retry" : "retries"}` : ""}
                {status === "completed" && mgrState.executor_result?.stages ? ` · ${mgrState.executor_result.stages.join(" → ")}` : ""}
                {status === "failed" && (mgrState.error ? ` · ${mgrState.error}` : ` · ${mgrState.step}`)}
              </div>
            </div>
            {status === "completed" && mgrState.executor_result?.sink_container && (
              <Button variant="primary" icon={Download}
                onClick={() => executor.download(mgrState.executor_result.sink_container).catch((err) => setLocalError(err.message))}>
                Download output
              </Button>
            )}
          </div>

          <Card title="Orchestration" icon={Activity} subtitle="Which agent is working on this run">
            <AgentFlow runState={mgrState} hasPlan />
          </Card>

          {(mgrState.plan?.stages?.length || cfg?.stages?.length) ? (
            <Card title="Pipeline" icon={GitBranch} subtitle="Stages light up as the executor reaches them">
              <PipelineFlow
                plan={mgrState.plan?.stages?.length ? mgrState.plan : cfg}
                runState={mgrState}
                inputLabel={csvFile?.name}
              />
            </Card>
          ) : null}

          <Card title="Decision log" icon={Shield} subtitle={`${(mgrState.decisions || []).length} entries`}>
            <DecisionLog decisions={mgrState.decisions || []} />
          </Card>

          {(mgrState.predictions?.stage_count || mgrState.parallelism?.execution_groups) && (
            <div className="grid grid-2" style={{ alignItems: "start" }}>
              <ResourceCard predictions={mgrState.predictions} cost={mgrState.cost_estimate} resourcePlan={mgrState.resource_plan} />
              <div className="stack" style={{ gap: 14 }}>
                <ParallelismCard parallelism={mgrState.parallelism} />
                <PerformanceCard perf={mgrState.performance_prediction} />
              </div>
            </div>
          )}

          <div className="grid grid-2" style={{ alignItems: "start" }}>
            <PlanAssuranceCard planAssurance={mgrState.plan_assurance} />
            <AssuranceCard assurance={mgrState.assurance} />
          </div>

          {mgrState.validation?.warnings?.length > 0 && (
            <Alert tone="warn" title="Plan warnings">
              {mgrState.validation.warnings.map((w, i) => <div key={i}>{w}</div>)}
            </Alert>
          )}
        </div>
      )}

      {!runId && !mgrState && !error && (
        <Card title="How a run works" icon={Activity} style={{ marginBottom: 14 }}
          subtitle="Each run passes through these agents in order">
          <AgentFlow hasPlan={!!savedPlan} />
        </Card>
      )}

      {/* ── Recent runs ── */}
      {allRuns.length > 0 && (
        <Card title="Recent runs" icon={Clock} pad={false}>
          <div style={{ overflowX: "auto" }}>
            <table className="table">
              <thead><tr><th /><th>Run</th><th>Status</th><th>Step</th><th>Stages</th><th>Started</th></tr></thead>
              <tbody>
                {allRuns.slice(0, 8).map((r) => (
                  <tr key={r.run_id} className="clickable" onClick={() => attachToRun(r.run_id)}
                    style={r.run_id === runId ? { background: "var(--surface-2)" } : undefined}>
                    <td style={{ width: 24 }}><Dot tone={RUN_TONE(r.status)} live={isLive(r.status)} /></td>
                    <td className="mono" style={{ color: "var(--text)" }}>{r.run_id.slice(0, 8)}</td>
                    <td><Badge tone={RUN_TONE(r.status)}>{r.status?.replace("_", " ")}</Badge></td>
                    <td className="muted" style={{ maxWidth: 360, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.step}</td>
                    <td>{r.stage_count}</td>
                    <td className="muted mono" style={{ fontSize: 12, whiteSpace: "nowrap" }}>{formatWhen(r.started_at) || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}
