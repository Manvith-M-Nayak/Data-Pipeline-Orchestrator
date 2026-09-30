import React, { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Upload, FileText, Sparkles, ArrowLeft, ArrowRight, Play, RotateCcw, Download,
  CheckCircle2, XCircle, ShieldCheck, AlertTriangle, BarChart3, GitBranch, Wand2, Clock,
} from "lucide-react";
import { schema as schemaApi, planner, executor } from "../api.js";
import { useAppContext, isLive } from "../AppContext.jsx";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import AgentFlow from "../flows/AgentFlow.jsx";
import {
  Alert, Badge, Button, Card, Empty, Field, KV, PageHeader, Segmented, Spinner,
} from "../ui/components.jsx";

// Guided path through the whole product: pick data → say what you want →
// review the designed pipeline → run it → see results. Every step writes to
// the same shared state as the individual pages (AppContext), so the Planner,
// Central Manager and Executor pages always show the same plan and run.

const STEPS = [
  { key: "data",     label: "Data",     hint: "Upload a file" },
  { key: "describe", label: "Describe", hint: "Say what you want" },
  { key: "review",   label: "Review",   hint: "Check the design" },
  { key: "run",      label: "Run",      hint: "Execute on Azure" },
  { key: "results",  label: "Results",  hint: "Output & insights" },
];

const NUMERIC = new Set(["integer", "long", "double", "float", "decimal", "number"]);

// Prompt ideas written with the dataset's own columns, so they are runnable
// and sensible. The sample rows decide each column's role: numeric columns
// holding only 0/1 are flags (good filters), other numbers are measures, and
// columns whose values repeat are group keys (a unique id column is not).
function suggestions(columns, preview = []) {
  const entries = Object.entries(columns || {});
  const vals = (c) => preview.map((r) => r?.[c]).filter((v) => v !== null && v !== undefined && v !== "");
  const isNum = (t) => NUMERIC.has(String(t).toLowerCase());
  const flags = [], measures = [], groups = [];
  for (const [c, t] of entries) {
    const v = vals(c);
    const distinct = new Set(v.map(String)).size;
    if (isNum(t) && v.length && v.every((x) => String(x) === "0" || String(x) === "1")) flags.push(c);
    else if (isNum(t)) measures.push(c);
    if (!isNum(t) && v.length > 1 && distinct < v.length) groups.push(c);
  }
  const by = groups[0] || flags[0];
  const out = [];
  if (flags[0] && flags[1]) out.push(`Keep only rows where ${flags[0]} is 1 and ${flags[1]} is 1.`);
  else if (flags[0]) out.push(`Keep only rows where ${flags[0]} is 1.`);
  if (measures[0] && by) out.push(`Average ${measures[0]} and count rows for each ${by}.`);
  if (measures[0] && by) out.push(`Keep rows where ${measures[0]} is greater than 0, then total ${measures[0]} per ${by}.`);
  if (flags[2] && measures[0]) out.push(`Keep rows where ${flags[2]} is 1, then find the highest ${measures[0]}.`);
  out.push("Remove duplicate rows and drop rows with missing values.");
  return out.slice(0, 4);
}

function fmtDuration(s) {
  if (s == null || !isFinite(s)) return "—";
  s = Math.round(s);
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`;
}

function Stepper({ current, reachable, onGo }) {
  return (
    <nav className="stepper" aria-label="Progress">
      {STEPS.map((s, i) => {
        const done = i < current;
        const can = reachable(i) && i !== current;
        return (
          <React.Fragment key={s.key}>
            <button
              className={`step${done ? " done" : ""}${i === current ? " current" : ""}${can ? " reachable" : ""}`}
              onClick={() => can && onGo(i)}
              aria-current={i === current ? "step" : undefined}
              disabled={!can && i !== current}
              style={{ opacity: 1 }}
            >
              <span className="step-num">{done ? <CheckCircle2 size={14} strokeWidth={2.2} /> : i + 1}</span>
              <span className="step-text">
                <div className="step-label">{s.label}</div>
                <div className="step-hint">{s.hint}</div>
              </span>
            </button>
            {i < STEPS.length - 1 && <div className={`step-line${done ? " done" : ""}`} />}
          </React.Fragment>
        );
      })}
    </nav>
  );
}

// ── Step 1: data ─────────────────────────────────────────────────────────────
function DataStep({ onNext }) {
  const { csvFile, csvRestoring, setCsvFile, detectedSchema, setDetectedSchema, setPlanResult } = useAppContext();
  const [over, setOver] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const input = useRef();

  async function pick(file) {
    if (!file) return;
    if (!/\.(csv|json|jsonl|ndjson)$/i.test(file.name)) { setError("Choose a .csv or .json file."); return; }
    setError(""); setBusy(true);
    try {
      const result = await schemaApi.detect(file);
      setCsvFile(file);
      setDetectedSchema({ ...result, file_name: file.name });
      setPlanResult(null);     // a plan belongs to the data it was designed for
    } catch (e) {
      setError(`Could not read the file: ${e.message}`);
    } finally {
      setBusy(false);
    }
  }

  const cols = Object.entries(detectedSchema?.columns || {});
  const rows = detectedSchema?.row_count ?? detectedSchema?.row_count_sample;
  const preview = (detectedSchema?.preview || []).slice(0, 5);
  const ready = !!csvFile && !!detectedSchema;

  return (
    <>
      <input ref={input} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden
        onChange={(e) => { pick(e.target.files[0]); e.target.value = ""; }} />

      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}

      {csvRestoring ? (
        <Card><div className="row" style={{ gap: 10, color: "var(--text-3)" }}><Spinner /> Restoring your data file…</div></Card>
      ) : !ready ? (
        <div
          className={`dropzone${over ? " over" : ""}`}
          onClick={() => !busy && input.current.click()}
          onDragOver={(e) => { e.preventDefault(); setOver(true); }}
          onDragLeave={() => setOver(false)}
          onDrop={(e) => { e.preventDefault(); setOver(false); pick(e.dataTransfer.files[0]); }}
          role="button" tabIndex={0}
          onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && input.current.click()}
        >
          <div className="dropzone-icon">{busy ? <Spinner size={18} /> : <Upload size={20} strokeWidth={1.8} />}</div>
          <div style={{ fontSize: 15, fontWeight: 500, color: "var(--text)" }}>
            {busy ? "Reading your file…" : "Drop a data file here, or click to choose"}
          </div>
          <div style={{ fontSize: 13, color: "var(--text-3)", marginTop: 4 }}>
            CSV with a header row, a JSON array of objects, or NDJSON
          </div>
          {detectedSchema && !csvFile && (
            <div style={{ fontSize: 12.5, color: "var(--warn)", marginTop: 10 }}>
              Your previous file “{detectedSchema.file_name || "data"}” could not be restored — choose it again.
            </div>
          )}
        </div>
      ) : (
        <div className="stack" style={{ gap: 14 }}>
          <Card
            title={<span className="mono">{csvFile.name}</span>}
            icon={FileText}
            subtitle={`${rows?.toLocaleString() ?? "?"} rows · ${cols.length} columns · ${(detectedSchema.file_format || "csv").toUpperCase()} · ${(csvFile.size / 1024).toFixed(1)} KB`}
            actions={<Button size="sm" icon={RotateCcw} loading={busy} onClick={() => input.current.click()}>Change file</Button>}
          >
            <div className="field-label">Columns</div>
            <div className="chips">
              {cols.map(([c, t]) => (
                <span key={c} className="chip"><span className="mono">{c}</span><small>{t}</small></span>
              ))}
            </div>
          </Card>

          {preview.length > 0 && (
            <Card title="First rows" subtitle="A sample of what the pipeline will read" pad={false}>
              <div style={{ overflowX: "auto" }}>
                <table className="preview-table">
                  <thead><tr>{cols.map(([c]) => <th key={c}>{c}</th>)}</tr></thead>
                  <tbody>
                    {preview.map((r, i) => (
                      <tr key={i}>{cols.map(([c]) => <td key={c}>{r[c] == null || r[c] === "" ? <span style={{ color: "var(--text-4)" }}>—</span> : String(r[c])}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      <div className="wizard-foot">
        <div className="spacer" />
        <Button variant="primary" size="lg" disabled={!ready} onClick={onNext}>
          Continue <ArrowRight size={15} />
        </Button>
      </div>
    </>
  );
}

// ── Step 2: describe ─────────────────────────────────────────────────────────
function DescribeStep({ onBack, onPlanned }) {
  const { detectedSchema, plannerPrompt, setPlannerPrompt, planResult, setPlanResult } = useAppContext();
  const [prompt, setPrompt] = useState(plannerPrompt || "");
  const [mode, setMode] = useState(planResult?.config?.mode === "streaming" ? "streaming" : "batch");
  const [layout, setLayout] = useState(planResult?.config?.streaming?.layout === "multi" ? "multi" : "single");
  const [containers, setContainers] = useState("");
  const [planning, setPlanning] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [error, setError] = useState("");
  const ideas = useMemo(() => suggestions(detectedSchema?.columns, detectedSchema?.preview), [detectedSchema]);

  useEffect(() => {
    if (!planning) return undefined;
    setElapsed(0);
    const t0 = Date.now();
    const t = setInterval(() => setElapsed(Math.round((Date.now() - t0) / 1000)), 1000);
    return () => clearInterval(t);
  }, [planning]);

  async function design() {
    if (!prompt.trim() || !detectedSchema) return;
    setError(""); setPlanning(true);
    setPlannerPrompt(prompt);
    try {
      const opts = {};
      if (mode === "streaming") { opts.mode = "streaming"; opts.stream_layout = layout; }
      const n = parseInt(containers, 10);
      if (n >= 2 && n <= 10) opts.num_containers = n;
      const result = await planner.plan({
        columns: detectedSchema.columns,
        row_count: detectedSchema.row_count ?? detectedSchema.row_count_sample,
        size_hint: detectedSchema.size_hint || "medium",
        preview: detectedSchema.preview,
      }, prompt, opts);
      setPlanResult(result);
      onPlanned();
    } catch (e) {
      setError(`The planner could not design this pipeline: ${e.message}`);
    } finally {
      setPlanning(false);
    }
  }

  return (
    <>
      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}
      <div className="grid describe-grid">
        <Card title="What should happen to this data?" icon={Sparkles}
          subtitle="Plain English. Number the steps if the order matters.">
          <textarea
            className="input" rows={5} value={prompt} disabled={planning}
            onChange={(e) => setPrompt(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) design(); }}
            placeholder="e.g. Keep only active customers, then total order value per region."
            style={{ fontSize: 14 }}
          />
          <div className="field-label" style={{ marginTop: 14 }}>Ideas for your columns</div>
          <div className="chips">
            {ideas.map((s) => (
              <button key={s} className="chip" disabled={planning} onClick={() => setPrompt(s)}>{s}</button>
            ))}
          </div>
        </Card>

        <Card title="Options" subtitle="Defaults work for most data">
          <div className="stack" style={{ gap: 16 }}>
            <Field label="Processing" hint={mode === "batch"
              ? "Processes the whole file once."
              : "Each run processes only newly arrived data (checkpointed)."}>
              <Segmented value={mode} onChange={setMode} options={[
                { value: "batch", label: "Batch" }, { value: "streaming", label: "Streaming" },
              ]} />
            </Field>
            {mode === "streaming" && (
              <Field label="Streaming stages" hint={layout === "single"
                ? "All steps in one stage — cheapest."
                : "One chained stage per step."}>
                <Segmented value={layout} onChange={setLayout} options={[
                  { value: "single", label: "Single" }, { value: "multi", label: "Multiple" },
                ]} />
              </Field>
            )}
            <Field label="Storage containers" hint="Leave empty to let the planner decide (2–10).">
              <input className="input" type="number" min={2} max={10} placeholder="auto"
                value={containers} onChange={(e) => setContainers(e.target.value)} />
            </Field>
          </div>
        </Card>
      </div>

      {planning && (
        <Card style={{ marginTop: 14 }}>
          <div className="row" style={{ gap: 12 }}>
            <Spinner size={16} />
            <div>
              <div style={{ color: "var(--text)", fontWeight: 500 }}>
                {elapsed < 25 ? "Designing your pipeline…" : "Checking the design against your request…"}
              </div>
              <div style={{ fontSize: 12.5, color: "var(--text-3)" }}>
                The planner model designs the stages, then verifies them and fixes problems it finds. Usually under a minute · {elapsed}s
              </div>
            </div>
          </div>
        </Card>
      )}

      <div className="wizard-foot">
        <Button variant="ghost" icon={ArrowLeft} onClick={onBack} disabled={planning}>Back</Button>
        <div className="spacer" />
        {planResult && !planning && (
          <Button onClick={onPlanned}>Keep current design</Button>
        )}
        <Button variant="primary" size="lg" icon={Wand2} loading={planning} disabled={!prompt.trim()} onClick={design}>
          {planResult ? "Design again" : "Design pipeline"}
        </Button>
      </div>
    </>
  );
}

// ── Step 3: review ───────────────────────────────────────────────────────────
function ReviewStep({ onBack, onRun }) {
  const { planResult, csvFile, runSchema, plannerPrompt, startRun } = useAppContext();
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState("");
  const cfg = planResult?.config;
  const v = planResult?.verification;
  const final = v?.final;
  const failures = (final?.structural_results || []).filter((c) => !c.passed);
  const issues = final?.semantic_result?.flagged ? final.semantic_result.issues || [] : [];
  const streaming = cfg?.mode === "streaming";
  const rs = cfg?.recommended_settings || {};

  async function run() {
    setError(""); setStarting(true);
    try {
      await startRun({ file: csvFile, config: cfg, schema: runSchema, request: plannerPrompt, origin: "wizard" });
      onRun();
    } catch (e) {
      setError(`Could not start the run: ${e.message}`);
    } finally {
      setStarting(false);
    }
  }

  if (!cfg) return <Empty title="No design yet" action={<Button onClick={onBack}>Describe your pipeline</Button>} />;

  return (
    <>
      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}

      <div className="stack" style={{ gap: 14 }}>
        <Card title="Your pipeline" icon={GitBranch}
          subtitle={`${cfg.stages?.length || 0} stage(s)${streaming ? ` · streaming (${cfg.streaming?.layout === "multi" ? "multiple stages" : "single stage"})` : " · batch"}`}
          actions={planResult.used_fallback ? <Badge tone="warn">fallback design</Badge> : null}>
          <PipelineFlow plan={cfg} inputLabel={csvFile?.name} />
          {cfg.reasoning && <p style={{ fontSize: 13, color: "var(--text-3)", marginTop: 12 }}>{cfg.reasoning}</p>}
          {cfg.streaming?.layout_note && <Alert tone="accent" style={{ marginTop: 12 }}>{cfg.streaming.layout_note}</Alert>}
        </Card>

        <div className="grid grid-2">
          <Card title="Self-check" icon={ShieldCheck}
            actions={v ? (v.verified ? <Badge tone="ok" dot>verified</Badge> : <Badge tone="warn" dot>needs a look</Badge>) : null}>
            {!v ? (
              <div style={{ fontSize: 13, color: "var(--text-3)" }}>No self-check result for this design.</div>
            ) : (
              <div className="stack" style={{ gap: 8, fontSize: 13 }}>
                <div style={{ color: "var(--text-2)" }}>
                  {v.verified
                    ? "The planner checked this design against your data and request and found no problems."
                    : "The planner found problems it could not fix. Review them, then describe the pipeline again or run it anyway."}
                  {v.replanned ? ` It redesigned once (${v.attempts} attempts).` : ""}
                </div>
                {failures.map((c) => (
                  <div key={c.check} className="row" style={{ gap: 8, alignItems: "flex-start", color: "var(--bad)" }}>
                    <XCircle size={14} style={{ marginTop: 3, flexShrink: 0 }} /> <span><b>{c.label}:</b> {c.message}</span>
                  </div>
                ))}
                {issues.map((it, i) => (
                  <div key={i} className="row" style={{ gap: 8, alignItems: "flex-start", color: "var(--warn)" }}>
                    <AlertTriangle size={14} style={{ marginTop: 3, flexShrink: 0 }} />
                    <span><b className="mono">{it.stage}</b>: {it.problem}{it.suggestion ? ` — ${it.suggestion}` : ""}</span>
                  </div>
                ))}
              </div>
            )}
          </Card>
          <Card title="Starting resources" icon={BarChart3}
            subtitle="The Resource and Cost agents refine these per stage before running">
            <KV items={[
              ["Copy throughput (DIU)", rs.diu ?? "auto"],
              ["Databricks workers", rs.num_workers ?? "auto"],
              ["Shuffle partitions", rs.shuffle_partitions ?? "auto"],
              ["Node type", rs.node_type ?? "auto"],
            ]} />
          </Card>
        </div>

        {!csvFile && (
          <Alert tone="warn" title="Data file missing">
            The data file is no longer available in this browser. Go back to step 1 and choose it again.
          </Alert>
        )}
      </div>

      <div className="wizard-foot">
        <Button variant="ghost" icon={ArrowLeft} onClick={onBack} disabled={starting}>Change request</Button>
        <div className="spacer" />
        <Button variant="primary" size="lg" icon={Play} loading={starting} disabled={!csvFile} onClick={run}>
          {streaming ? "Run once (seed data)" : "Run pipeline"}
        </Button>
      </div>
    </>
  );
}

// ── Step 4: run ──────────────────────────────────────────────────────────────
function RunStep({ onDone }) {
  const { run, runId, planResult, csvFile } = useAppContext();
  const live = !run || isLive(run.status);
  useEffect(() => { if (run && !live) onDone(); }, [run, live]); // eslint-disable-line react-hooks/exhaustive-deps

  const started = run?.started_at ? new Date(run.started_at) : null;
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="stack" style={{ gap: 14 }}>
      <Card>
        <div className="row" style={{ gap: 14 }}>
          <Spinner size={18} />
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ color: "var(--text)", fontWeight: 600, fontSize: 15 }}>
              {run?.step || "Starting…"}
            </div>
            <div style={{ fontSize: 12.5, color: "var(--text-3)" }}>
              Run <span className="mono">{runId?.slice(0, 8)}</span>
              {started ? ` · ${fmtDuration((now - started.getTime()) / 1000)} elapsed` : ""}
              {run?.retries > 0 ? ` · ${run.retries} retr${run.retries === 1 ? "y" : "ies"}` : ""}
              {" · you can leave this page, the run keeps going"}
            </div>
          </div>
        </div>
      </Card>
      <Card title="Agents" subtitle="Which agent is working on your run">
        <AgentFlow runState={run} hasPlan height={300} />
      </Card>
      <Card title="Pipeline" subtitle="Stages light up as the executor reaches them">
        <PipelineFlow plan={run?.plan?.stages?.length ? run.plan : planResult?.config} runState={run} inputLabel={csvFile?.name} />
      </Card>
    </div>
  );
}

// ── Step 5: results ──────────────────────────────────────────────────────────
function ResultsStep({ onRestart, onReplan }) {
  const navigate = useNavigate();
  const { run, csvFile } = useAppContext();
  const [dlError, setDlError] = useState("");
  if (!run) return <div className="row" style={{ gap: 8, color: "var(--text-3)" }}><Spinner /> Loading the run…</div>;

  const ok = run.status === "completed";
  const duration = run.started_at && run.completed_at
    ? (new Date(run.completed_at) - new Date(run.started_at)) / 1000 : null;
  const sink = run.executor_result?.sink_container;
  const cost = run.cost_optimization?.estimated_cost?.total_usd ?? run.cost_estimate?.total_usd;

  return (
    <div className="stack" style={{ gap: 14 }}>
      <div className="big-status" style={{
        background: ok ? "var(--ok-soft)" : "var(--bad-soft)", borderColor: ok ? "var(--ok-line)" : "var(--bad-line)",
      }}>
        <div className="icon" style={{ background: "var(--surface)", color: ok ? "var(--ok)" : "var(--bad)" }}>
          {ok ? <CheckCircle2 size={22} /> : <XCircle size={22} />}
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 16, fontWeight: 600, color: "var(--text)" }}>
            {ok ? "Your pipeline ran successfully" : "The run failed"}
          </div>
          <div style={{ fontSize: 13, color: "var(--text-2)", wordBreak: "break-word" }}>
            {ok ? `Output is in the “${sink || "result"}” container.` : (run.error || run.step)}
          </div>
          {dlError && <div style={{ fontSize: 12.5, color: "var(--bad)", marginTop: 4 }}>{dlError}</div>}
        </div>
        {ok && sink && (
          <Button variant="primary" icon={Download}
            onClick={() => { setDlError(""); executor.download(sink).catch((e) => setDlError(e.message)); }}>
            Download output
          </Button>
        )}
      </div>

      <div className="grid grid-4">
        <div className="card card-pad">
          <div className="stat-label"><Clock size={14} /> Duration</div>
          <div className="stat-value">{fmtDuration(duration)}</div>
          <div className="stat-sub">predicted {fmtDuration(run.performance_prediction?.predicted_total_s)}</div>
        </div>
        <div className="card card-pad">
          <div className="stat-label"><GitBranch size={14} /> Stages</div>
          <div className="stat-value">{(run.executor_result?.stages_completed || run.executor_result?.stages || []).length}/{run.plan?.stages?.length ?? "?"}</div>
          <div className="stat-sub">completed</div>
        </div>
        <div className="card card-pad">
          <div className="stat-label"><BarChart3 size={14} /> Estimated cost</div>
          <div className="stat-value">{typeof cost === "number" ? `$${cost.toFixed(4)}` : "—"}</div>
          <div className="stat-sub">Azure estimate</div>
        </div>
        <div className="card card-pad">
          <div className="stat-label"><ShieldCheck size={14} /> Output checks</div>
          <div className="stat-value" style={{ color: run.assurance?.passed ? "var(--ok)" : run.assurance && Object.keys(run.assurance).length ? "var(--warn)" : undefined }}>
            {run.assurance && Object.keys(run.assurance).length ? (run.assurance.passed ? "Passed" : "Warnings") : "—"}
          </div>
          <div className="stat-sub">assurance agent</div>
        </div>
      </div>

      <Card title="How the run went" pad>
        <AgentFlow runState={run} hasPlan height={280} />
        <div style={{ height: 12 }} />
        <PipelineFlow plan={run.plan} runState={run} inputLabel={csvFile?.name} />
      </Card>

      <div className="wizard-foot">
        <Button icon={BarChart3} onClick={() => navigate("/insights")}>Open in Run Insights</Button>
        {!ok && <Button icon={Wand2} onClick={onReplan}>Change the request</Button>}
        <div className="spacer" />
        <Button variant="primary" icon={RotateCcw} onClick={onRestart}>Start another pipeline</Button>
      </div>
    </div>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────
export default function NewPipeline() {
  const ctx = useAppContext();
  const { csvFile, detectedSchema, planResult, run, runId, runOrigin, clearRun, setCsvFile, setDetectedSchema, setPlanResult, setPlannerPrompt } = ctx;

  // Where the user is, derived from shared state so a reload lands on the
  // right step: a guided run in progress → Run, finished → Results, a plan →
  // Review, a file → Describe. Explicit navigation overrides it.
  const guidedRun = runId && runOrigin === "wizard";
  const derived = guidedRun ? (run && !isLive(run.status) ? 4 : 3)
    : planResult ? 2 : (csvFile && detectedSchema) ? 1 : 0;
  const [picked, setPicked] = useState(null);
  const step = picked ?? derived;

  const reachable = (i) => {
    if (guidedRun && isLive(run?.status)) return i === 3;     // don't wander off mid-run
    if (i === 0) return true;
    if (i === 1) return !!(csvFile && detectedSchema);
    if (i === 2) return !!planResult;
    if (i === 3 || i === 4) return !!guidedRun;
    return false;
  };

  function restart() {
    clearRun(); setCsvFile(null); setDetectedSchema(null); setPlanResult(null); setPlannerPrompt("");
    setPicked(0);
  }

  return (
    <div>
      <PageHeader
        eyebrow="Guided" icon={Sparkles}
        title="New pipeline"
        description="From a data file to a running Azure pipeline in five steps. Everything here is shared with the Planner, Central Manager and Executor pages."
      />
      <Stepper current={step} reachable={reachable} onGo={setPicked} />

      {step === 0 && <DataStep onNext={() => setPicked(1)} />}
      {step === 1 && <DescribeStep onBack={() => setPicked(0)} onPlanned={() => setPicked(2)} />}
      {step === 2 && <ReviewStep onBack={() => setPicked(1)} onRun={() => setPicked(3)} />}
      {step === 3 && (guidedRun
        ? <RunStep onDone={() => setPicked(4)} />
        : <Empty title="No run yet" action={<Button onClick={() => setPicked(2)}>Review the design</Button>} />)}
      {step === 4 && (guidedRun
        ? <ResultsStep onRestart={restart} onReplan={() => setPicked(1)} />
        : <Empty title="No results yet" />)}
    </div>
  );
}
