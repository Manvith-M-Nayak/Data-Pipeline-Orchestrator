import React, { useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import {
  ArrowRight, Brain, FileText, GitBranch, RotateCcw, Settings, ShieldCheck, Sparkles, Upload, Wand2,
} from "lucide-react";
import { schema as schemaApi, planner, assurance } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import {
  Alert, Badge, Button, Card, Field, PageHeader, Segmented, Spinner,
} from "../ui/components.jsx";

// Mirrors DEFAULT_EDITABLE_SETTINGS in planner_agent/planner_common.py —
// used until a plan arrives with its own editable_settings.
const DEFAULT_EDITABLE = {
  diu:                [1, 2, 4, 8, 16, 32],
  num_workers:        [0, 2, 4, 8, 16],
  shuffle_partitions: [4, 8, 16, 32, 64],
  node_type:          ["Standard_D4s_v3", "Standard_DS4_v2", "Standard_D8s_v3"],
};

const SETTING_LABELS = {
  diu:                "Copy throughput (DIU)",
  num_workers:        "Databricks workers",
  shuffle_partitions: "Shuffle partitions",
  node_type:          "Node type",
};

const EXAMPLE_PROMPTS = [
  "Filter rows where status is 'active' and calculate average amount by region.",
  "Remove duplicates, compute total sales per product, flag products below 100 units.",
  "Group by department, calculate average salary, flag departments above $80,000.",
  "Convert temperature from Celsius to Fahrenheit, keep readings above 25°C.",
];

export default function PlannerTab() {
  const navigate = useNavigate();
  const {
    csvFile, setCsvFile, csvName,
    detectedSchema: detected, setDetectedSchema: setDetected,
    plannerPrompt:  prompt,   setPlannerPrompt:  setPrompt,
    planResult:     plan,     setPlanResult:     setPlan,
  } = useAppContext();

  const [dragging,  setDragging]  = useState(false);
  const [detecting, setDetecting] = useState(false);
  const [planning,  setPlanning]  = useState(false);
  const [error,     setError]     = useState("");
  const fileRef = useRef();

  // ── assurance (plan validation) ────────────────────────────────────────────
  const [assuring,        setAssuring]        = useState(false);
  // Restored with the plan after a reload: the Planner's self-check result is
  // saved inside the plan (plan.verification), so it is shown again.
  const [assuranceResult, setAssuranceResult] = useState(() => plan?.verification?.final || null);
  // true when assuranceResult is the Planner's own self-check (vs a manual re-check)
  const [assuranceFromPlanner, setAssuranceFromPlanner] = useState(() => !!plan?.verification?.final);

  async function handleValidate() {
    if (!plan?.config) return;
    setError(""); setAssuring(true); setAssuranceResult(null); setAssuranceFromPlanner(false);
    try {
      const res = await assurance.validate(prompt, plan.config, { columns: detected?.columns || {} });
      setAssuranceResult(res);
    } catch (e) { setError("Assurance failed: " + e.message); }
    finally { setAssuring(false); }
  }

  // ── pipeline settings (user overrides; null/"" = auto/recommended) ────────
  // Start from the restored plan's mode/layout so "Re-generate" after a page
  // reload doesn't silently turn a streaming plan back into batch.
  const [pipelineMode,   setPipelineMode]   = useState(
    () => (plan?.config?.mode === "streaming" ? "streaming" : "batch"));            // batch | streaming
  const [streamLayout,   setStreamLayout]   = useState(
    () => (plan?.config?.streaming?.layout === "multi" ? "multi" : "single"));    // single | multi (streaming only)
  const [numStages,      setNumStages]      = useState(null);   // null = model decides
  const [containerNames, setContainerNames] = useState("");
  const [overrides,      setOverrides]      = useState({
    diu: "", num_workers: "", shuffle_partitions: "", node_type: "",
  });

  function buildPlanOpts() {
    const opts = {};
    if (pipelineMode === "streaming") {
      opts.mode = "streaming";
      opts.stream_layout = streamLayout;
    }
    if (numStages !== null) opts.num_containers = numStages;
    const custom = {};
    Object.entries(overrides).forEach(([k, v]) => {
      if (v !== "") custom[k] = k === "node_type" ? v : Number(v);
    });
    if (Object.keys(custom).length) opts.custom_settings = custom;
    const names = containerNames.split(",").map((s) => s.trim()).filter(Boolean);
    if (numStages !== null && names.length === numStages) opts.container_names = names;
    return opts;
  }

  const containerNameCount = containerNames.split(",").map((s) => s.trim()).filter(Boolean).length;
  const containerNamesMismatch =
    containerNameCount > 0 && (numStages === null || containerNameCount !== numStages);

  async function handleFile(file) {
    const ok = /\.(csv|json|jsonl|ndjson)$/i.test(file?.name || "");
    if (!ok) { setError("Upload a .csv or .json file."); return; }
    setError(""); setCsvFile(file); setDetecting(true); setDetected(null); setPlan(null);
    try {
      const result = await schemaApi.detect(file);
      // One stored schema: the run schema (AppContext.runSchema) is derived
      // from it, and file_name lets the Executor spot a different file.
      setDetected({ ...result, file_name: file.name });
    } catch (e) {
      // Drop the file too: keeping it without a schema shows "undefined columns".
      setCsvFile(null);
      setError("Could not read file: " + e.message);
    }
    finally { setDetecting(false); }
  }

  function onDrop(e) { e.preventDefault(); setDragging(false); handleFile(e.dataTransfer.files[0]); }

  function buildSchemaPayload() {
    return {
      columns: detected.columns,
      row_count: detected.row_count ?? detected.row_count_sample,
      size_hint: detected.size_hint || "medium",
      preview: detected.preview,
    };
  }

  async function handlePlan(extraInstructions = "") {
    if (!prompt.trim() || !detected) return;
    // guard: buttons pass the click event as the first arg
    const extra = typeof extraInstructions === "string" ? extraInstructions : "";
    setError(""); setPlanning(true); setPlan(null); setAssuranceResult(null);
    try {
      const fullPrompt = extra ? `${prompt}\n\n${extra}` : prompt;
      const result = await planner.plan(buildSchemaPayload(), fullPrompt, buildPlanOpts());
      setPlan(result);
      // The Planner already verified this plan (structure + intent) — show it.
      if (result?.verification?.final) {
        setAssuranceResult(result.verification.final);
        setAssuranceFromPlanner(true);
      }
    } catch (e) { setError("Planner failed: " + e.message); }
    finally { setPlanning(false); }
  }

  // Feed assurance findings back to the planner as corrective instructions.
  async function handleReplanWithFixes() {
    if (!assuranceResult) return;
    const lines = [];
    (assuranceResult.structural_results || [])
      .filter((c) => !c.passed)
      .forEach((c) => lines.push(`- ${c.label}: ${c.message}`));
    const sem = assuranceResult.semantic_result;
    if (sem?.flagged) {
      lines.push(`- ${sem.reasoning}`);
      (sem.issues || []).forEach((it) =>
        lines.push(`- Stage '${it.stage}': ${it.problem}${it.suggestion ? ` — fix: ${it.suggestion}` : ""}`));
    }
    if (!lines.length) return;
    const constraints = [];
    if (numStages !== null) {
      constraints.push(
        `The pipeline must have exactly ${numStages - 1} stage(s); ` +
        "distribute the operations across ALL of them in the order the request numbers them — " +
        "do not stack multiple operations into one stage while leaving others empty."
      );
    }
    await handlePlan(
      "IMPORTANT — a previous plan for this request was rejected by review. " +
      "Generate a corrected plan that fixes these issues:\n" + lines.join("\n") +
      (constraints.length ? "\n" + constraints.join("\n") : "")
    );
  }

  function reset() {
    setCsvFile(null); setDetected(null); setPrompt(""); setPlan(null); setError("");
    setAssuranceResult(null); setAssuranceFromPlanner(false);
  }

  // ── execution flow (concurrency) editing ──────────────────────────────────
  const cfg = plan?.config;

  // A notebook stage with no operations just copies its input forward.
  const isPassThrough = (s) =>
    s.type === "notebook" &&
    !(s.transformations || []).some((t) => t && t.trim()) &&
    !s.filter_condition &&
    !(s.aggregation?.aggregations?.length);
  const passThroughStages = (cfg?.stages || []).filter(isPassThrough);
  const stageNames = cfg?.stages?.map((s) => s.name) || [];
  const execGroups = cfg?.execution_groups?.length
    ? cfg.execution_groups
    : stageNames.map((n) => [n]);

  function stageGroupIndex(name) {
    const i = execGroups.findIndex((g) => g.includes(name));
    return i === -1 ? 0 : i;
  }

  function setStageGroup(name, gi) {
    const idx = {};
    stageNames.forEach((n) => { idx[n] = stageGroupIndex(n); });
    idx[name] = gi;
    const rebuilt = [];
    stageNames.forEach((n) => {
      (rebuilt[idx[n]] = rebuilt[idx[n]] || []).push(n);
    });
    const cleaned = rebuilt.filter((g) => g && g.length);
    // Groups changed: the saved self-check no longer describes this plan.
    setPlan({ ...plan, verification: undefined, config: { ...cfg, execution_groups: cleaned } });
    setAssuranceResult(null);   // groups changed — previous validation is stale
  }

  const cols = Object.entries(detected?.columns || {});
  const rows = detected?.row_count ?? detected?.row_count_sample;

  // ── render ─────────────────────────────────────────────────────────────────
  return (
    <div>
      <PageHeader
        eyebrow="Build" icon={Brain}
        title="Planner"
        description="Describe what should happen to your data. The planner model designs the ADF + Databricks pipeline, then checks its own design against your data and request."
        actions={<Button size="sm" icon={Sparkles} onClick={() => navigate("/new")}>Guided mode</Button>}
      />

      {error && <Alert tone="bad" style={{ marginBottom: 14 }}>{error}</Alert>}

      <input ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden
        onChange={(e) => { handleFile(e.target.files[0]); e.target.value = ""; }} />

      {/* ── Data ── */}
      {!(csvFile && detected) && !detecting ? (
        <div
          className={`dropzone${dragging ? " over" : ""}`}
          style={{ marginBottom: 14 }}
          onClick={() => fileRef.current.click()}
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
          role="button" tabIndex={0}
          onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && fileRef.current.click()}
        >
          <div className="dropzone-icon"><Upload size={20} strokeWidth={1.8} /></div>
          <div style={{ fontSize: 15, fontWeight: 500, color: "var(--text)" }}>Drop a data file, or click to choose</div>
          <div className="muted" style={{ fontSize: 13, marginTop: 4 }}>CSV with a header row · JSON array of objects · NDJSON</div>
          {detected && !csvFile && (
            <div style={{ fontSize: 12.5, color: "var(--warn)", marginTop: 10 }}>
              The plan below was made for “{detected.file_name || csvName || "your file"}” — choose it again to run it.
            </div>
          )}
        </div>
      ) : (
        <Card
          style={{ marginBottom: 14 }}
          icon={FileText}
          title={detecting ? "Reading your file…" : <span className="mono">{csvFile?.name}</span>}
          subtitle={detecting ? undefined : `${rows?.toLocaleString() ?? "?"} rows · ${cols.length} columns · ${(detected?.file_format || "csv").toUpperCase()}`}
          actions={!detecting && <>
            <Button size="sm" icon={RotateCcw} onClick={() => fileRef.current.click()}>Change file</Button>
            <Button size="sm" variant="ghost" onClick={reset}>Clear</Button>
          </>}
        >
          {detecting ? <div className="row muted" style={{ gap: 8 }}><Spinner /> Detecting columns and types…</div> : (
            <>
              <div className="chips">
                {cols.map(([c, t]) => <span key={c} className="chip"><span className="mono">{c}</span><small>{t}</small></span>)}
              </div>
              {(detected?.preview || []).length > 0 && (
                <details style={{ marginTop: 12 }}>
                  <summary className="link" style={{ fontSize: 13 }}>Preview rows</summary>
                  <div style={{ overflowX: "auto", marginTop: 8 }}>
                    <table className="preview-table">
                      <thead><tr>{cols.map(([c]) => <th key={c}>{c}</th>)}</tr></thead>
                      <tbody>
                        {detected.preview.slice(0, 5).map((r, i) => (
                          <tr key={i}>{cols.map(([c]) => <td key={c}>{r[c] ?? "—"}</td>)}</tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              )}
            </>
          )}
        </Card>
      )}

      {/* ── Request + settings ── */}
      {detected && (
        <div className="grid describe-grid" style={{ marginBottom: 14 }}>
          <Card title="Your request" icon={Brain}
            subtitle={plan ? "Edit and design again to refine the plan." : "Plain English — number the steps if order matters."}>
            <textarea
              className="input" rows={4} value={prompt} disabled={planning}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="e.g. Filter active users, group by region, calculate average order value."
              onKeyDown={(e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) handlePlan(); }}
            />
            {!plan && (
              <>
                <div className="field-label" style={{ marginTop: 12 }}>Examples</div>
                <div className="chips">
                  {EXAMPLE_PROMPTS.map((p) => (
                    <button key={p} className="chip" onClick={() => setPrompt(p)}>{p}</button>
                  ))}
                </div>
              </>
            )}
            <div className="wizard-foot">
              {planning && <span className="muted" style={{ fontSize: 12.5 }}>Designing and self-checking — usually under a minute</span>}
              <div className="spacer" />
              <Button variant="primary" icon={Wand2} loading={planning} disabled={!prompt.trim()} onClick={handlePlan}>
                {plan ? "Design again" : "Design pipeline"}
              </Button>
            </div>
          </Card>

          <Card title="Settings" icon={Settings} subtitle="Optional — auto uses size-based recommendations">
            <div className="stack" style={{ gap: 14 }}>
              <Field label="Processing" hint={pipelineMode === "batch" ? "Whole dataset once." : "Only new data each run (checkpointed)."}>
                <Segmented value={pipelineMode} onChange={setPipelineMode}
                  options={[{ value: "batch", label: "Batch" }, { value: "streaming", label: "Streaming" }]} />
              </Field>
              {pipelineMode === "streaming" && (
                <Field label="Streaming stages" hint="Aggregations cover each run's new rows, not everything so far.">
                  <Segmented value={streamLayout} onChange={setStreamLayout}
                    options={[{ value: "single", label: "Single" }, { value: "multi", label: "Multiple" }]} />
                </Field>
              )}
              <Field label="Storage containers"
                hint={numStages !== null ? `= 1 copy + ${numStages - 2} transform stage(s)` : "Auto — the planner decides (2–10)"}>
                <input className="input" type="number" min={2} max={10} value={numStages ?? ""} placeholder="auto"
                  onChange={(e) => {
                    const v = e.target.value;
                    setNumStages(v === "" ? null : Math.max(2, Math.min(10, Number(v) || 3)));
                  }} />
              </Field>
              <Field label="Container names"
                hint={containerNamesMismatch
                  ? <span style={{ color: "var(--warn)" }}>{numStages === null ? "Set the container count first — ignored" : `${containerNameCount} name(s) ≠ ${numStages} containers — ignored`}</span>
                  : "Comma-separated, optional"}>
                <input className="input" value={containerNames} placeholder="raw, bronze, silver"
                  onChange={(e) => setContainerNames(e.target.value)} />
              </Field>
              <details>
                <summary className="link" style={{ fontSize: 13 }}>Compute overrides</summary>
                <div className="stack" style={{ gap: 12, marginTop: 10 }}>
                  {Object.keys(SETTING_LABELS).map((key) => {
                    const options = cfg?.editable_settings?.[key] || DEFAULT_EDITABLE[key];
                    const recommended = cfg?.recommended_settings?.[key];
                    return (
                      <Field key={key} label={SETTING_LABELS[key]}>
                        <select className="input" value={overrides[key]}
                          onChange={(e) => setOverrides({ ...overrides, [key]: e.target.value })}>
                          <option value="">Auto{recommended !== undefined ? ` (recommended: ${recommended})` : ""}</option>
                          {options.map((o) => <option key={o} value={o}>{o}</option>)}
                        </select>
                      </Field>
                    );
                  })}
                </div>
              </details>
            </div>
          </Card>
        </div>
      )}

      {/* ── Plan ── */}
      {planning && !plan && (
        <Card><div className="row" style={{ gap: 12 }}><Spinner size={16} /> <span>Designing your pipeline, then checking it against your data and request…</span></div></Card>
      )}

      {plan && cfg && (
        <Card
          title="Pipeline plan" icon={GitBranch}
          subtitle={`${cfg.stages?.length || 0} stage(s) · ${cfg.mode === "streaming" ? `streaming, ${cfg.streaming?.layout === "multi" ? "multiple stages" : "single stage"}` : "batch"}`}
          actions={plan.used_fallback ? <Badge tone="warn">fallback design</Badge> : null}
        >
          {cfg.reasoning && <p className="muted" style={{ fontSize: 13, marginBottom: 12 }}>{cfg.reasoning}</p>}
          {cfg.streaming?.layout_note && <Alert tone="accent" style={{ marginBottom: 12 }}>{cfg.streaming.layout_note}</Alert>}
          {passThroughStages.length > 0 && (
            <Alert tone="warn" style={{ marginBottom: 12 }}>
              <span className="mono">{passThroughStages.map((s) => s.name).join(", ")}</span>{" "}
              {passThroughStages.length > 1 ? "do" : "does"} nothing except copy data forward. Lower the container count, or describe what each stage should do.
            </Alert>
          )}

          <PipelineFlow plan={cfg} inputLabel={csvFile?.name || csvName} />

          {cfg.recommended_settings && (
            <div className="row muted" style={{ gap: 14, flexWrap: "wrap", fontSize: 12.5, marginTop: 10 }}>
              <span>DIU <b style={{ color: "var(--text)" }}>{cfg.recommended_settings.diu}</b></span>
              <span>Workers <b style={{ color: "var(--text)" }}>{cfg.recommended_settings.num_workers}</b></span>
              <span>Shuffle <b style={{ color: "var(--text)" }}>{cfg.recommended_settings.shuffle_partitions}</b></span>
              <span>Node <b className="mono" style={{ color: "var(--text)" }}>{cfg.recommended_settings.node_type}</b></span>
            </div>
          )}

          {/* Execution flow — user-controlled concurrency */}
          {stageNames.length > 1 && (
            <details style={{ marginTop: 16 }}>
              <summary className="link" style={{ fontSize: 13 }}>Edit execution order ({execGroups.length} group{execGroups.length > 1 ? "s" : ""})</summary>
              <div className="muted" style={{ fontSize: 12.5, margin: "8px 0 10px" }}>
                Stages in the same group run in parallel; groups run in order. Data dependencies are checked and repaired at run time.
              </div>
              <div className="stack" style={{ gap: 8 }}>
                {execGroups.map((g, gi) => (
                  <div key={gi} className="row" style={{ gap: 8, flexWrap: "wrap" }}>
                    <span style={{ width: 74, fontSize: 12, fontWeight: 500, color: g.length > 1 ? "var(--violet)" : "var(--text-3)" }}>
                      Group {gi + 1}{g.length > 1 ? " · ∥" : ""}
                    </span>
                    {g.map((n) => {
                      const locked = cfg.stages.find((s) => s.name === n)?.type === "copy";
                      return (
                        <span key={n} className="chip">
                          <span className="mono">{n}</span>
                          <select value={gi} disabled={locked}
                            title={locked ? "The copy stage always runs first" : "Move to another group"}
                            onChange={(e) => setStageGroup(n, Number(e.target.value))}
                            style={{ background: "var(--surface-2)", border: "1px solid var(--border)", borderRadius: 5, fontSize: 11.5, color: "var(--text-2)" }}>
                            {stageNames.map((_, i) => <option key={i} value={i}>G{i + 1}</option>)}
                          </select>
                        </span>
                      );
                    })}
                  </div>
                ))}
              </div>
            </details>
          )}

          {/* Self-check / re-check result */}
          {assuranceResult && (() => {
            const pass = assuranceResult.overall_status === "pass";
            const failures = (assuranceResult.structural_results || []).filter((c) => !c.passed);
            const sem = assuranceResult.semantic_result;
            return (
              <Alert tone={pass ? "ok" : "bad"} style={{ marginTop: 16 }}
                title={<span><ShieldCheck size={14} style={{ verticalAlign: -2, marginRight: 6 }} />{assuranceResult.summary}</span>}
                action={(failures.length > 0 || sem?.flagged) ? (
                  <Button size="sm" icon={RotateCcw} loading={planning} onClick={handleReplanWithFixes}>Fix & design again</Button>
                ) : null}>
                <div className="stack" style={{ gap: 4, fontSize: 12.5 }}>
                  {assuranceFromPlanner && plan?.verification && (
                    <div className="muted">
                      Planner self-check: {plan.verification.verified ? "verified" : "open issues remain"}
                      {plan.verification.replanned ? ` after redesigning (${plan.verification.attempts} attempts)` : " on the first attempt"}
                    </div>
                  )}
                  {failures.map((c) => <div key={c.check} style={{ color: "var(--bad)" }}>✗ {c.label}: {c.message}</div>)}
                  {sem && (
                    <div style={{ color: sem.available ? (sem.flagged ? "var(--warn)" : "var(--text-3)") : "var(--text-4)" }}>
                      Intent ({sem.model || "semantic"}): {!sem.available ? "unavailable" : sem.flagged ? "flagged (advisory)" : "matches the request"} — {sem.reasoning}
                    </div>
                  )}
                  {(sem?.issues || []).map((it, i) => (
                    <div key={i} style={{ color: "var(--warn)" }}>
                      • <span className="mono">{it.stage}</span>: {it.problem}{it.suggestion ? <span className="muted"> — fix: {it.suggestion}</span> : null}
                    </div>
                  ))}
                </div>
              </Alert>
            );
          })()}

          <div className="wizard-foot">
            <Button icon={ShieldCheck} loading={assuring} onClick={handleValidate}>Re-check plan</Button>
            <Button variant="ghost" onClick={() => setPlan(null)}>Discard plan</Button>
            <div className="spacer" />
            <Button variant="primary" onClick={() => navigate("/manager")}>Send to Manager <ArrowRight size={15} /></Button>
          </div>
        </Card>
      )}
    </div>
  );
}
