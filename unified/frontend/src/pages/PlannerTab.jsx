import React, { useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { schema as schemaApi, planner, assurance } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import {
  Upload, Brain, CheckCircle, XCircle, Zap, RotateCcw, ArrowRight, Settings, ShieldCheck,
} from "lucide-react";

const C = {
  page:   { maxWidth: 760, margin: "0 auto" },
  header: { marginBottom: 28 },
  agent:  { display: "flex", alignItems: "center", gap: 10, marginBottom: 6 },
  agentBadge: {
    padding: "4px 12px", background: "var(--violet-soft)", border: "1px solid var(--violet-line)",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "var(--violet)",
    display: "flex", alignItems: "center", gap: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "var(--text)", marginBottom: 4 },
  sub:    { fontSize: 13, color: "var(--text-3)" },
  card:   { background: "var(--surface)", borderRadius: 14, padding: 24, border: "1px solid var(--border)", marginBottom: 16 },
  cardHdr:{ fontSize: 15, fontWeight: 700, color: "var(--text)", marginBottom: 4, display: "flex", alignItems: "center", gap: 8 },
  cardSub:{ fontSize: 13, color: "var(--text-3)", marginBottom: 18 },
  drop:   (active, hasFile) => ({
    border: `2px dashed ${hasFile ? "var(--ok)" : active ? "var(--accent)" : "var(--border-strong)"}`,
    borderRadius: 12, padding: "30px 20px", textAlign: "center", cursor: "pointer",
    background: active ? "var(--surface-2)" : "transparent", transition: "all 0.2s",
  }),
  table:  { width: "100%", borderCollapse: "collapse", fontSize: 12, marginTop: 4 },
  th:     { padding: "8px 10px", textAlign: "left", color: "var(--text-3)", borderBottom: "1px solid var(--border)", fontWeight: 600, fontSize: 11, textTransform: "uppercase" },
  td:     { padding: "7px 10px", borderBottom: "1px solid var(--divider)", color: "var(--text-2)", fontFamily: "monospace" },
  typeBadge: (t) => ({
    display: "inline-block", padding: "1px 7px", borderRadius: 10, fontSize: 11, fontWeight: 700,
    background: t === "integer" ? "var(--accent-soft)" : t === "double" ? "var(--violet-soft)" : "var(--ok-soft)",
    color:      t === "integer" ? "var(--accent)" : t === "double" ? "var(--violet)" : "var(--ok)",
  }),
  textarea: {
    width: "100%", background: "var(--surface-2)", border: "1px solid var(--border)",
    color: "var(--text)", borderRadius: 10, padding: "12px 14px", fontSize: 14,
    resize: "none", lineHeight: 1.6, outline: "none",
  },
  btnRow: { display: "flex", gap: 10, marginTop: 18, alignItems: "center", flexWrap: "wrap" },
  btnPrimary: (disabled) => ({
    padding: "10px 22px", background: disabled ? "var(--surface-2)" : "var(--accent)",
    color: disabled ? "var(--text-4)" : "var(--accent-fg)", border: "none", borderRadius: 10,
    cursor: disabled ? "not-allowed" : "pointer", fontSize: 13, fontWeight: 600,
    display: "inline-flex", alignItems: "center", gap: 7,
  }),
  btnSecondary: {
    padding: "10px 18px", background: "transparent", color: "var(--text-3)",
    border: "1px solid var(--border)", borderRadius: 10, cursor: "pointer",
    fontSize: 13, display: "inline-flex", alignItems: "center", gap: 6,
  },
  successBox: {
    background: "var(--ok-soft)", borderRadius: 10, padding: 16,
    border: "1px solid var(--ok-line)", marginTop: 14,
    display: "flex", alignItems: "flex-start", gap: 12,
  },
  errBox: {
    background: "var(--bad-soft)", borderRadius: 8, padding: "10px 14px", marginBottom: 14,
    color: "var(--bad)", fontSize: 13, display: "flex", gap: 8,
  },
};

function Spinner() {
  return (
    <span style={{
      display: "inline-block", width: 13, height: 13,
      border: "2px solid var(--border)", borderTopColor: "var(--violet)",
      borderRadius: "50%", animation: "spin 0.7s linear infinite",
    }} />
  );
}

// Mirrors DEFAULT_EDITABLE_SETTINGS in planner_agent/planner_common.py —
// used until a plan arrives with its own editable_settings.
const DEFAULT_EDITABLE = {
  diu:                [1, 2, 4, 8, 16, 32],
  num_workers:        [0, 2, 4, 8, 16],
  shuffle_partitions: [4, 8, 16, 32, 64],
  node_type:          ["Standard_D4s_v3", "Standard_DS4_v2", "Standard_D8s_v3"],
};

const SETTING_LABELS = {
  diu:                "DIU (Copy Activity)",
  num_workers:        "Notebook Workers",
  shuffle_partitions: "Shuffle Partitions",
  node_type:          "Node Type",
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

  return (
    <div style={C.page}>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>

      <div style={C.header}>
        <div style={C.agent}>
          <span style={C.agentBadge}><Brain size={13} /> Planner Agent</span>
        </div>
        <h1 style={C.title}>Design your pipeline</h1>
        <p style={C.sub}>Upload data, describe your goal — AI designs the ADF + Databricks pipeline config.</p>
      </div>

      {error && (
        <div style={C.errBox}><XCircle size={14} style={{ flexShrink: 0 }} />{error}</div>
      )}

      {/* Upload */}
      <div style={C.card}>
        <div style={C.cardHdr}><Upload size={16} color="var(--accent)" />Upload Data File</div>
        <div style={C.cardSub}>Drop a CSV or JSON file — column names and types detected automatically.</div>
        <div
          style={C.drop(dragging, !!csvFile)}
          onClick={() => fileRef.current.click()}
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <input ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden onChange={(e) => { handleFile(e.target.files[0]); e.target.value = ""; }} />
          <Upload size={32} color={csvFile ? "var(--ok)" : dragging ? "var(--accent)" : "var(--border-strong)"} style={{ marginBottom: 10 }} />
          {detecting ? (
            <div style={{ fontSize: 14, color: "var(--text-2)" }}>Detecting schema… <Spinner /></div>
          ) : csvFile && detected ? (
            <div style={{ fontSize: 14, color: "var(--ok)", fontWeight: 600 }}>
              <CheckCircle size={14} style={{ verticalAlign: "middle", marginRight: 6 }} />
              {csvFile.name} · {detected?.column_count} columns · {(detected?.row_count ?? detected?.row_count_sample)?.toLocaleString()} rows
              <span style={{ marginLeft: 10, fontSize: 12, color: "var(--text-3)", cursor: "pointer" }}
                onClick={(e) => { e.stopPropagation(); reset(); }}>
                Change
              </span>
            </div>
          ) : (
            <>
              <div style={{ fontSize: 14, color: "var(--text-3)", fontWeight: 600 }}>Click or drag-and-drop your CSV or JSON</div>
              <div style={{ fontSize: 12, color: "var(--text-4)" }}>CSV with a header row · JSON array of objects · NDJSON</div>
            </>
          )}
        </div>

        {/* Schema preview */}
        {detected && (
          <div style={{ marginTop: 16, overflowX: "auto" }}>
            <table style={C.table}>
              <thead>
                <tr>
                  <th style={C.th}>Column</th>
                  <th style={C.th}>Type</th>
                  {detected.preview?.[0] && Object.keys(detected.preview[0]).slice(0, 3).map((_, i) => (
                    <th key={i} style={C.th}>Sample {i + 1}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Object.entries(detected.columns || {}).map(([col, type]) => (
                  <tr key={col}>
                    <td style={{ ...C.td, fontWeight: 600, color: "var(--text)" }}>{col}</td>
                    <td style={C.td}><span style={C.typeBadge(type)}>{type}</span></td>
                    {(detected.preview || []).slice(0, 3).map((row, i) => (
                      <td key={i} style={C.td}>{row[col] ?? "—"}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Prompt — stays visible after planning so it can be edited + re-run */}
      {detected && (
        <div style={C.card}>
          <div style={C.cardHdr}><Brain size={16} color="var(--violet)" />Describe your goal</div>
          <div style={C.cardSub}>
            {plan
              ? "Edit the prompt and re-generate to refine the plan."
              : "Plain English — no technical knowledge needed."}
          </div>

          <textarea
            style={{ ...C.textarea, minHeight: 80 }}
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            placeholder="e.g. Filter active users, group by region, calculate average order value."
            onKeyDown={(e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) handlePlan(); }}
          />

          {!plan && (
            <>
              <div style={{ marginTop: 8, marginBottom: 6, fontSize: 11, color: "var(--text-4)" }}>Click an example to use it:</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {EXAMPLE_PROMPTS.map((p, i) => (
                  <button key={i} onClick={() => setPrompt(p)}
                    style={{ fontSize: 11, color: "var(--text-3)", background: "var(--surface-2)", border: "1px solid var(--border)", borderRadius: 6, padding: "4px 8px", cursor: "pointer", textAlign: "left" }}>
                    {p.slice(0, 55)}…
                  </button>
                ))}
              </div>
            </>
          )}

          <div style={C.btnRow}>
            <button style={C.btnPrimary(!prompt.trim() || planning)} disabled={!prompt.trim() || planning} onClick={handlePlan}>
              <Brain size={13} />{planning ? <><Spinner /> Planning…</> : plan ? "Re-generate Plan" : "Generate Pipeline Plan"}
            </button>
          </div>
        </div>
      )}

      {/* Pipeline settings — stage count + cloud resources */}
      {detected && (
        <div style={C.card}>
          <div style={C.cardHdr}>
            <Settings size={16} color="var(--warn)" />Pipeline Settings
            <span style={{ fontSize: 11, color: "var(--text-4)", fontWeight: 400 }}>(optional)</span>
          </div>
          <div style={C.cardSub}>
            Auto uses size-based recommendations. Override to control stage count and cloud resources
            {plan ? " — then re-plan to apply." : " before generating the plan."}
          </div>

          {/* Pipeline mode: batch (ETL, run-to-completion) vs streaming (incremental) */}
          <div style={{ marginBottom: 14 }}>
            <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 6 }}>Pipeline Mode</div>
            <div style={{ display: "flex", gap: 8 }}>
              {[
                { id: "batch",     label: "Batch (ETL)",  hint: "Process the whole dataset once, then finish." },
                { id: "streaming", label: "Streaming",    hint: "Incremental: each run processes only new data (checkpointed)." },
              ].map((m) => (
                <button
                  key={m.id}
                  onClick={() => setPipelineMode(m.id)}
                  title={m.hint}
                  style={{
                    flex: 1, padding: "10px 12px", cursor: "pointer", textAlign: "left",
                    borderRadius: 8, fontSize: 13, fontWeight: pipelineMode === m.id ? 700 : 400,
                    color: pipelineMode === m.id ? "var(--accent)" : "var(--text-2)",
                    background: pipelineMode === m.id ? "var(--accent-soft)" : "var(--surface-2)",
                    border: `1px solid ${pipelineMode === m.id ? "var(--accent)" : "var(--border-strong)"}`,
                  }}
                >
                  {m.label}
                  <div style={{ fontSize: 11, fontWeight: 400, color: "var(--text-3)", marginTop: 2 }}>{m.hint}</div>
                </button>
              ))}
            </div>
            {pipelineMode === "streaming" && (
              <div style={{ marginTop: 10 }}>
                <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 6 }}>Streaming Stages</div>
                <div style={{ display: "flex", gap: 8 }}>
                  {[
                    { id: "single", label: "Single stage",
                      hint: "All steps in one incremental stage — one Databricks job per run (fastest, cheapest)." },
                    { id: "multi",  label: "Multiple stages",
                      hint: "One incremental stage per step, chained with their own checkpoints — one job per stage per run." },
                  ].map((l) => (
                    <button
                      key={l.id}
                      onClick={() => setStreamLayout(l.id)}
                      title={l.hint}
                      style={{
                        flex: 1, padding: "8px 12px", cursor: "pointer", textAlign: "left",
                        borderRadius: 8, fontSize: 12, fontWeight: streamLayout === l.id ? 700 : 400,
                        color: streamLayout === l.id ? "var(--accent)" : "var(--text-2)",
                        background: streamLayout === l.id ? "var(--accent-soft)" : "var(--surface-2)",
                        border: `1px solid ${streamLayout === l.id ? "var(--accent)" : "var(--border-strong)"}`,
                      }}
                    >
                      {l.label}
                      <div style={{ fontSize: 11, fontWeight: 400, color: "var(--text-3)", marginTop: 2 }}>{l.hint}</div>
                    </button>
                  ))}
                </div>
                <div style={{ fontSize: 11, color: "var(--warn)", marginTop: 6 }}>
                  Each run processes only newly arrived data (checkpointed). Aggregations cover
                  each run's new rows, not everything so far.
                </div>
              </div>
            )}
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 4 }}>
                Storage Containers (2–10)
                <span style={{ color: "var(--text-4)", marginLeft: 6 }}>
                  {numStages !== null
                    ? `= 1 copy + ${numStages - 2} transform stage(s)`
                    : "auto — model decides"}
                </span>
              </div>
              <input
                type="number" min={2} max={10} value={numStages ?? ""} placeholder="auto"
                onChange={(e) => {
                  const v = e.target.value;
                  setNumStages(v === "" ? null : Math.max(2, Math.min(10, Number(v) || 3)));
                }}
                style={{ ...C.textarea, padding: "8px 10px", fontSize: 13 }}
                title="N containers = N−1 stages: the first stage is always an ADF Copy (ingest); the rest are Databricks notebooks. Leave empty to let the planner decide."
              />
            </div>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 4 }}>
                Container Names (comma-separated)
                {containerNamesMismatch && (
                  <span style={{ color: "var(--warn)", marginLeft: 6 }}>
                    {numStages === null
                      ? "set container count first — ignored"
                      : `${containerNameCount} name(s) ≠ ${numStages} containers — ignored`}
                  </span>
                )}
              </div>
              <input
                type="text" value={containerNames} placeholder="auto (e.g. raw, bronze, silver)"
                onChange={(e) => setContainerNames(e.target.value)}
                style={{ ...C.textarea, padding: "8px 10px", fontSize: 13 }}
              />
            </div>

            {Object.keys(SETTING_LABELS).map((key) => {
              const options = plan?.config?.editable_settings?.[key] || DEFAULT_EDITABLE[key];
              const recommended = plan?.config?.recommended_settings?.[key];
              return (
                <div key={key}>
                  <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 4 }}>{SETTING_LABELS[key]}</div>
                  <select
                    value={overrides[key]}
                    onChange={(e) => setOverrides({ ...overrides, [key]: e.target.value })}
                    style={{ ...C.textarea, padding: "8px 10px", fontSize: 13 }}
                  >
                    <option value="">
                      Auto{recommended !== undefined ? ` (recommended: ${recommended})` : " (recommended)"}
                    </option>
                    {options.map((o) => (
                      <option key={o} value={o}>{o}</option>
                    ))}
                  </select>
                </div>
              );
            })}
          </div>

          {plan && (
            <div style={C.btnRow}>
              <button
                style={C.btnPrimary(planning || !prompt.trim())}
                disabled={planning || !prompt.trim()}
                onClick={handlePlan}
              >
                <Settings size={13} />{planning ? <><Spinner /> Re-planning…</> : "Apply Settings & Re-plan"}
              </button>
            </div>
          )}
        </div>
      )}

      {/* Plan result */}
      {plan && (
        <div style={C.card}>
          <div style={C.cardHdr}><Brain size={16} color="var(--violet)" />Pipeline Plan — Ready</div>

          <div style={C.successBox}>
            <CheckCircle size={18} color="var(--ok)" style={{ flexShrink: 0, marginTop: 1 }} />
            <div>
              <div style={{ fontWeight: 700, color: "var(--ok)", marginBottom: 4 }}>
                Plan generated · {plan.config?.stages?.length} stage(s)
                {plan.used_fallback && <span style={{ marginLeft: 8, fontSize: 11, color: "var(--warn)" }}>fallback used</span>}
                {plan.config?.streaming?.layout && (
                  <span style={{ marginLeft: 8, fontSize: 11, color: "var(--accent)" }}>
                    streaming · {plan.config.streaming.layout === "multi"
                      ? `${plan.config.stages?.length || 0} stages` : "single stage"}
                  </span>
                )}
              </div>
              <div style={{ fontSize: 13, color: "var(--text-3)" }}>{plan.config?.reasoning}</div>
            </div>
          </div>

          {plan.config?.streaming?.layout_note && (
            <div style={{ fontSize: 12, color: "var(--accent)", background: "var(--accent-soft)", border: "1px solid var(--accent-line)",
                          borderRadius: 8, padding: "8px 10px", marginBottom: 10 }}>
              ℹ {plan.config.streaming.layout_note}
            </div>
          )}

          {passThroughStages.length > 0 && (
            <div style={{
              background: "var(--warn-soft)", border: "1px solid var(--warn-line)", borderRadius: 8,
              padding: "10px 14px", marginTop: 12, fontSize: 12, color: "var(--warn)",
            }}>
              ⚠ {passThroughStages.map((s) => s.name).join(", ")}{" "}
              {passThroughStages.length > 1 ? "do" : "does"} nothing except copy data forward.
              Reduce Storage Containers in Pipeline Settings, or re-plan with a prompt
              describing what each stage should do.
            </div>
          )}

          {/* Stages as a data-flow graph: input → copy → steps (parallel stacked) → output */}
          <div style={{ marginTop: 14 }}>
            <PipelineFlow plan={plan.config} inputLabel={csvFile?.name || csvName} />
          </div>

          {cfg?.recommended_settings && (
            <div style={{ marginTop: 12, fontSize: 12, color: "var(--text-3)" }}>
              Resources: DIU {cfg.recommended_settings.diu} ·{" "}
              workers {cfg.recommended_settings.num_workers} ·{" "}
              shuffle {cfg.recommended_settings.shuffle_partitions} ·{" "}
              {cfg.recommended_settings.node_type}
            </div>
          )}

          {/* Execution flow — user-controlled concurrency */}
          {stageNames.length > 1 && (
            <div style={{ marginTop: 18 }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: "var(--text)", marginBottom: 4 }}>
                Execution Flow
              </div>
              <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 10 }}>
                Stages in the same group run in parallel; groups run in order.
                Data dependencies are validated and auto-repaired at run time.
              </div>
              {execGroups.map((g, gi) => (
                <div key={gi} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                  <span style={{ fontSize: 11, color: g.length > 1 ? "var(--violet)" : "var(--text-3)", width: 70, flexShrink: 0, fontWeight: 600 }}>
                    Group {gi + 1}{g.length > 1 ? " ⚡" : ""}
                  </span>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {g.map((n) => {
                      const st = cfg.stages.find((s) => s.name === n);
                      const locked = st?.type === "copy";
                      return (
                        <span key={n} style={{
                          display: "inline-flex", alignItems: "center", gap: 6,
                          background: "var(--surface-2)", border: "1px solid var(--border)",
                          borderRadius: 8, padding: "4px 8px", fontSize: 11, color: "var(--text-2)",
                        }}>
                          {n}
                          <select
                            value={gi}
                            disabled={locked}
                            title={locked ? "Copy stage always runs first" : "Move to another group"}
                            onChange={(e) => setStageGroup(n, Number(e.target.value))}
                            style={{
                              background: "var(--surface)", color: locked ? "var(--text-4)" : "var(--text-2)",
                              border: "1px solid var(--border)", borderRadius: 6, fontSize: 11,
                            }}
                          >
                            {stageNames.map((_, i) => (
                              <option key={i} value={i}>G{i + 1}</option>
                            ))}
                          </select>
                        </span>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Assurance validation result */}
          {assuranceResult && (() => {
            const pass = assuranceResult.overall_status === "pass";
            const failures = (assuranceResult.structural_results || []).filter((c) => !c.passed);
            const sem = assuranceResult.semantic_result;
            return (
              <div style={{
                marginTop: 14, borderRadius: 10, padding: 12,
                background: pass ? "var(--ok-soft)" : "var(--bad-soft)",
                border: `1px solid ${pass ? "var(--ok-soft)" : "var(--bad-soft)"}`,
              }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: pass ? "var(--ok)" : "var(--bad)", marginBottom: failures.length || sem ? 8 : 0 }}>
                  <ShieldCheck size={13} style={{ verticalAlign: "middle", marginRight: 6 }} />
                  {assuranceResult.summary}
                </div>
                {assuranceFromPlanner && plan?.verification && (
                  <div style={{ fontSize: 12, color: "var(--text-2)", marginBottom: 6 }}>
                    Planner self-check: {plan.verification.verified ? "verified" : "open issues remain"}
                    {plan.verification.replanned
                      ? ` after re-planning (${plan.verification.attempts} attempts)`
                      : " on the first attempt"}
                  </div>
                )}
                {failures.map((c) => (
                  <div key={c.check} style={{ fontSize: 12, color: "var(--bad)", marginBottom: 4 }}>
                    ✗ {c.label}: {c.message}
                  </div>
                ))}
                {sem && (
                  <div style={{ fontSize: 12, color: sem.available ? (sem.flagged ? "var(--warn)" : "var(--text-3)") : "var(--text-4)" }}>
                    Intent ({sem.model || "semantic"}):{" "}
                    {!sem.available ? "unavailable" : sem.flagged ? "FLAGGED (advisory)" : "matches request"} — {sem.reasoning}
                  </div>
                )}
                {(sem?.issues?.length ?? 0) > 0 && (
                  <div style={{ marginTop: 6 }}>
                    {sem.issues.map((it, i) => (
                      <div key={i} style={{ fontSize: 12, color: "var(--warn)", marginBottom: 3 }}>
                        • <span style={{ fontWeight: 600 }}>{it.stage}</span>: {it.problem}
                        {it.suggestion && <span style={{ color: "var(--text-2)" }}> — fix: {it.suggestion}</span>}
                      </div>
                    ))}
                  </div>
                )}
                {(failures.length > 0 || sem?.flagged) && (
                  <button
                    style={{ ...C.btnSecondary, marginTop: 10, color: "var(--warn)", borderColor: "var(--warn-soft)" }}
                    disabled={planning}
                    onClick={handleReplanWithFixes}
                  >
                    <RotateCcw size={13} />{planning ? <><Spinner /> Re-planning…</> : "Fix & Re-plan"}
                  </button>
                )}
              </div>
            );
          })()}

          <div style={C.btnRow}>
            <button style={C.btnPrimary(false)} onClick={() => navigate("/manager")}>
              <Zap size={13} /> Send to Manager <ArrowRight size={13} />
            </button>
            <button style={{ ...C.btnSecondary, color: "var(--ok)", borderColor: "var(--ok-soft)" }} disabled={assuring} onClick={handleValidate}>
              <ShieldCheck size={13} />{assuring ? <><Spinner /> Checking…</> : "Re-check Plan"}
            </button>
            <button style={C.btnSecondary} onClick={() => { setPlan(null); }}>
              <RotateCcw size={13} /> Re-plan
            </button>
            <button style={C.btnSecondary} onClick={reset}>
              New dataset
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
