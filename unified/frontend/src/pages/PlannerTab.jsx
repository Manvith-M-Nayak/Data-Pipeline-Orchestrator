import React, { useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { schema as schemaApi, planner, assurance } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import {
  Upload, Brain, CheckCircle, XCircle, Zap, RotateCcw, ArrowRight, Settings, ShieldCheck,
} from "lucide-react";

const C = {
  page:   { maxWidth: 760, margin: "0 auto" },
  header: { marginBottom: 28 },
  agent:  { display: "flex", alignItems: "center", gap: 10, marginBottom: 6 },
  agentBadge: {
    padding: "4px 12px", background: "#2d1b69", border: "1px solid #4c1d95",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "#a78bfa",
    display: "flex", alignItems: "center", gap: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:    { fontSize: 13, color: "#64748b" },
  card:   { background: "#1e293b", borderRadius: 14, padding: 24, border: "1px solid #334155", marginBottom: 16 },
  cardHdr:{ fontSize: 15, fontWeight: 700, color: "#f1f5f9", marginBottom: 4, display: "flex", alignItems: "center", gap: 8 },
  cardSub:{ fontSize: 13, color: "#64748b", marginBottom: 18 },
  drop:   (active, hasFile) => ({
    border: `2px dashed ${hasFile ? "#22c55e" : active ? "#3b82f6" : "#334155"}`,
    borderRadius: 12, padding: "30px 20px", textAlign: "center", cursor: "pointer",
    background: active ? "#0f172a" : "transparent", transition: "all 0.2s",
  }),
  table:  { width: "100%", borderCollapse: "collapse", fontSize: 12, marginTop: 4 },
  th:     { padding: "8px 10px", textAlign: "left", color: "#64748b", borderBottom: "1px solid #334155", fontWeight: 600, fontSize: 11, textTransform: "uppercase" },
  td:     { padding: "7px 10px", borderBottom: "1px solid #1e293b", color: "#cbd5e1", fontFamily: "monospace" },
  typeBadge: (t) => ({
    display: "inline-block", padding: "1px 7px", borderRadius: 10, fontSize: 10, fontWeight: 700,
    background: t === "integer" ? "#1e3a5f" : t === "double" ? "#2d1b69" : "#1a2e1a",
    color:      t === "integer" ? "#38bdf8" : t === "double" ? "#a78bfa" : "#4ade80",
  }),
  textarea: {
    width: "100%", background: "#0f172a", border: "1px solid #334155",
    color: "#e2e8f0", borderRadius: 10, padding: "12px 14px", fontSize: 14,
    resize: "none", lineHeight: 1.6, outline: "none",
  },
  btnRow: { display: "flex", gap: 10, marginTop: 18, alignItems: "center", flexWrap: "wrap" },
  btnPrimary: (disabled) => ({
    padding: "10px 22px", background: disabled ? "#1e293b" : "#3b82f6",
    color: disabled ? "#475569" : "#fff", border: "none", borderRadius: 10,
    cursor: disabled ? "not-allowed" : "pointer", fontSize: 13, fontWeight: 600,
    display: "inline-flex", alignItems: "center", gap: 7,
  }),
  btnSecondary: {
    padding: "10px 18px", background: "transparent", color: "#64748b",
    border: "1px solid #334155", borderRadius: 10, cursor: "pointer",
    fontSize: 13, display: "inline-flex", alignItems: "center", gap: 6,
  },
  stageGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(170px, 1fr))", gap: 10, marginTop: 14 },
  stage: { background: "#0f172a", borderRadius: 8, padding: 12, border: "1px solid #334155" },
  stageName: { fontWeight: 700, fontSize: 13, color: "#f1f5f9", marginBottom: 5, overflowWrap: "anywhere" },
  stageType: (t) => ({
    display: "inline-block", padding: "2px 8px", borderRadius: 10, fontSize: 11, fontWeight: 600,
    background: t === "copy" ? "#1e3a5f" : "#2d1b69",
    color: t === "copy" ? "#38bdf8" : "#a78bfa", marginBottom: 5,
  }),
  stageDetail: { fontSize: 11, color: "#475569", lineHeight: 1.5 },
  successBox: {
    background: "#0d2b0d", borderRadius: 10, padding: 16,
    border: "1px solid #166534", marginTop: 14,
    display: "flex", alignItems: "flex-start", gap: 12,
  },
  errBox: {
    background: "#450a0a", borderRadius: 8, padding: "10px 14px", marginBottom: 14,
    color: "#f87171", fontSize: 13, display: "flex", gap: 8,
  },
};

function Spinner() {
  return (
    <span style={{
      display: "inline-block", width: 13, height: 13,
      border: "2px solid #334155", borderTopColor: "#a78bfa",
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
    csvFile, setCsvFile,
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
  const [assuranceResult, setAssuranceResult] = useState(null);

  async function handleValidate() {
    if (!plan?.config) return;
    setError(""); setAssuring(true); setAssuranceResult(null);
    try {
      const res = await assurance.validate(prompt, plan.config, { columns: detected?.columns || {} });
      setAssuranceResult(res);
    } catch (e) { setError("Assurance failed: " + e.message); }
    finally { setAssuring(false); }
  }

  // ── pipeline settings (user overrides; null/"" = auto/recommended) ────────
  const [numStages,      setNumStages]      = useState(null);   // null = model decides
  const [containerNames, setContainerNames] = useState("");
  const [overrides,      setOverrides]      = useState({
    diu: "", num_workers: "", shuffle_partitions: "", node_type: "",
  });

  function buildPlanOpts() {
    const opts = {};
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
    if (!file?.name.endsWith(".csv")) { setError("Upload a .csv file."); return; }
    setError(""); setCsvFile(file); setDetecting(true); setDetected(null); setPlan(null);
    try {
      const result = await schemaApi.detect(file);
      setDetected(result);
      // persist schema columns for executor
      try { localStorage.setItem("last_csv_schema", JSON.stringify(result.columns)); } catch {}
    } catch (e) { setError("Could not read CSV: " + e.message); }
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

  function reset() { setCsvFile(null); setDetected(null); setPrompt(""); setPlan(null); setError(""); }

  // ── execution flow (concurrency) editing ──────────────────────────────────
  const cfg = plan?.config;

  // A notebook stage that only adds processed_time does nothing useful.
  const isPassThrough = (s) =>
    s.type === "notebook" &&
    !(s.transformations || []).some((t) => t && !t.includes("processed_time")) &&
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
    setPlan({ ...plan, config: { ...cfg, execution_groups: cleaned } });
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
        <div style={C.cardHdr}><Upload size={16} color="#38bdf8" />Upload CSV</div>
        <div style={C.cardSub}>Drop any CSV — column names and types detected automatically.</div>
        <div
          style={C.drop(dragging, !!csvFile)}
          onClick={() => fileRef.current.click()}
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <input ref={fileRef} type="file" accept=".csv" hidden onChange={(e) => handleFile(e.target.files[0])} />
          <Upload size={32} color={csvFile ? "#22c55e" : dragging ? "#3b82f6" : "#334155"} style={{ marginBottom: 10 }} />
          {detecting ? (
            <div style={{ fontSize: 14, color: "#94a3b8" }}>Detecting schema… <Spinner /></div>
          ) : csvFile ? (
            <div style={{ fontSize: 14, color: "#4ade80", fontWeight: 600 }}>
              <CheckCircle size={14} style={{ verticalAlign: "middle", marginRight: 6 }} />
              {csvFile.name} · {detected?.column_count} columns · {(detected?.row_count ?? detected?.row_count_sample)?.toLocaleString()} rows
              <span style={{ marginLeft: 10, fontSize: 12, color: "#64748b", cursor: "pointer" }}
                onClick={(e) => { e.stopPropagation(); reset(); }}>
                Change
              </span>
            </div>
          ) : (
            <>
              <div style={{ fontSize: 14, color: "#64748b", fontWeight: 600 }}>Click or drag-and-drop your CSV</div>
              <div style={{ fontSize: 12, color: "#475569" }}>Any CSV with a header row</div>
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
                  {detected.preview[0] && Object.keys(detected.preview[0]).slice(0, 3).map((_, i) => (
                    <th key={i} style={C.th}>Sample {i + 1}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Object.entries(detected.columns).map(([col, type]) => (
                  <tr key={col}>
                    <td style={{ ...C.td, fontWeight: 600, color: "#f1f5f9" }}>{col}</td>
                    <td style={C.td}><span style={C.typeBadge(type)}>{type}</span></td>
                    {detected.preview.slice(0, 3).map((row, i) => (
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
          <div style={C.cardHdr}><Brain size={16} color="#a78bfa" />Describe your goal</div>
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
            onKeyDown={(e) => { if (e.key === "Enter" && e.metaKey) handlePlan(); }}
          />

          {!plan && (
            <>
              <div style={{ marginTop: 8, marginBottom: 6, fontSize: 11, color: "#475569" }}>Click an example to use it:</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {EXAMPLE_PROMPTS.map((p, i) => (
                  <button key={i} onClick={() => setPrompt(p)}
                    style={{ fontSize: 11, color: "#64748b", background: "#0f172a", border: "1px solid #334155", borderRadius: 6, padding: "4px 8px", cursor: "pointer", textAlign: "left" }}>
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
            <Settings size={16} color="#f59e0b" />Pipeline Settings
            <span style={{ fontSize: 11, color: "#475569", fontWeight: 400 }}>(optional)</span>
          </div>
          <div style={C.cardSub}>
            Auto uses size-based recommendations. Override to control stage count and cloud resources
            {plan ? " — then re-plan to apply." : " before generating the plan."}
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            <div>
              <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 4 }}>
                Storage Containers (2–10)
                <span style={{ color: "#475569", marginLeft: 6 }}>
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
              <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 4 }}>
                Container Names (comma-separated)
                {containerNamesMismatch && (
                  <span style={{ color: "#f59e0b", marginLeft: 6 }}>
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
                  <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 4 }}>{SETTING_LABELS[key]}</div>
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
          <div style={C.cardHdr}><Brain size={16} color="#a78bfa" />Pipeline Plan — Ready</div>

          <div style={C.successBox}>
            <CheckCircle size={18} color="#4ade80" style={{ flexShrink: 0, marginTop: 1 }} />
            <div>
              <div style={{ fontWeight: 700, color: "#4ade80", marginBottom: 4 }}>
                Plan generated · {plan.config?.stages?.length} stage(s)
                {plan.used_fallback && <span style={{ marginLeft: 8, fontSize: 11, color: "#f59e0b" }}>fallback used</span>}
              </div>
              <div style={{ fontSize: 13, color: "#64748b" }}>{plan.config?.reasoning}</div>
            </div>
          </div>

          {passThroughStages.length > 0 && (
            <div style={{
              background: "#451a03", border: "1px solid #92400e", borderRadius: 8,
              padding: "10px 14px", marginTop: 12, fontSize: 12, color: "#fbbf24",
            }}>
              ⚠ {passThroughStages.map((s) => s.name).join(", ")}{" "}
              {passThroughStages.length > 1 ? "do" : "does"} nothing except copy data forward.
              Reduce Storage Containers in Pipeline Settings, or re-plan with a prompt
              describing what each stage should do.
            </div>
          )}

          <div style={C.stageGrid}>
            {(plan.config?.stages || []).map((s, i) => {
              const transforms = (s.transformations || []).filter((t) => t && !t.includes("processed_time"));
              const srcSink = (s.source_container && s.sink_container)
                ? `${s.source_container} → ${s.sink_container}` : null;
              return (
                <div key={i} style={C.stage}>
                  <div style={C.stageName} title={s.name}>{s.name}</div>
                  <div style={C.stageType(s.type)}>{s.type}</div>
                  {s.type === "copy" ? (
                    <div style={C.stageDetail}>
                      Ingests raw files unchanged via ADF Copy
                      {srcSink ? ` (${srcSink})` : ""} · DIU: {s.diu ?? "auto"}
                    </div>
                  ) : (
                    <>
                      {srcSink && <div style={C.stageDetail}>{srcSink}</div>}
                      {transforms.length > 0 && (
                        <div style={C.stageDetail}>
                          Transforms: {transforms.slice(0, 3).join(", ")}
                          {transforms.length > 3 ? ` (+${transforms.length - 3} more)` : ""}
                        </div>
                      )}
                      {s.filter_condition && <div style={C.stageDetail}>Filter: {s.filter_condition}</div>}
                      {s.aggregation?.aggregations?.length > 0 && (
                        <div style={C.stageDetail}>
                          Group by: {s.aggregation.group_by?.join(", ")} ·{" "}
                          {s.aggregation.aggregations.map((a) => `${a.op}(${a.column})`).join(", ")}
                        </div>
                      )}
                      {isPassThrough(s) && (
                        <div style={{ ...C.stageDetail, color: "#f59e0b" }}>
                          Pass-through — copies data unchanged (adds processed_time only)
                        </div>
                      )}
                    </>
                  )}
                </div>
              );
            })}
          </div>

          {cfg?.recommended_settings && (
            <div style={{ marginTop: 12, fontSize: 12, color: "#64748b" }}>
              Resources: DIU {cfg.recommended_settings.diu} ·{" "}
              workers {cfg.recommended_settings.num_workers} ·{" "}
              shuffle {cfg.recommended_settings.shuffle_partitions} ·{" "}
              {cfg.recommended_settings.node_type}
            </div>
          )}

          {/* Execution flow — user-controlled concurrency */}
          {stageNames.length > 1 && (
            <div style={{ marginTop: 18 }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 }}>
                Execution Flow
              </div>
              <div style={{ fontSize: 11, color: "#64748b", marginBottom: 10 }}>
                Stages in the same group run in parallel; groups run in order.
                Data dependencies are validated and auto-repaired at run time.
              </div>
              {execGroups.map((g, gi) => (
                <div key={gi} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                  <span style={{ fontSize: 11, color: g.length > 1 ? "#a78bfa" : "#64748b", width: 70, flexShrink: 0, fontWeight: 600 }}>
                    Group {gi + 1}{g.length > 1 ? " ⚡" : ""}
                  </span>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {g.map((n) => {
                      const st = cfg.stages.find((s) => s.name === n);
                      const locked = st?.type === "copy";
                      return (
                        <span key={n} style={{
                          display: "inline-flex", alignItems: "center", gap: 6,
                          background: "#0f172a", border: "1px solid #334155",
                          borderRadius: 8, padding: "4px 8px", fontSize: 11, color: "#cbd5e1",
                        }}>
                          {n}
                          <select
                            value={gi}
                            disabled={locked}
                            title={locked ? "Copy stage always runs first" : "Move to another group"}
                            onChange={(e) => setStageGroup(n, Number(e.target.value))}
                            style={{
                              background: "#1e293b", color: locked ? "#475569" : "#94a3b8",
                              border: "1px solid #334155", borderRadius: 6, fontSize: 11,
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
                background: pass ? "#0d2b0d" : "#2d0808",
                border: `1px solid ${pass ? "#166534" : "#7f1d1d"}`,
              }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: pass ? "#4ade80" : "#f87171", marginBottom: failures.length || sem ? 8 : 0 }}>
                  <ShieldCheck size={13} style={{ verticalAlign: "middle", marginRight: 6 }} />
                  {assuranceResult.summary}
                </div>
                {failures.map((c) => (
                  <div key={c.check} style={{ fontSize: 12, color: "#f87171", marginBottom: 4 }}>
                    ✗ {c.label}: {c.message}
                  </div>
                ))}
                {sem && (
                  <div style={{ fontSize: 12, color: sem.available ? (sem.flagged ? "#f59e0b" : "#64748b") : "#475569" }}>
                    Intent ({sem.model || "semantic"}):{" "}
                    {!sem.available ? "unavailable" : sem.flagged ? "FLAGGED (advisory)" : "matches request"} — {sem.reasoning}
                  </div>
                )}
                {(sem?.issues?.length ?? 0) > 0 && (
                  <div style={{ marginTop: 6 }}>
                    {sem.issues.map((it, i) => (
                      <div key={i} style={{ fontSize: 12, color: "#fbbf24", marginBottom: 3 }}>
                        • <span style={{ fontWeight: 600 }}>{it.stage}</span>: {it.problem}
                        {it.suggestion && <span style={{ color: "#94a3b8" }}> — fix: {it.suggestion}</span>}
                      </div>
                    ))}
                  </div>
                )}
                {(failures.length > 0 || sem?.flagged) && (
                  <button
                    style={{ ...C.btnSecondary, marginTop: 10, color: "#fbbf24", borderColor: "#92400e" }}
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
            <button style={{ ...C.btnSecondary, color: "#4ade80", borderColor: "#166534" }} disabled={assuring} onClick={handleValidate}>
              <ShieldCheck size={13} />{assuring ? <><Spinner /> Validating…</> : "Validate Plan"}
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
