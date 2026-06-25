import React, { useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { schema as schemaApi, planner } from "../api.js";
import { useAppContext } from "../AppContext.jsx";
import {
  Upload, Brain, CheckCircle, XCircle, Zap, RotateCcw, ArrowRight,
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
  stageName: { fontWeight: 700, fontSize: 13, color: "#f1f5f9", marginBottom: 5 },
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

  async function handlePlan() {
    if (!prompt.trim() || !detected) return;
    setError(""); setPlanning(true); setPlan(null);
    try {
      const result = await planner.plan(
        { columns: detected.columns, row_count: detected.row_count_sample, size_hint: "auto-detected", preview: detected.preview },
        prompt,
      );
      setPlan(result);
    } catch (e) { setError("Planner failed: " + e.message); }
    finally { setPlanning(false); }
  }

  function reset() { setCsvFile(null); setDetected(null); setPrompt(""); setPlan(null); setError(""); }

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
              {csvFile.name} · {detected?.column_count} columns · {detected?.row_count_sample} rows
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

      {/* Prompt */}
      {detected && !plan && (
        <div style={C.card}>
          <div style={C.cardHdr}><Brain size={16} color="#a78bfa" />Describe your goal</div>
          <div style={C.cardSub}>Plain English — no technical knowledge needed.</div>

          <textarea
            style={{ ...C.textarea, minHeight: 80 }}
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            placeholder="e.g. Filter active users, group by region, calculate average order value."
            onKeyDown={(e) => { if (e.key === "Enter" && e.metaKey) handlePlan(); }}
          />

          <div style={{ marginTop: 8, marginBottom: 6, fontSize: 11, color: "#475569" }}>Click an example to use it:</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {EXAMPLE_PROMPTS.map((p, i) => (
              <button key={i} onClick={() => setPrompt(p)}
                style={{ fontSize: 11, color: "#64748b", background: "#0f172a", border: "1px solid #334155", borderRadius: 6, padding: "4px 8px", cursor: "pointer", textAlign: "left" }}>
                {p.slice(0, 55)}…
              </button>
            ))}
          </div>

          <div style={C.btnRow}>
            <button style={C.btnPrimary(!prompt.trim() || planning)} disabled={!prompt.trim() || planning} onClick={handlePlan}>
              <Brain size={13} />{planning ? <><Spinner /> Planning…</> : "Generate Pipeline Plan"}
            </button>
          </div>
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

          <div style={C.stageGrid}>
            {(plan.config?.stages || []).map((s, i) => (
              <div key={i} style={C.stage}>
                <div style={C.stageName}>{s.name}</div>
                <div style={C.stageType(s.type)}>{s.type}</div>
                {s.transforms?.length > 0 && <div style={C.stageDetail}>Transforms: {s.transforms.slice(0, 2).join(", ")}</div>}
                {s.filter_condition && <div style={C.stageDetail}>Filter: {s.filter_condition}</div>}
                {s.aggregation?.aggregations?.length > 0 && (
                  <div style={C.stageDetail}>
                    Group by: {s.aggregation.group_by?.join(", ")} ·{" "}
                    {s.aggregation.aggregations.map((a) => `${a.op}(${a.column})`).join(", ")}
                  </div>
                )}
              </div>
            ))}
          </div>

          <div style={C.btnRow}>
            <button style={C.btnPrimary(false)} onClick={() => navigate("/executor")}>
              <Zap size={13} /> Send to Executor <ArrowRight size={13} />
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
