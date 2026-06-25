import React, { useState } from "react";
import { planner } from "../api.js";
import { Brain, AlertCircle, CheckCircle } from "lucide-react";

const S = {
  title:    { fontSize: 22, fontWeight: 700, marginBottom: 4, color: "#f1f5f9" },
  subtitle: { fontSize: 13, color: "#64748b", marginBottom: 24 },
  label:    { fontSize: 12, color: "#64748b", fontWeight: 600, marginBottom: 6, display: "block" },
  textarea: {
    width: "100%", background: "#1e293b", border: "1px solid #334155",
    color: "#e2e8f0", borderRadius: 8, padding: "10px 12px", fontSize: 13,
    resize: "vertical", fontFamily: "monospace",
  },
  input: {
    width: "100%", background: "#1e293b", border: "1px solid #334155",
    color: "#e2e8f0", borderRadius: 8, padding: "10px 12px", fontSize: 13,
  },
  btn: {
    marginTop: 16, padding: "10px 24px", background: "#6366f1",
    color: "#fff", border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 14, fontWeight: 600, display: "flex", alignItems: "center", gap: 8,
  },
  card: {
    background: "#1e293b", borderRadius: 12, padding: 20,
    border: "1px solid #334155", marginTop: 24,
  },
  badge: (ok) => ({
    display: "inline-block", padding: "2px 10px", borderRadius: 20,
    fontSize: 11, fontWeight: 700,
    background: ok ? "#14532d" : "#7f1d1d", color: ok ? "#4ade80" : "#f87171",
    marginBottom: 12,
  }),
  stageGrid: {
    display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
    gap: 10, marginTop: 10,
  },
  stage: {
    background: "#0f172a", borderRadius: 8, padding: 12,
    border: "1px solid #334155", fontSize: 13,
  },
  error: {
    marginTop: 16, padding: 14, background: "#450a0a", borderRadius: 8,
    color: "#f87171", fontSize: 13, display: "flex", gap: 8,
  },
  row: { marginBottom: 16 },
};

const DEFAULT_SCHEMA = JSON.stringify({
  columns: { order_id: "integer", region: "string", amount: "double", status: "string" },
  size_hint: "small (< 5MB)",
  row_count: 4200,
}, null, 2);

export default function PlannerPage() {
  const [schemaText, setSchemaText] = useState(DEFAULT_SCHEMA);
  const [prompt,     setPrompt]     = useState("Filter by status='completed', compute avg amount by region.");
  const [loading,    setLoading]    = useState(false);
  const [result,     setResult]     = useState(null);
  const [error,      setError]      = useState("");

  async function handlePlan() {
    setError(""); setResult(null); setLoading(true);
    try {
      const schema = JSON.parse(schemaText);
      const res    = await planner.plan(schema, prompt);
      setResult(res);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  const cfg    = result?.config;
  const stages = cfg?.stages || [];

  return (
    <div>
      <h1 style={S.title}>Pipeline Planner</h1>
      <p style={S.subtitle}>Describe your data and transformation goal — AI generates the ADF + Databricks pipeline config.</p>

      <div style={S.row}>
        <label style={S.label}>Schema (JSON)</label>
        <textarea
          style={{ ...S.textarea, minHeight: 140 }}
          value={schemaText}
          onChange={(e) => setSchemaText(e.target.value)}
          spellCheck={false}
        />
      </div>

      <div style={S.row}>
        <label style={S.label}>Transformation prompt</label>
        <input
          style={S.input}
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="What should the pipeline do?"
        />
      </div>

      <button style={S.btn} onClick={handlePlan} disabled={loading}>
        <Brain size={16} />
        {loading ? "Planning…" : "Generate Pipeline Config"}
      </button>

      {error && (
        <div style={S.error}>
          <AlertCircle size={16} style={{ flexShrink: 0, marginTop: 1 }} />
          {error}
        </div>
      )}

      {result && cfg && (
        <div style={S.card}>
          <span style={S.badge(!result.used_fallback)}>
            {result.used_fallback ? "Fallback config" : "AI-generated"}
          </span>

          <div style={{ fontSize: 13, color: "#94a3b8", marginBottom: 12 }}>
            {cfg.reasoning}
          </div>

          <div style={{ fontSize: 12, color: "#64748b", marginBottom: 8 }}>
            {stages.length} stage(s) · Cluster: {cfg.recommended_settings?.cluster_type || "—"} ·
            Nodes: {cfg.recommended_settings?.num_workers || "—"}
          </div>

          <div style={S.stageGrid}>
            {stages.map((s, i) => (
              <div key={i} style={S.stage}>
                <div style={{ fontWeight: 700, marginBottom: 4, color: "#f1f5f9" }}>{s.name}</div>
                <div style={{ color: "#64748b", fontSize: 12, marginBottom: 4 }}>
                  Type: <span style={{ color: "#38bdf8" }}>{s.type}</span>
                </div>
                {s.transforms?.length > 0 && (
                  <div style={{ fontSize: 11, color: "#475569" }}>
                    Transforms: {s.transforms.join(", ")}
                  </div>
                )}
                {s.filter && (
                  <div style={{ fontSize: 11, color: "#475569" }}>Filter: {s.filter}</div>
                )}
              </div>
            ))}
          </div>

          <details style={{ marginTop: 16 }}>
            <summary style={{ cursor: "pointer", fontSize: 12, color: "#475569" }}>
              Raw JSON config
            </summary>
            <pre style={{
              marginTop: 8, background: "#0f172a", padding: 12, borderRadius: 8,
              fontSize: 11, color: "#94a3b8", overflow: "auto", maxHeight: 300,
            }}>
              {JSON.stringify(cfg, null, 2)}
            </pre>
          </details>
        </div>
      )}
    </div>
  );
}
