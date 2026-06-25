import React, { useState, useEffect, useRef } from "react";
import { executor, planner } from "../api.js";
import { Play, Upload, CheckCircle, XCircle, Clock, RefreshCw } from "lucide-react";

const S = {
  title:    { fontSize: 22, fontWeight: 700, marginBottom: 4, color: "#f1f5f9" },
  subtitle: { fontSize: 13, color: "#64748b", marginBottom: 24 },
  section:  { marginBottom: 20 },
  label:    { fontSize: 12, color: "#64748b", fontWeight: 600, marginBottom: 6, display: "block" },
  fileZone: {
    border: "2px dashed #334155", borderRadius: 10, padding: "24px 20px",
    textAlign: "center", cursor: "pointer", color: "#475569", fontSize: 13,
    transition: "border-color 0.2s",
  },
  textarea: {
    width: "100%", background: "#1e293b", border: "1px solid #334155",
    color: "#e2e8f0", borderRadius: 8, padding: "10px 12px", fontSize: 12,
    fontFamily: "monospace", resize: "vertical",
  },
  btn: (color = "#22c55e") => ({
    padding: "10px 20px", background: color, color: "#fff",
    border: "none", borderRadius: 8, cursor: "pointer", fontSize: 13,
    fontWeight: 600, display: "inline-flex", alignItems: "center", gap: 8,
    marginRight: 10,
  }),
  card: {
    background: "#1e293b", borderRadius: 10, padding: 16,
    border: "1px solid #334155", marginTop: 20,
  },
  statusRow: { display: "flex", alignItems: "center", gap: 10, fontSize: 14 },
  statusIcon: (s) => ({
    running:   { color: "#f59e0b" },
    completed: { color: "#22c55e" },
    failed:    { color: "#ef4444" },
  }[s] || { color: "#64748b" }),
};

function StatusIcon({ status }) {
  if (status === "running")   return <Clock size={18} style={S.statusIcon("running")} />;
  if (status === "completed") return <CheckCircle size={18} style={S.statusIcon("completed")} />;
  if (status === "failed")    return <XCircle size={18} style={S.statusIcon("failed")} />;
  return null;
}

export default function ExecutorPage() {
  const [csvFile,   setCsvFile]   = useState(null);
  const [configTxt, setConfigTxt] = useState("");
  const [schemaTxt, setSchemaTxt] = useState("");
  const [jobId,     setJobId]     = useState(null);
  const [jobState,  setJobState]  = useState(null);
  const [loading,   setLoading]   = useState(false);
  const [error,     setError]     = useState("");
  const pollRef = useRef(null);
  const fileRef = useRef();

  function handleFileDrop(e) {
    e.preventDefault();
    const f = e.dataTransfer?.files[0] || e.target.files?.[0];
    if (f) setCsvFile(f);
  }

  async function handleRun() {
    if (!csvFile)     return setError("Upload a CSV file.");
    if (!configTxt.trim()) return setError("Paste a pipeline config JSON.");
    if (!schemaTxt.trim()) return setError("Paste a schema JSON.");
    setError(""); setLoading(true);
    try {
      const config = JSON.parse(configTxt);
      const schema = JSON.parse(schemaTxt);
      const res    = await executor.run(csvFile, config, schema);
      setJobId(res.job_id);
      setJobState({ status: "running" });
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!jobId) return;
    pollRef.current = setInterval(async () => {
      try {
        const s = await executor.status(jobId);
        setJobState(s);
        if (s.status !== "running") clearInterval(pollRef.current);
      } catch {}
    }, 3000);
    return () => clearInterval(pollRef.current);
  }, [jobId]);

  return (
    <div>
      <h1 style={S.title}>Pipeline Executor</h1>
      <p style={S.subtitle}>
        Upload a CSV, paste the pipeline config from the Planner, and trigger the ADF + Databricks run.
      </p>

      <div style={S.section}>
        <label style={S.label}>CSV File</label>
        <div
          style={{ ...S.fileZone, borderColor: csvFile ? "#22c55e" : "#334155" }}
          onClick={() => fileRef.current.click()}
          onDragOver={(e) => e.preventDefault()}
          onDrop={handleFileDrop}
        >
          {csvFile ? `✓ ${csvFile.name}` : "Click or drag-and-drop a CSV file here"}
          <input ref={fileRef} type="file" accept=".csv" hidden onChange={handleFileDrop} />
        </div>
      </div>

      <div style={S.section}>
        <label style={S.label}>Pipeline Config JSON (from Planner)</label>
        <textarea
          style={{ ...S.textarea, minHeight: 160 }}
          value={configTxt}
          onChange={(e) => setConfigTxt(e.target.value)}
          placeholder='{"stages": [...], "recommended_settings": {...}}'
          spellCheck={false}
        />
      </div>

      <div style={S.section}>
        <label style={S.label}>Schema JSON</label>
        <textarea
          style={{ ...S.textarea, minHeight: 100 }}
          value={schemaTxt}
          onChange={(e) => setSchemaTxt(e.target.value)}
          placeholder='{"order_id": "integer", "region": "string", ...}'
          spellCheck={false}
        />
      </div>

      {error && (
        <div style={{ color: "#f87171", fontSize: 13, marginBottom: 12 }}>{error}</div>
      )}

      <button style={S.btn()} onClick={handleRun} disabled={loading}>
        <Play size={15} />
        {loading ? "Submitting…" : "Run Pipeline"}
      </button>

      {jobId && jobState && (
        <div style={S.card}>
          <div style={S.statusRow}>
            <StatusIcon status={jobState.status} />
            <span style={{ fontWeight: 600 }}>
              {jobState.status === "running"   && "Pipeline running…"}
              {jobState.status === "completed" && "Pipeline completed!"}
              {jobState.status === "failed"    && "Pipeline failed"}
            </span>
            {jobState.status === "running" && (
              <RefreshCw size={13} style={{ color: "#475569", marginLeft: "auto" }} />
            )}
          </div>
          <div style={{ fontSize: 11, color: "#475569", marginTop: 6 }}>Job: {jobId}</div>

          {jobState.error && (
            <pre style={{
              marginTop: 10, background: "#450a0a", padding: 10, borderRadius: 6,
              fontSize: 11, color: "#f87171", overflow: "auto",
            }}>
              {jobState.error}
            </pre>
          )}

          {jobState.result && (
            <details style={{ marginTop: 12 }}>
              <summary style={{ cursor: "pointer", fontSize: 12, color: "#22c55e" }}>
                View result
              </summary>
              <pre style={{
                marginTop: 8, background: "#0f172a", padding: 12, borderRadius: 8,
                fontSize: 11, color: "#94a3b8", overflow: "auto", maxHeight: 280,
              }}>
                {JSON.stringify(jobState.result, null, 2)}
              </pre>
            </details>
          )}
        </div>
      )}
    </div>
  );
}
