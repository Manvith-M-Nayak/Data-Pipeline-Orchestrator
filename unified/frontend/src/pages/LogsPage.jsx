import React, { useEffect, useState, useCallback } from "react";
import { monitor, connectWS } from "../api.js";
import { ChevronDown, ChevronRight, FileText } from "lucide-react";

const SEV = { low: "var(--ok)", medium: "var(--warn)", high: "var(--bad)" };

const S = {
  title:  { fontSize: 22, fontWeight: 700, marginBottom: 24, color: "var(--text)" },
  filters:{ display: "flex", gap: 10, marginBottom: 20, flexWrap: "wrap" },
  ctrl:   { background: "var(--surface)", border: "1px solid var(--border)", color: "var(--text)", borderRadius: 8, padding: "8px 12px", fontSize: 13 },
  table:  { width: "100%", borderCollapse: "collapse" },
  th:     { textAlign: "left", padding: "10px 12px", fontSize: 11, color: "var(--text-3)", borderBottom: "1px solid var(--border)", textTransform: "uppercase", letterSpacing: 0.5 },
  td:     { padding: "11px 12px", fontSize: 13, borderBottom: "1px solid var(--divider)", verticalAlign: "top" },
  expand: { background: "var(--surface-2)", padding: 16, borderRadius: 8, marginTop: 6, fontSize: 13, lineHeight: 1.7 },
  lbl:    { color: "var(--text-3)", fontWeight: 600, fontSize: 11, textTransform: "uppercase", marginBottom: 4, letterSpacing: 0.5 },
};

const badge = (s) => ({
  display: "inline-block", padding: "2px 8px", borderRadius: 12, fontSize: 11, fontWeight: 700,
  background: s === "Succeeded" ? "var(--ok-soft)" : s === "Failed" ? "var(--bad-soft)" : "var(--surface)",
  color:      s === "Succeeded" ? "var(--ok)" : s === "Failed" ? "var(--bad)" : "var(--text-2)",
});

const sevBadge = (s) => ({
  display: "inline-block", padding: "2px 8px", borderRadius: 12, fontSize: 11, fontWeight: 700,
  color: SEV[s] || "var(--text-2)", background: "var(--surface-2)", border: `1px solid ${SEV[s] || "var(--border-strong)"}`,
});

function parseJ(v) { try { return v ? JSON.parse(v) : []; } catch { return [v]; } }
function fmtMs(ms) { if (!ms) return "—"; const s = Math.round(ms/1000); return s < 60 ? `${s}s` : `${Math.floor(s/60)}m ${s%60}s`; }

function Row({ run }) {
  const [open, setOpen] = useState(false);
  const anomalies = parseJ(run.anomalies);
  const insights  = parseJ(run.performance_insights);
  const suggestions = parseJ(run.suggestions);

  return (
    <>
      <tr style={{ cursor: "pointer" }} onClick={() => setOpen((o) => !o)}>
        <td style={S.td}>{open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}</td>
        <td style={S.td}>{run.pipeline_name}</td>
        <td style={S.td}><span style={badge(run.status)}>{run.status}</span></td>
        <td style={S.td}>{fmtMs(run.duration_ms)}</td>
        <td style={S.td}>{run.severity ? <span style={sevBadge(run.severity)}>{run.severity}</span> : "—"}</td>
        <td style={{ ...S.td, color: "var(--text-4)", fontSize: 11 }}>{run.run_id?.slice(0, 8)}…</td>
      </tr>
      {open && (
        <tr>
          <td colSpan={6} style={{ padding: "0 12px 12px", background: "var(--surface)" }}>
            <div style={S.expand}>
              {run.status_summary && <><div style={S.lbl}>Summary</div><p style={{ marginBottom: 12 }}>{run.status_summary}</p></>}
              {run.explanation    && <><div style={S.lbl}>Why it took this long</div><p style={{ marginBottom: 12 }}>{run.explanation}</p></>}
              {run.root_cause     && <><div style={S.lbl}>Root Cause</div><p style={{ marginBottom: 12 }}>{run.root_cause}</p></>}
              {anomalies.length > 0 && <><div style={S.lbl}>Anomalies</div><ul style={{ paddingLeft: 18, marginBottom: 12 }}>{anomalies.map((a, i) => <li key={i}>{a}</li>)}</ul></>}
              {insights.length   > 0 && <><div style={S.lbl}>Insights</div><ul style={{ paddingLeft: 18, marginBottom: 12 }}>{insights.map((a, i) => <li key={i}>{a}</li>)}</ul></>}
              {suggestions.length> 0 && <><div style={S.lbl}>Suggestions</div><ul style={{ paddingLeft: 18 }}>{suggestions.map((a, i) => <li key={i}>{a}</li>)}</ul></>}
              {!run.status_summary && <span style={{ color: "var(--text-4)" }}>No AI analysis yet.</span>}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export default function LogsPage() {
  const [logs,    setLogs]    = useState([]);
  const [f,       setF]       = useState({ status: "", pipeline_name: "" });
  const [loading, setLoading] = useState(false);
  const [newRuns, setNewRuns] = useState(0); // banner counter for live completions
  const [error,   setError]   = useState("");

  async function load(filters = f) {
    setLoading(true);
    setNewRuns(0);
    setError("");
    try {
      const params = {};
      if (filters.status)        params.status        = filters.status;
      if (filters.pipeline_name) params.pipeline_name = filters.pipeline_name;
      setLogs(await monitor.getLogs(params));
    } catch (e) {
      setError(`Could not load logs: ${e.message}`);
    } finally { setLoading(false); }
  }

  useEffect(() => { load(); }, []); // eslint-disable-line react-hooks/exhaustive-deps -- initial load only

  // Auto-refresh when monitor agent finishes analyzing a run
  const onWs = useCallback((data) => {
    if (data.event === "run_completed") {
      setNewRuns((n) => n + 1);
    }
  }, []);
  useEffect(() => connectWS(onWs), [onWs]);

  return (
    <div>
      <h1 style={S.title}>Run Logs</h1>
      {error && <div style={{ color: "var(--bad)", fontSize: 13, marginBottom: 12 }}>{error}</div>}

      {newRuns > 0 && (
        <div style={{ background: "var(--accent-soft)", border: "1px solid var(--accent-line)", borderRadius: 8, padding: "9px 14px", marginBottom: 14, fontSize: 13, color: "var(--accent)", display: "flex", alignItems: "center", gap: 10 }}>
          {newRuns} new run{newRuns > 1 ? "s" : ""} completed.
          <button onClick={() => load()} style={{ background: "none", border: "none", color: "var(--accent)", cursor: "pointer", fontWeight: 700, fontSize: 13, textDecoration: "underline" }}>
            Refresh now
          </button>
        </div>
      )}

      <div style={S.filters}>
        <input style={S.ctrl} placeholder="Pipeline name…" value={f.pipeline_name}
          onChange={(e) => setF((p) => ({ ...p, pipeline_name: e.target.value }))}
          onKeyDown={(e) => e.key === "Enter" && load(f)} />
        <select style={S.ctrl} value={f.status} onChange={(e) => setF((p) => ({ ...p, status: e.target.value }))}>
          <option value="">All statuses</option>
          <option value="Succeeded">Succeeded</option>
          <option value="Failed">Failed</option>
          <option value="InProgress">InProgress</option>
        </select>
        <button style={{ ...S.ctrl, background: "var(--accent)", border: "none", cursor: "pointer" }} onClick={() => load(f)}>Search</button>
      </div>
      {loading ? (
        <div style={{ color: "var(--text-4)", textAlign: "center", marginTop: 60 }}>Loading logs…</div>
      ) : logs.length === 0 ? (
        <div style={{ color: "var(--text-4)", textAlign: "center", marginTop: 60 }}>
          <FileText size={40} style={{ marginBottom: 12, color: "var(--text-4)" }} />
          <p>No runs found.</p>
          <p style={{ fontSize: 12, color: "var(--text-4)", marginTop: 8 }}>
            Run a pipeline first, or click "Sync (48h)" in the header to pull recent ADF runs.
          </p>
        </div>
      ) : (
        <table style={S.table}>
          <thead><tr>
            <th style={S.th} /><th style={S.th}>Pipeline</th><th style={S.th}>Status</th>
            <th style={S.th}>Duration</th><th style={S.th}>Severity</th><th style={S.th}>Run ID</th>
          </tr></thead>
          <tbody>{logs.map((r) => <Row key={r.run_id} run={r} />)}</tbody>
        </table>
      )}
    </div>
  );
}
