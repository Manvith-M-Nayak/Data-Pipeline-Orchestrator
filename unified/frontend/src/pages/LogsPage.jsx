import React, { useEffect, useState } from "react";
import { monitor } from "../api.js";
import { ChevronDown, ChevronRight } from "lucide-react";

const SEV = { low: "#22c55e", medium: "#f59e0b", high: "#ef4444" };

const S = {
  title:  { fontSize: 22, fontWeight: 700, marginBottom: 24, color: "#f1f5f9" },
  filters:{ display: "flex", gap: 10, marginBottom: 20, flexWrap: "wrap" },
  ctrl:   { background: "#1e293b", border: "1px solid #334155", color: "#e2e8f0", borderRadius: 8, padding: "8px 12px", fontSize: 13 },
  table:  { width: "100%", borderCollapse: "collapse" },
  th:     { textAlign: "left", padding: "10px 12px", fontSize: 11, color: "#64748b", borderBottom: "1px solid #334155", textTransform: "uppercase", letterSpacing: 0.5 },
  td:     { padding: "11px 12px", fontSize: 13, borderBottom: "1px solid #1e293b", verticalAlign: "top" },
  expand: { background: "#0f172a", padding: 16, borderRadius: 8, marginTop: 6, fontSize: 13, lineHeight: 1.7 },
  lbl:    { color: "#64748b", fontWeight: 600, fontSize: 11, textTransform: "uppercase", marginBottom: 4, letterSpacing: 0.5 },
};

const badge = (s) => ({
  display: "inline-block", padding: "2px 8px", borderRadius: 12, fontSize: 11, fontWeight: 700,
  background: s === "Succeeded" ? "#14532d" : s === "Failed" ? "#7f1d1d" : "#1e293b",
  color:      s === "Succeeded" ? "#4ade80" : s === "Failed" ? "#f87171" : "#94a3b8",
});

const sevBadge = (s) => ({
  display: "inline-block", padding: "2px 8px", borderRadius: 12, fontSize: 11, fontWeight: 700,
  color: SEV[s] || "#94a3b8", background: "#0f172a", border: `1px solid ${SEV[s] || "#334155"}`,
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
        <td style={{ ...S.td, color: "#334155", fontSize: 11 }}>{run.run_id?.slice(0, 8)}…</td>
      </tr>
      {open && (
        <tr>
          <td colSpan={6} style={{ padding: "0 12px 12px", background: "#1e293b" }}>
            <div style={S.expand}>
              {run.status_summary && <><div style={S.lbl}>Summary</div><p style={{ marginBottom: 12 }}>{run.status_summary}</p></>}
              {run.explanation    && <><div style={S.lbl}>Why it took this long</div><p style={{ marginBottom: 12 }}>{run.explanation}</p></>}
              {run.root_cause     && <><div style={S.lbl}>Root Cause</div><p style={{ marginBottom: 12 }}>{run.root_cause}</p></>}
              {anomalies.length > 0 && <><div style={S.lbl}>Anomalies</div><ul style={{ paddingLeft: 18, marginBottom: 12 }}>{anomalies.map((a, i) => <li key={i}>{a}</li>)}</ul></>}
              {insights.length   > 0 && <><div style={S.lbl}>Insights</div><ul style={{ paddingLeft: 18, marginBottom: 12 }}>{insights.map((a, i) => <li key={i}>{a}</li>)}</ul></>}
              {suggestions.length> 0 && <><div style={S.lbl}>Suggestions</div><ul style={{ paddingLeft: 18 }}>{suggestions.map((a, i) => <li key={i}>{a}</li>)}</ul></>}
              {!run.status_summary && <span style={{ color: "#475569" }}>No AI analysis yet.</span>}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export default function LogsPage() {
  const [logs,   setLogs]   = useState([]);
  const [f,      setF]      = useState({ status: "", pipeline_name: "" });
  const [loading,setLoading]= useState(false);

  async function load() {
    setLoading(true);
    try {
      const params = {};
      if (f.status)        params.status        = f.status;
      if (f.pipeline_name) params.pipeline_name = f.pipeline_name;
      setLogs(await monitor.getLogs(params));
    } finally { setLoading(false); }
  }

  useEffect(() => { load(); }, []);

  return (
    <div>
      <h1 style={S.title}>Run Logs</h1>
      <div style={S.filters}>
        <input style={S.ctrl} placeholder="Pipeline name…" value={f.pipeline_name}
          onChange={(e) => setF((p) => ({ ...p, pipeline_name: e.target.value }))}
          onKeyDown={(e) => e.key === "Enter" && load()} />
        <select style={S.ctrl} value={f.status} onChange={(e) => setF((p) => ({ ...p, status: e.target.value }))}>
          <option value="">All statuses</option>
          <option value="Succeeded">Succeeded</option>
          <option value="Failed">Failed</option>
          <option value="InProgress">InProgress</option>
        </select>
        <button style={{ ...S.ctrl, background: "#0ea5e9", border: "none", cursor: "pointer" }} onClick={load}>Search</button>
      </div>
      {loading ? <div style={{ color: "#475569" }}>Loading…</div> : (
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
