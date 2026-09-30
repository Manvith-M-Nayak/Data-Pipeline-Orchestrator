import React, { useEffect, useState } from "react";
import { monitor } from "../api.js";
import { AlertTriangle } from "lucide-react";

const S = {
  title: { fontSize: 22, fontWeight: 700, marginBottom: 16, color: "#f1f5f9" },
  section: { fontSize: 15, fontWeight: 700, color: "#cbd5e1", margin: "28px 0 12px" },
  card:  { background: "#1e293b", borderRadius: 10, padding: 16, border: "1px solid #334155", marginBottom: 10, display: "flex", gap: 14 },
  name:  { fontWeight: 700, fontSize: 14, marginBottom: 4 },
  meta:  { fontSize: 12, color: "#64748b", marginBottom: 6 },
  verdict: { fontSize: 13, color: "#cbd5e1", lineHeight: 1.5 },
  stats:   { fontSize: 12, color: "#475569", marginTop: 6 },
  empty:   { color: "#475569", textAlign: "center", margin: "40px 0" },
  error:   { color: "#f87171", fontSize: 13, marginBottom: 12 },
  chips:   { display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 14 },
  badge:   { fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 999, textTransform: "uppercase" },
};

const KINDS = ["failure", "timeout", "retry_storm", "slow_runtime", "cold_start",
               "zero_rows", "cost_spike", "schema_drift"];
const SEVERITY_COLOR = { high: "#ef4444", medium: "#f97316", low: "#eab308" };

function fmtSec(s) { if (!s) return "0s"; const m = Math.floor(s/60); return m > 0 ? `${m}m ${Math.round(s%60)}s` : `${Math.round(s)}s`; }

function fmtMetrics(json) {
  let m;
  try { m = JSON.parse(json || "{}"); } catch { return ""; }
  return Object.entries(m)
    .map(([k, v]) => `${k}: ${typeof v === "number" ? +v.toFixed(4) : Array.isArray(v) ? v.join(", ") || "—" : v}`)
    .join(" · ");
}

function Chip({ active, onClick, children }) {
  return (
    <button onClick={onClick} style={{
      fontSize: 12, padding: "4px 10px", borderRadius: 999, cursor: "pointer",
      border: `1px solid ${active ? "#0ea5e9" : "#334155"}`,
      background: active ? "#0c4a6e" : "transparent", color: active ? "#e0f2fe" : "#94a3b8",
    }}>{children}</button>
  );
}

export default function AnomaliesPage() {
  const [events,  setEvents]  = useState([]);
  const [legacy,  setLegacy]  = useState([]);
  const [kind,    setKind]    = useState("");
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError("");
    Promise.all([monitor.getAnomalyEvents(kind), monitor.getAnomalies()])
      .then(([ev, lg]) => { if (!cancelled) { setEvents(ev); setLegacy(lg); } })
      .catch((e) => { if (!cancelled) setError(`Could not load anomalies: ${e.message}`); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [kind]);

  return (
    <div>
      <h1 style={S.title}>Anomalies</h1>
      {error && <div style={S.error}>{error}</div>}

      <div style={S.chips}>
        <Chip active={!kind} onClick={() => setKind("")}>All</Chip>
        {KINDS.map((k) => <Chip key={k} active={kind === k} onClick={() => setKind(k)}>{k.replace("_", " ")}</Chip>)}
      </div>

      {loading ? <div style={{ color: "#475569" }}>Loading…</div> : events.length === 0 ? (
        <div style={S.empty}><AlertTriangle size={40} style={{ marginBottom: 12, color: "#334155" }} /><p>No {kind ? kind.replace("_", " ") + " " : ""}anomalies detected yet.</p></div>
      ) : events.map((e) => {
        const color = SEVERITY_COLOR[e.severity] || "#94a3b8";
        return (
          <div key={e.id} style={S.card}>
            <AlertTriangle size={18} style={{ color, flexShrink: 0, marginTop: 2 }} />
            <div style={{ flex: 1 }}>
              <div style={{ ...S.name, display: "flex", gap: 8, alignItems: "center" }}>
                {e.kind.replace("_", " ")}
                <span style={{ ...S.badge, background: `${color}22`, color }}>{e.severity}</span>
              </div>
              <div style={S.meta}>{e.detected_at} UTC · {e.pipeline_name} · Run {e.run_id?.slice(0, 12)}</div>
              <div style={S.verdict}>{e.detail}</div>
              <div style={S.stats}>{fmtMetrics(e.metrics_json)}</div>
            </div>
          </div>
        );
      })}

      {!kind && legacy.length > 0 && (
        <>
          <h2 style={S.section}>Monitor AI verdicts</h2>
          {legacy.map((a) => (
            <div key={a.id} style={S.card}>
              <AlertTriangle size={18} style={{ color: "#f97316", flexShrink: 0, marginTop: 2 }} />
              <div style={{ flex: 1 }}>
                <div style={S.name}>{a.pipeline_name}</div>
                <div style={S.meta}>{a.logged_at} · Run {a.run_id?.slice(0, 8)}…</div>
                <div style={S.verdict}>{a.groq_verdict}</div>
                <div style={S.stats}>Elapsed: {fmtSec(a.elapsed_sec)} · Avg: {fmtSec(a.avg_sec)} · p95: {fmtSec(a.p95_sec)}</div>
              </div>
            </div>
          ))}
        </>
      )}
    </div>
  );
}
