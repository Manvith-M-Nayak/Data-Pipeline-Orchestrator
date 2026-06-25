import React, { useEffect, useState } from "react";
import { monitor } from "../api.js";
import { AlertTriangle } from "lucide-react";

const S = {
  title: { fontSize: 22, fontWeight: 700, marginBottom: 24, color: "#f1f5f9" },
  card:  { background: "#1e293b", borderRadius: 10, padding: 16, border: "1px solid #334155", marginBottom: 10, display: "flex", gap: 14 },
  name:  { fontWeight: 700, fontSize: 14, marginBottom: 4 },
  meta:  { fontSize: 12, color: "#64748b", marginBottom: 6 },
  verdict: { fontSize: 13, color: "#cbd5e1", lineHeight: 1.5 },
  stats:   { fontSize: 12, color: "#475569", marginTop: 6 },
  empty:   { color: "#475569", textAlign: "center", marginTop: 60 },
};

function fmtSec(s) { if (!s) return "0s"; const m = Math.floor(s/60); return m > 0 ? `${m}m ${Math.round(s%60)}s` : `${Math.round(s)}s`; }

export default function AnomaliesPage() {
  const [items,   setItems]   = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    monitor.getAnomalies().then(setItems).finally(() => setLoading(false));
  }, []);

  if (loading) return <div style={{ color: "#475569" }}>Loading…</div>;

  return (
    <div>
      <h1 style={S.title}>Anomaly Log</h1>
      {items.length === 0 ? (
        <div style={S.empty}><AlertTriangle size={40} style={{ marginBottom: 12, color: "#334155" }} /><p>No anomalies detected yet.</p></div>
      ) : items.map((a) => (
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
    </div>
  );
}
