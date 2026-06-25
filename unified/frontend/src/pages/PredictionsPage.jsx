import React, { useEffect, useState } from "react";
import { monitor } from "../api.js";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from "recharts";
import { TrendingUp } from "lucide-react";

const CONF = { low: "#64748b", medium: "#f59e0b", high: "#22c55e" };

const S = {
  title:  { fontSize: 22, fontWeight: 700, marginBottom: 24, color: "#f1f5f9" },
  row:    { display: "flex", gap: 10, marginBottom: 20 },
  select: { background: "#1e293b", border: "1px solid #334155", color: "#e2e8f0", borderRadius: 8, padding: "8px 12px", fontSize: 13, flex: 1 },
  btn:    { background: "#0ea5e9", border: "none", color: "#fff", borderRadius: 8, padding: "8px 16px", cursor: "pointer", fontSize: 13 },
  card:   { background: "#1e293b", borderRadius: 12, padding: 24, border: "1px solid #334155", marginBottom: 16 },
  val:    { fontSize: 28, fontWeight: 700, color: "#f1f5f9" },
  sub:    { fontSize: 13, color: "#94a3b8", marginTop: 4 },
  grid:   { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))", gap: 12, marginTop: 16 },
  stat:   { background: "#0f172a", borderRadius: 8, padding: 12, textAlign: "center" },
  statV:  { fontSize: 20, fontWeight: 700, color: "#38bdf8", marginBottom: 4 },
  statL:  { fontSize: 12, color: "#64748b" },
  empty:  { color: "#475569", textAlign: "center", marginTop: 60 },
};

function fmt(s) { if (s == null) return "—"; const m = Math.floor(s/60); return m > 0 ? `${m}m ${Math.round(s%60)}s` : `${Math.round(s)}s`; }

export default function PredictionsPage() {
  const [names,   setNames]   = useState([]);
  const [chosen,  setChosen]  = useState("");
  const [result,  setResult]  = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    monitor.getNames().then((n) => { setNames(n); if (n.length) setChosen(n[0]); });
  }, []);

  async function load() {
    if (!chosen) return;
    setLoading(true);
    try { setResult(await monitor.getPrediction(chosen)); } finally { setLoading(false); }
  }

  const p = result?.prediction;
  const s = result?.stats;
  const chartData = p ? [
    { name: "Min", value: p.range_min_sec },
    { name: "Predicted", value: p.predicted_duration_sec },
    { name: "Max", value: p.range_max_sec },
  ] : [];

  return (
    <div>
      <h1 style={S.title}>Runtime Predictions</h1>
      <div style={S.row}>
        <select style={S.select} value={chosen} onChange={(e) => setChosen(e.target.value)}>
          {names.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
        <button style={S.btn} onClick={load} disabled={loading}>{loading ? "…" : "Predict"}</button>
      </div>

      {!result && !loading && (
        <div style={S.empty}><TrendingUp size={40} style={{ marginBottom: 12, color: "#334155" }} /><p>Select a pipeline and click Predict.</p></div>
      )}

      {result && p && (
        <>
          <div style={S.card}>
            <div style={S.val}>{fmt(p.predicted_duration_sec)}</div>
            <div style={S.sub}>
              Range: {fmt(p.range_min_sec)} – {fmt(p.range_max_sec)} ·{" "}
              <span style={{ color: CONF[p.confidence] || "#94a3b8", fontWeight: 600 }}>{p.confidence} confidence</span>
            </div>
            {p.reasoning && <div style={{ marginTop: 10, fontSize: 13, color: "#64748b" }}>{p.reasoning}</div>}
            <div style={{ height: 160, marginTop: 20 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <XAxis dataKey="name" tick={{ fill: "#64748b", fontSize: 12 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip formatter={(v) => [fmt(v), "Duration"]} contentStyle={{ background: "#0f172a", border: "1px solid #334155" }} labelStyle={{ color: "#94a3b8" }} />
                  <Bar dataKey="value" fill="#38bdf8" radius={[4, 4, 0, 0]} />
                  <ReferenceLine y={p.predicted_duration_sec} stroke="#f59e0b" strokeDasharray="4 4" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          {s && (
            <div style={{ ...S.card, padding: 20 }}>
              <div style={{ fontSize: 12, color: "#64748b", marginBottom: 10 }}>Historical stats ({s.count} successful runs)</div>
              <div style={S.grid}>
                {[["Avg", s.avg], ["Min", s.min], ["Max", s.max], ["p95", s.p95]].map(([l, v]) => (
                  <div key={l} style={S.stat}><div style={S.statV}>{fmt(v)}</div><div style={S.statL}>{l}</div></div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
