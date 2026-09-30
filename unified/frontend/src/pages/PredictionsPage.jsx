import React, { useEffect, useState } from "react";
import { monitor } from "../api.js";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from "recharts";
import { TrendingUp } from "lucide-react";

const CONF = { low: "var(--text-3)", medium: "var(--warn)", high: "var(--ok)" };

const S = {
  title:  { fontSize: 22, fontWeight: 700, marginBottom: 24, color: "var(--text)" },
  row:    { display: "flex", gap: 10, marginBottom: 20 },
  select: { background: "var(--surface)", border: "1px solid var(--border)", color: "var(--text)", borderRadius: 8, padding: "8px 12px", fontSize: 13, flex: 1 },
  btn:    { background: "var(--accent)", border: "none", color: "var(--accent-fg)", borderRadius: 8, padding: "8px 16px", cursor: "pointer", fontSize: 13 },
  card:   { background: "var(--surface)", borderRadius: 12, padding: 24, border: "1px solid var(--border)", marginBottom: 16 },
  val:    { fontSize: 28, fontWeight: 700, color: "var(--text)" },
  sub:    { fontSize: 13, color: "var(--text-2)", marginTop: 4 },
  grid:   { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))", gap: 12, marginTop: 16 },
  stat:   { background: "var(--surface-2)", borderRadius: 8, padding: 12, textAlign: "center" },
  statV:  { fontSize: 20, fontWeight: 700, color: "var(--accent)", marginBottom: 4 },
  statL:  { fontSize: 12, color: "var(--text-3)" },
  empty:  { color: "var(--text-4)", textAlign: "center", marginTop: 60 },
};

function fmt(s) { if (s == null) return "—"; const m = Math.floor(s/60); return m > 0 ? `${m}m ${Math.round(s%60)}s` : `${Math.round(s)}s`; }

export default function PredictionsPage() {
  const [names,   setNames]   = useState([]);
  const [chosen,  setChosen]  = useState("");
  const [result,  setResult]  = useState(null);
  const [loading, setLoading] = useState(false);

  const [syncing, setSyncing] = useState(false);
  const [error,   setError]   = useState("");

  async function syncAndReload() {
    setSyncing(true);
    setError("");
    try {
      await monitor.sync(48);
      const n = await monitor.getNames();
      setNames(n);
      if (n.length) { setChosen(n[0]); setResult(null); }
    } catch (e) {
      setError(`Sync failed: ${e.message}`);
    } finally {
      setSyncing(false);
    }
  }

  useEffect(() => {
    monitor.getNames().then((n) => {
      setNames(n);
      if (n.length) {
        setChosen(n[0]);
      } else {
        // DB empty — auto-sync ADF history so predictions have data
        syncAndReload();
      }
    }).catch((e) => setError(`Could not load pipelines: ${e.message}`));
  }, []);

  async function load() {
    if (!chosen) return;
    setLoading(true);
    setError("");
    try { setResult(await monitor.getPrediction(chosen)); }
    catch (e) { setError(`Prediction failed: ${e.message}`); }
    finally { setLoading(false); }
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
      {error && <div style={{ color: "var(--bad)", fontSize: 13, marginBottom: 12 }}>{error}</div>}
      <div style={S.row}>
        <select style={S.select} value={chosen} onChange={(e) => { setChosen(e.target.value); setResult(null); }}>
          {names.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
        <button style={S.btn} onClick={load} disabled={loading}>{loading ? "…" : "Predict"}</button>
      </div>

      {names.length === 0 && (
        <div style={S.empty}>
          <TrendingUp size={40} style={{ marginBottom: 12, color: "var(--text-4)" }} />
          {syncing ? (
            <p style={{ color: "var(--accent)" }}>Syncing ADF history…</p>
          ) : (
            <>
              <p>No pipelines found yet.</p>
              <p style={{ fontSize: 12, color: "var(--text-3)", marginTop: 8 }}>
                Run a pipeline first, or{" "}
                <button onClick={syncAndReload} style={{ background: "none", border: "none", color: "var(--accent)", cursor: "pointer", fontSize: 12, textDecoration: "underline" }}>
                  sync history now
                </button>.
              </p>
            </>
          )}
        </div>
      )}

      {names.length > 0 && !result && !loading && (
        <div style={S.empty}><TrendingUp size={40} style={{ marginBottom: 12, color: "var(--text-4)" }} /><p>Select a pipeline and click Predict.</p></div>
      )}

      {result && p && (
        <>
          <div style={S.card}>
            <div style={S.val}>{fmt(p.predicted_duration_sec)}</div>
            <div style={S.sub}>
              Range: {fmt(p.range_min_sec)} – {fmt(p.range_max_sec)} ·{" "}
              <span style={{ color: CONF[p.confidence] || "var(--text-2)", fontWeight: 600 }}>{p.confidence} confidence</span>
            </div>
            {p.reasoning && <div style={{ marginTop: 10, fontSize: 13, color: "var(--text-3)" }}>{p.reasoning}</div>}
            <div style={{ height: 160, marginTop: 20 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <XAxis dataKey="name" tick={{ fill: "var(--text-3)", fontSize: 12 }} />
                  <YAxis tick={{ fill: "var(--text-3)", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip formatter={(v) => [fmt(v), "Duration"]} contentStyle={{ background: "var(--surface-2)", border: "1px solid var(--border)" }} labelStyle={{ color: "var(--text-2)" }} />
                  <Bar dataKey="value" fill="var(--accent)" radius={[4, 4, 0, 0]} />
                  <ReferenceLine y={p.predicted_duration_sec} stroke="var(--warn)" strokeDasharray="4 4" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          {s && (
            <div style={{ ...S.card, padding: 20 }}>
              <div style={{ fontSize: 12, color: "var(--text-3)", marginBottom: 10 }}>Historical stats ({s.count} successful runs)</div>
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
