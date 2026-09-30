import React, { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine, Cell } from "recharts";
import { TrendingUp } from "lucide-react";
import { monitor } from "../api.js";
import { Alert, Badge, Button, Card, Empty, Spinner, Stat } from "../ui/components.jsx";

const CONF_TONE = { low: "neutral", medium: "warn", high: "ok" };

function fmt(s) { if (s == null) return "—"; const m = Math.floor(s / 60); return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`; }

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
      if (n.length) setChosen(n[0]);
      else syncAndReload();     // DB empty — pull ADF history so predictions have data
    }).catch((e) => setError(`Could not load pipelines: ${e.message}`));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

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
    { name: "Fastest likely", value: p.range_min_sec },
    { name: "Predicted",      value: p.predicted_duration_sec },
    { name: "Slowest likely", value: p.range_max_sec },
  ] : [];

  return (
    <div className="stack" style={{ gap: 14 }}>
      {error && <Alert tone="bad">{error}</Alert>}

      <div className="row" style={{ gap: 8 }}>
        <select className="input" style={{ maxWidth: 360 }} value={chosen} disabled={!names.length}
          onChange={(e) => { setChosen(e.target.value); setResult(null); }}>
          {names.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
        <Button variant="primary" icon={TrendingUp} loading={loading} disabled={!chosen} onClick={load}>Predict runtime</Button>
      </div>

      {names.length === 0 ? (
        <Card>
          {syncing ? (
            <div className="row muted" style={{ gap: 8 }}><Spinner /> Pulling ADF history…</div>
          ) : (
            <Empty icon={TrendingUp} title="No pipelines yet"
              action={<Button onClick={syncAndReload}>Sync history now</Button>}>
              Predictions need past runs. Run a pipeline, or pull the last 48 hours from ADF.
            </Empty>
          )}
        </Card>
      ) : !result && !loading ? (
        <Card><Empty icon={TrendingUp} title="Pick a pipeline">Choose one and press “Predict runtime”.</Empty></Card>
      ) : result && p && (
        <>
          <div className="grid grid-3">
            <Stat label="Predicted runtime" value={fmt(p.predicted_duration_sec)}
              sub={`likely ${fmt(p.range_min_sec)} – ${fmt(p.range_max_sec)}`} />
            <Stat label="Confidence" value={<Badge tone={CONF_TONE[p.confidence] || "neutral"} style={{ fontSize: 14, height: 28 }}>{p.confidence}</Badge>}
              sub={p.source ? `source: ${p.source}` : undefined} />
            <Stat label="History" value={s?.count ?? 0} sub="successful runs used" />
          </div>

          <Card title="Predicted range" subtitle={p.reasoning}>
            <div style={{ height: 190 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <XAxis dataKey="name" tick={{ fill: "var(--text-3)", fontSize: 12 }} axisLine={{ stroke: "var(--border)" }} tickLine={false} />
                  <YAxis tick={{ fill: "var(--text-3)", fontSize: 11 }} tickFormatter={fmt} axisLine={false} tickLine={false} width={56} />
                  <Tooltip
                    formatter={(v) => [fmt(v), "Duration"]}
                    cursor={{ fill: "var(--surface-hover)" }}
                    contentStyle={{ background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 8, color: "var(--text)" }}
                    labelStyle={{ color: "var(--text-2)" }}
                  />
                  <Bar dataKey="value" radius={[5, 5, 0, 0]} maxBarSize={72}>
                    {chartData.map((d, i) => <Cell key={i} fill={i === 1 ? "var(--accent)" : "var(--accent-line)"} />)}
                  </Bar>
                  <ReferenceLine y={p.predicted_duration_sec} stroke="var(--warn)" strokeDasharray="4 4" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Card>

          {s && (
            <Card title={`Past runs of ${chosen}`} subtitle={`${s.count} successful run(s)`}>
              <div className="grid grid-4">
                {[["Average", s.avg], ["Fastest", s.min], ["Slowest", s.max], ["95th percentile", s.p95]].map(([l, v]) => (
                  <div key={l}>
                    <div className="stat-label">{l}</div>
                    <div className="stat-value" style={{ fontSize: 20 }}>{fmt(v)}</div>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
