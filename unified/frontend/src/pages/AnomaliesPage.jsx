import React, { useEffect, useState } from "react";
import { AlertTriangle } from "lucide-react";
import { monitor } from "../api.js";
import { Alert, Badge, Card, Empty, Segmented, Spinner } from "../ui/components.jsx";

const KINDS = ["failure", "timeout", "retry_storm", "slow_runtime", "cold_start",
               "zero_rows", "cost_spike", "schema_drift"];
const SEV_TONE  = { high: "bad", medium: "warn", low: "neutral" };
const SEV_COLOR = { high: "var(--bad)", medium: "var(--warn)", low: "var(--text-3)" };
const label = (k) => (k || "").replaceAll("_", " ");

function fmtSec(s) { if (!s) return "0s"; const m = Math.floor(s / 60); return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`; }

function fmtMetrics(json) {
  let m;
  try { m = JSON.parse(json || "{}"); } catch { return ""; }
  return Object.entries(m)
    .map(([k, v]) => `${label(k)}: ${typeof v === "number" ? +v.toFixed(4) : Array.isArray(v) ? v.join(", ") || "—" : v}`)
    .join(" · ");
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
    <div className="stack" style={{ gap: 14 }}>
      {error && <Alert tone="bad">{error}</Alert>}

      <div style={{ overflowX: "auto" }}>
        <Segmented value={kind} onChange={setKind}
          options={[{ value: "", label: "All" }, ...KINDS.map((k) => ({ value: k, label: label(k) }))]} />
      </div>

      <Card title="Detected anomalies" subtitle="One event per anomaly kind per run" pad={false}>
        {loading ? (
          <div className="row muted" style={{ gap: 8, padding: 24 }}><Spinner /> Loading…</div>
        ) : events.length === 0 ? (
          <Empty icon={AlertTriangle} title={`No ${kind ? label(kind) + " " : ""}anomalies`}>
            The detector has not flagged anything{kind ? " of this kind" : ""} yet.
          </Empty>
        ) : (
          <div style={{ padding: "4px 20px" }}>
            <div className="list">
              {events.map((e) => (
                <div key={e.id} className="list-row" style={{ alignItems: "flex-start", padding: "14px 0" }}>
                  <AlertTriangle size={15} style={{ marginTop: 2, flexShrink: 0, color: SEV_COLOR[e.severity] || "var(--text-3)" }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div className="row" style={{ gap: 8, marginBottom: 2 }}>
                      <span style={{ color: "var(--text)", fontWeight: 600, textTransform: "capitalize" }}>{label(e.kind)}</span>
                      <Badge tone={SEV_TONE[e.severity] || "neutral"}>{e.severity}</Badge>
                    </div>
                    <div style={{ color: "var(--text-2)" }}>{e.detail}</div>
                    <div className="faint" style={{ fontSize: 12, marginTop: 4 }}>
                      {e.detected_at} UTC · {e.pipeline_name} · run <span className="mono">{e.run_id?.slice(0, 12)}</span>
                    </div>
                    {fmtMetrics(e.metrics_json) && (
                      <div className="mono muted" style={{ fontSize: 11.5, marginTop: 4 }}>{fmtMetrics(e.metrics_json)}</div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </Card>

      {!kind && legacy.length > 0 && (
        <Card title="Monitor AI verdicts" subtitle="Runs the monitor's model judged unusual" pad={false}>
          <div style={{ padding: "4px 20px" }}>
            <div className="list">
              {legacy.map((a) => (
                <div key={a.id} className="list-row" style={{ alignItems: "flex-start", padding: "14px 0" }}>
                  <AlertTriangle size={15} style={{ marginTop: 2, flexShrink: 0, color: "var(--orange)" }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ color: "var(--text)", fontWeight: 600 }}>{a.pipeline_name}</div>
                    <div style={{ color: "var(--text-2)" }}>{a.groq_verdict}</div>
                    <div className="faint" style={{ fontSize: 12, marginTop: 4 }}>
                      {a.logged_at} · run <span className="mono">{a.run_id?.slice(0, 8)}</span> ·
                      took {fmtSec(a.elapsed_sec)} (usual {fmtSec(a.avg_sec)}, p95 {fmtSec(a.p95_sec)})
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}
    </div>
  );
}
