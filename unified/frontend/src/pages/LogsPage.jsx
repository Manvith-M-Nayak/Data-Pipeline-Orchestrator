import React, { useEffect, useState, useCallback } from "react";
import { ChevronDown, ChevronRight, FileText, Search } from "lucide-react";
import { monitor, connectWS } from "../api.js";
import { Alert, Badge, Button, Card, Empty, Spinner } from "../ui/components.jsx";

const STATUS_TONE = { Succeeded: "ok", Failed: "bad", InProgress: "accent" };
const SEV_TONE = { low: "ok", medium: "warn", high: "bad" };

function parseJ(v) { try { return v ? JSON.parse(v) : []; } catch { return [v]; } }
function fmtMs(ms) { if (!ms) return "—"; const s = Math.round(ms / 1000); return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`; }

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div className="list-title" style={{ marginTop: 0 }}>{title}</div>
      <div style={{ color: "var(--text-2)", fontSize: 13, lineHeight: 1.6 }}>{children}</div>
    </div>
  );
}

function Row({ run }) {
  const [open, setOpen] = useState(false);
  const anomalies   = parseJ(run.anomalies);
  const insights    = parseJ(run.performance_insights);
  const suggestions = parseJ(run.suggestions);

  return (
    <>
      <tr className="clickable" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
        <td style={{ width: 28, color: "var(--text-4)" }}>{open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}</td>
        <td style={{ color: "var(--text)" }}>{run.pipeline_name}</td>
        <td><Badge tone={STATUS_TONE[run.status] || "neutral"}>{run.status}</Badge></td>
        <td>{fmtMs(run.duration_ms)}</td>
        <td>{run.severity ? <Badge tone={SEV_TONE[run.severity] || "neutral"}>{run.severity}</Badge> : <span className="faint">—</span>}</td>
        <td className="mono faint">{run.run_id?.slice(0, 8)}</td>
      </tr>
      {open && (
        <tr>
          <td colSpan={6} style={{ background: "var(--surface-2)", padding: "16px 20px" }}>
            {run.status_summary && <Section title="Summary">{run.status_summary}</Section>}
            {run.explanation    && <Section title="Why it took this long">{run.explanation}</Section>}
            {run.root_cause     && <Section title="Root cause">{run.root_cause}</Section>}
            {anomalies.length > 0 && <Section title="Anomalies"><ul style={{ paddingLeft: 18 }}>{anomalies.map((a, i) => <li key={i}>{a}</li>)}</ul></Section>}
            {insights.length > 0 && <Section title="Insights"><ul style={{ paddingLeft: 18 }}>{insights.map((a, i) => <li key={i}>{a}</li>)}</ul></Section>}
            {suggestions.length > 0 && <Section title="Suggestions"><ul style={{ paddingLeft: 18 }}>{suggestions.map((a, i) => <li key={i}>{a}</li>)}</ul></Section>}
            {!run.status_summary && <span className="faint">No AI analysis for this run yet.</span>}
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

  // Count runs the monitor finishes analysing while this page is open.
  const onWs = useCallback((data) => {
    if (data.event === "run_completed") setNewRuns((n) => n + 1);
  }, []);
  useEffect(() => connectWS(onWs), [onWs]);

  return (
    <div className="stack" style={{ gap: 14 }}>
      {error && <Alert tone="bad">{error}</Alert>}
      {newRuns > 0 && (
        <Alert tone="accent" action={<Button size="sm" onClick={() => load()}>Refresh</Button>}>
          {newRuns} new run{newRuns > 1 ? "s" : ""} finished since this list loaded.
        </Alert>
      )}

      <form className="row" style={{ gap: 8, flexWrap: "wrap" }} onSubmit={(e) => { e.preventDefault(); load(f); }}>
        <input className="input" style={{ maxWidth: 280 }} placeholder="Pipeline name" value={f.pipeline_name}
          onChange={(e) => setF((p) => ({ ...p, pipeline_name: e.target.value }))} />
        <select className="input" style={{ maxWidth: 170 }} value={f.status}
          onChange={(e) => setF((p) => ({ ...p, status: e.target.value }))}>
          <option value="">All statuses</option>
          <option value="Succeeded">Succeeded</option>
          <option value="Failed">Failed</option>
          <option value="InProgress">In progress</option>
        </select>
        <Button type="submit" variant="primary" icon={Search}>Search</Button>
      </form>

      <Card pad={false}>
        {loading ? (
          <div className="row muted" style={{ gap: 8, padding: 24 }}><Spinner /> Loading runs…</div>
        ) : logs.length === 0 ? (
          <Empty icon={FileText} title="No runs found">
            Run a pipeline, or use “Sync ADF runs” in the top bar to pull the last 48 hours.
          </Empty>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table className="table">
              <thead><tr>
                <th /><th>Pipeline</th><th>Status</th><th>Duration</th><th>Severity</th><th>Run</th>
              </tr></thead>
              <tbody>{logs.map((r) => <Row key={r.run_id} run={r} />)}</tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
