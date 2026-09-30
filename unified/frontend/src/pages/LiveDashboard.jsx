import React, { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { AlertTriangle, CheckCircle2, Clock, Zap } from "lucide-react";
import { connectWS, monitor } from "../api.js";
import { useAppContext, isLive } from "../AppContext.jsx";
import { Alert, Badge, Button, Card, Dot, Empty } from "../ui/components.jsx";

function fmtSec(s) {
  if (!s) return "0s";
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`;
}
const SEV_TONE = { high: "bad", medium: "warn", low: "ok" };

export default function LiveDashboard() {
  const navigate = useNavigate();
  const [runs,       setRuns]       = useState([]);
  const [ts,         setTs]         = useState(null);
  const [completed,  setCompleted]  = useState([]);
  const [cancelling, setCancelling] = useState({});
  const [confirming, setConfirming] = useState(null);   // runId awaiting a second click
  const [error,      setError]      = useState("");

  async function handleCancel(runId) {
    // Cancelling an ADF run cannot be undone — require a second click.
    if (confirming !== runId) { setConfirming(runId); return; }
    setConfirming(null);
    setCancelling((p) => ({ ...p, [runId]: true }));
    setError("");
    try {
      await monitor.cancelRun(runId);
      setRuns((prev) => prev.filter((r) => r.runId !== runId));
      monitor.sync(1).catch(() => {});
    } catch (e) {
      setError("Cancel failed: " + e.message);
    } finally {
      setCancelling((p) => ({ ...p, [runId]: false }));
    }
  }

  const onWs = useCallback((data) => {
    if (data.event === "live_update") {
      setRuns(data.runs || []);
      setTs(new Date().toLocaleTimeString());
    }
    if (data.event === "run_completed") {
      setCompleted((prev) => [data, ...prev].slice(0, 5));
    }
  }, []);

  // In-progress managed runs — from the shared run list (AppContext), so this
  // view agrees with the Central Manager and Executor pages.
  const { runs: managedRuns } = useAppContext();
  const execJobs = managedRuns.filter((r) => isLive(r.status));

  useEffect(() => {
    monitor.getLiveRuns().then(setRuns).catch(() => {});
    return connectWS(onWs);
  }, [onWs]);

  const nothing = runs.length === 0 && execJobs.length === 0;

  return (
    <div className="stack" style={{ gap: 14 }}>
      <div className="row" style={{ gap: 8, fontSize: 12.5 }}>
        <Dot tone={ts ? "ok" : "neutral"} live={!!ts} />
        <span className="muted">{ts ? `Last ADF update ${ts}` : "Waiting for the first ADF update…"}</span>
      </div>

      {error && <Alert tone="bad">{error}</Alert>}

      {completed.length > 0 && (
        <Card title="Just finished" icon={CheckCircle2}>
          <div className="list">
            {completed.map((c) => (
              <div key={c.runId} className="list-row">
                <CheckCircle2 size={14} style={{ color: "var(--ok)", flexShrink: 0 }} />
                <span style={{ color: "var(--text)", fontWeight: 500, flexShrink: 0 }}>{c.pipelineName}</span>
                <span className="grow muted" style={{ color: "var(--text-3)" }}>{c.summary}</span>
                {c.severity && <Badge tone={SEV_TONE[c.severity] || "neutral"}>{c.severity}</Badge>}
              </div>
            ))}
          </div>
        </Card>
      )}

      {execJobs.length > 0 && (
        <Card title="Managed runs in progress" icon={Zap}
          actions={<button className="link" onClick={() => navigate("/manager")}>Open Manager →</button>}>
          <div className="list">
            {execJobs.map((j) => (
              <div key={j.run_id} className="list-row">
                <Dot tone="warn" live />
                <span className="mono" style={{ color: "var(--text)", flexShrink: 0 }}>{j.run_id.slice(0, 8)}</span>
                <span className="grow" style={{ color: "var(--text-2)" }}>{j.step || "Running…"}</span>
                <Badge tone="accent">{j.status?.replace("_", " ")}</Badge>
              </div>
            ))}
          </div>
        </Card>
      )}

      {nothing ? (
        <Card><Empty icon={CheckCircle2} title="Nothing running">No ADF pipelines or managed runs are in progress.</Empty></Card>
      ) : runs.length > 0 && (
        <div className="grid grid-3">
          {runs.map((r) => (
            <div key={r.runId} className="card card-pad">
              <div className="row" style={{ gap: 8, marginBottom: 10 }}>
                <span style={{ color: "var(--text)", fontWeight: 600, flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }}>
                  {r.pipelineName}
                </span>
                <Badge tone="accent" dot>{r.status}</Badge>
              </div>
              <div className="row muted" style={{ gap: 6, fontSize: 12.5 }}>
                <Clock size={13} /> Running {fmtSec(r.elapsedSec)}
                <span className="faint mono" style={{ marginLeft: "auto" }}>{r.runId?.slice(0, 8)}</span>
              </div>
              {r.anomaly && (
                <Alert tone="warn" style={{ marginTop: 10, padding: "8px 10px", fontSize: 12.5 }}>{r.anomaly}</Alert>
              )}
              <div style={{ marginTop: 12 }}>
                <Button size="sm" variant="danger" loading={cancelling[r.runId]}
                  onClick={() => handleCancel(r.runId)} onBlur={() => confirming === r.runId && setConfirming(null)}>
                  {confirming === r.runId ? "Click again to cancel" : "Cancel run"}
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}

      {runs.length === 0 && execJobs.length > 0 && (
        <div className="row faint" style={{ gap: 6, fontSize: 12.5 }}>
          <AlertTriangle size={13} /> No ADF pipelines running — managed runs appear in ADF once the copy step starts.
        </div>
      )}
    </div>
  );
}
