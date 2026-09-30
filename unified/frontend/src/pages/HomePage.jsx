import React, { useEffect, useState, useCallback, useRef } from "react";
import { useNavigate } from "react-router-dom";
import {
  Activity, AlertTriangle, ArrowRight, Brain, CheckCircle2, Clock, GitBranch,
  LayoutGrid, Plus, RefreshCw, XCircle,
} from "lucide-react";
import { monitor, connectWS } from "../api.js";
import { useAppContext, isLive } from "../AppContext.jsx";
import PipelineFlow from "../flows/PipelineFlow.jsx";
import { Alert, Badge, Button, Card, Dot, Empty, PageHeader, Spinner, Stat } from "../ui/components.jsx";

function fmtSec(s) {
  if (!s) return "0s";
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${Math.round(s % 60)}s` : `${Math.round(s)}s`;
}
const fmtMs = (ms) => (ms ? fmtSec(Math.round(ms / 1000)) : "—");
const ADF_TONE = { Succeeded: "ok", Failed: "bad", InProgress: "accent", Queued: "accent", Cancelled: "neutral" };
const SEV_TONE = { high: "bad", medium: "warn", low: "ok" };

export default function HomePage() {
  const navigate = useNavigate();
  const [summary,   setSummary]   = useState(null);
  const [liveRuns,  setLiveRuns]  = useState([]);
  const [wsOk,      setWsOk]      = useState(false);
  const [loading,   setLoading]   = useState(true);
  const [loadError, setLoadError] = useState("");
  const [syncing,   setSyncing]   = useState(false);
  // Shared state — the same plan and runs the other pages show.
  const { planResult: savedPlan, runs: managedRuns, run, csvName } = useAppContext();
  // Refresh the counts when a managed run starts or finishes, instead of
  // waiting for the 30 s timer — so this page agrees with the Manager page.
  const liveKey = managedRuns.filter((r) => isLive(r.status)).map((r) => r.run_id).join(",");

  async function loadSummary() {
    try {
      const [s, live] = await Promise.all([monitor.getSummary(), monitor.getLiveRuns()]);
      setSummary(s);
      setLiveRuns(live);
      setLoadError("");
    } catch (e) {
      setLoadError(`Could not reach the backend: ${e.message}`);
    } finally { setLoading(false); }
  }

  async function syncNow() {
    setSyncing(true);
    setLoadError("");
    try { await monitor.sync(48); await loadSummary(); }
    catch (e) { setLoadError(`Sync failed: ${e.message}`); }
    finally { setSyncing(false); }
  }

  const firstLive = useRef(true);
  useEffect(() => {
    if (firstLive.current) { firstLive.current = false; return; }
    loadSummary();
  }, [liveKey]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    loadSummary();
    const t = setInterval(loadSummary, 30000);   // auto-refresh
    return () => clearInterval(t);
  }, []);

  const onWs = useCallback((data) => {
    setWsOk(true);
    if (data.event === "live_update") setLiveRuns(data.runs || []);
    if (data.event === "run_completed") monitor.getSummary().then(setSummary).catch(() => {});
  }, []);
  useEffect(() => connectWS(onWs), [onWs]);

  // Only "no data" when the backend answered; an unreachable backend is an error, not an empty DB.
  const noData = !loading && !loadError && (!summary || summary.total_runs === 0);
  const activeRun = run && isLive(run.status) ? run : null;
  const cfg = savedPlan?.config;

  return (
    <div>
      <PageHeader
        eyebrow="Workspace" icon={LayoutGrid}
        title="Overview"
        description="What is running, what finished, and what needs attention across ADF and Databricks."
        actions={<>
          <Badge tone={wsOk ? "ok" : "neutral"} dot>{wsOk ? "Live" : "Connecting"}</Badge>
          <Button size="sm" icon={RefreshCw} onClick={loadSummary}>Refresh</Button>
          <Button size="sm" variant="primary" icon={Plus} onClick={() => navigate("/new")}>New pipeline</Button>
        </>}
      />

      {loadError && <Alert tone="bad" style={{ marginBottom: 14 }}>{loadError}</Alert>}
      {noData && (
        <Alert tone="warn" style={{ marginBottom: 14 }}
          action={<Button size="sm" loading={syncing} onClick={syncNow}>Sync last 48h</Button>}>
          No pipeline history yet. Pull recent ADF runs, or run your first pipeline.
        </Alert>
      )}

      {/* The managed run in progress, if any — same state as the Manager page */}
      {activeRun && (
        <Card style={{ marginBottom: 14 }}>
          <div className="row" style={{ gap: 12 }}>
            <Spinner size={16} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ color: "var(--text)", fontWeight: 600 }}>{activeRun.step || "Running…"}</div>
              <div className="muted" style={{ fontSize: 12.5 }}>
                Run <span className="mono">{activeRun.run_id?.slice(0, 8)}</span> · {activeRun.phase?.replace("_", " ")}
              </div>
            </div>
            <Button size="sm" onClick={() => navigate("/manager")}>Open in Manager <ArrowRight size={13} /></Button>
          </div>
        </Card>
      )}

      <div className="grid grid-4" style={{ marginBottom: 14 }}>
        <Stat icon={Activity} label="Active in ADF" value={liveRuns.length} sub="pipelines running now" />
        <Stat icon={CheckCircle2} label="Succeeded" value={summary?.succeeded ?? "—"} tone="ok"
          sub={`of ${summary?.total_runs ?? 0} ADF runs`} />
        <Stat icon={XCircle} label="Failed" value={summary?.failed ?? "—"}
          tone={(summary?.failed ?? 0) > 0 ? "bad" : undefined} sub="pipeline failures" />
        <Stat icon={AlertTriangle} label="Anomalies" value={summary?.anomaly_count ?? "—"}
          tone={(summary?.anomaly_count ?? 0) > 0 ? "warn" : undefined} sub="flagged by the monitor" />
      </div>

      <div className="grid grid-2" style={{ marginBottom: 14 }}>
        <Card title="Needs attention" icon={AlertTriangle}
          actions={<button className="link" onClick={() => navigate("/monitor")}>Monitor →</button>}>
          <div className="list-title">Running now</div>
          {liveRuns.length === 0 ? (
            <div className="faint" style={{ fontSize: 13, padding: "6px 0" }}>Nothing running in ADF.</div>
          ) : (
            <div className="list">
              {liveRuns.map((r) => (
                <div key={r.runId} className="list-row">
                  <Dot tone="accent" live />
                  <span className="grow">{r.pipelineName}</span>
                  {r.anomaly && <AlertTriangle size={13} style={{ color: "var(--warn)" }} />}
                  <span className="meta"><Clock size={11} style={{ verticalAlign: -1 }} /> {fmtSec(r.elapsedSec)}</span>
                </div>
              ))}
            </div>
          )}

          {(summary?.recent_anomalies?.length ?? 0) > 0 && (
            <>
              <div className="list-title">Anomalies</div>
              <div className="list">
                {summary.recent_anomalies.map((a) => (
                  <div key={a.id} className="list-row" style={{ alignItems: "flex-start" }}>
                    <AlertTriangle size={13} style={{ color: "var(--warn)", marginTop: 3, flexShrink: 0 }} />
                    <div style={{ minWidth: 0 }}>
                      <div style={{ color: "var(--text)", fontWeight: 500 }}>{a.pipeline_name}</div>
                      <div className="muted" style={{ fontSize: 12.5 }}>
                        {(a.groq_verdict || "").slice(0, 110)}{(a.groq_verdict || "").length > 110 ? "…" : ""}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}

          {(summary?.recent_failed?.length ?? 0) > 0 && (
            <>
              <div className="list-title">Recent failures</div>
              <div className="list">
                {summary.recent_failed.map((r) => (
                  <div key={r.run_id} className="list-row">
                    <XCircle size={13} style={{ color: "var(--bad)", flexShrink: 0 }} />
                    <span className="grow">{r.pipeline_name}</span>
                    <span className="meta">{fmtMs(r.duration_ms)}</span>
                    {r.severity && <Badge tone={SEV_TONE[r.severity] || "neutral"}>{r.severity}</Badge>}
                  </div>
                ))}
              </div>
            </>
          )}
        </Card>

        <Card title="Recent ADF runs" icon={Clock}
          actions={<button className="link" onClick={() => navigate("/monitor")}>All logs →</button>}>
          {(summary?.recent_runs?.length ?? 0) === 0 ? (
            <Empty icon={Clock} title="No run history">Sync ADF runs or run a pipeline.</Empty>
          ) : (
            <div className="list">
              {summary.recent_runs.map((r) => (
                <div key={r.run_id} className="list-row">
                  <span className="grow">{r.pipeline_name}</span>
                  <span className="meta">{fmtMs(r.duration_ms)}</span>
                  <Badge tone={ADF_TONE[r.status] || "neutral"}>{r.status}</Badge>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      <Card title="Current plan" icon={Brain}
        subtitle={cfg ? `${cfg.stages?.length || 0} stage(s)${cfg.mode === "streaming" ? " · streaming" : ""}${csvName ? ` · for ${csvName}` : ""}` : undefined}
        actions={cfg ? <>
          <Button size="sm" onClick={() => navigate("/planner")}>Edit in Planner</Button>
          <Button size="sm" variant="primary" icon={GitBranch} onClick={() => navigate("/manager")}>Run in Manager</Button>
        </> : null}>
        {cfg ? (
          <>
            <PipelineFlow plan={cfg} inputLabel={csvName} height={230} />
            {savedPlan.used_fallback && <div className="muted" style={{ fontSize: 12.5, marginTop: 8 }}>Designed with the fallback planner.</div>}
          </>
        ) : (
          <Empty icon={Brain} title="No plan yet"
            action={<Button variant="primary" icon={Plus} onClick={() => navigate("/new")}>Start a new pipeline</Button>}>
            Upload data and describe what you want — the planner designs the pipeline.
          </Empty>
        )}
      </Card>
    </div>
  );
}
