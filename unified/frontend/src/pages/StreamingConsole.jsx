import React, { useState, useRef, useEffect, useCallback } from "react";
import { Radio, Upload, Square, RefreshCw } from "lucide-react";
import { stream as streamApi } from "../api.js";
import { Alert, Badge, Button, Card, Spinner } from "../ui/components.jsx";

// The stream id is remembered so leaving the Manager page doesn't orphan a
// stream that is still active on the backend.
const STREAM_KEY = "stream_id";
function savedStreamId() {
  try { return localStorage.getItem(STREAM_KEY); } catch { return null; }
}
function saveStreamId(sid) {
  try { sid ? localStorage.setItem(STREAM_KEY, sid) : localStorage.removeItem(STREAM_KEY); } catch { /* storage unavailable */ }
}

// Live streaming console: start a stream, drop data → it processes incrementally,
// outputs appear as each trigger completes. Shown only for streaming-mode plans.
export default function StreamingConsole({ config, schema, fileFormat = "csv" }) {
  const [streamId, setStreamIdRaw] = useState(savedStreamId);
  const [status,   setStatus]   = useState(null);
  const [rows,     setRows]     = useState([]);
  const [busy,     setBusy]     = useState(false);
  const [error,    setError]    = useState("");
  const [over,     setOver]     = useState(false);
  const fileRef = useRef();

  const setStreamId = useCallback((sid) => { setStreamIdRaw(sid); saveStreamId(sid); }, []);

  const refresh = useCallback(async (sid) => {
    try {
      const s = await streamApi.get(sid);
      setStatus(s);
      const out = await streamApi.output(sid, 200);
      setRows(out.rows || []);
      return s;
    } catch (e) {
      if (e?.status === 404) {
        // Stream gone (server restarted) — back to the start button.
        setStreamId(null); setStatus(null); setRows([]); setBusy(false);
        setError("That stream no longer exists on the server — start a new one.");
      } else {
        setError(e.message);
      }
    }
  }, [setStreamId]);

  // Restore a remembered stream once on mount.
  useEffect(() => { if (streamId) refresh(streamId); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Poll while the stream is active so output updates when a tick finishes;
  // a stopped stream has nothing new to show.
  const polling = !!streamId && (status === null || !!status.active);
  useEffect(() => {
    if (!polling) return;
    const t = setInterval(async () => {
      const s = await refresh(streamId);
      if (s && !s.running) { setBusy(false); }
    }, 5000);
    return () => clearInterval(t);
  }, [polling, streamId, refresh]);

  async function start() {
    setError(""); setBusy(true);
    try {
      const s = await streamApi.start(config, schema || {}, fileFormat, 0);
      setStreamId(s.stream_id);
      setStatus(s);
    } catch (e) { setError("Start failed: " + e.message); }
    finally { setBusy(false); }
  }

  async function onDrop(file) {
    // One trigger at a time: a drop while a tick runs would queue a second one.
    if (!file || !streamId || busy) return;
    if (file.size === 0) { setError("That file is empty — drop a file with data."); return; }
    setError(""); setBusy(true);
    try {
      await streamApi.addData(streamId, file);   // uploads + triggers a tick
      await refresh(streamId);
    } catch (e) { setError("Drop failed: " + e.message); setBusy(false); }
    // busy cleared by the poll when the tick finishes
  }

  async function stop() {
    if (!streamId) return;
    try { await streamApi.stop(streamId); await refresh(streamId); }
    catch (e) { setError(e.message); }
    finally { setBusy(false); }   // polling ends with the stream, so clear it here
  }

  const cols = rows.length ? Object.keys(rows[0]) : [];
  const running = status?.running;
  const state = status?.active ? (running ? "processing" : "live") : "stopped";

  return (
    <Card title="Streaming console" icon={Radio}
      subtitle="Each file you add runs one incremental trigger (~1–2 min on Databricks); the checkpoint skips data already processed"
      actions={streamId ? <Badge tone={state === "live" ? "ok" : state === "processing" ? "warn" : "neutral"} dot>{state}</Badge> : null}>
      {error && <Alert tone="bad" style={{ marginBottom: 12 }}>{error}</Alert>}

      {!streamId ? (
        <Button variant="primary" icon={Radio} loading={busy} onClick={start}>Start streaming</Button>
      ) : (
        <div className="stack" style={{ gap: 12 }}>
          <div className="row muted" style={{ gap: 16, flexWrap: "wrap", fontSize: 12.5 }}>
            <span>Stream <span className="mono" style={{ color: "var(--text)" }}>{streamId}</span></span>
            <span>Triggers <b style={{ color: "var(--text)" }}>{status?.tick_count ?? 0}</b></span>
            <span>Sink <span className="mono" style={{ color: "var(--text)" }}>{status?.sink_container || "—"}</span></span>
          </div>

          <div
            className={`dropzone${over ? " over" : ""}`}
            style={{ padding: "24px 16px", opacity: busy ? 0.6 : 1, cursor: busy ? "default" : "pointer" }}
            onClick={() => !busy && fileRef.current?.click()}
            onDragOver={(e) => { e.preventDefault(); setOver(true); }}
            onDragLeave={() => setOver(false)}
            onDrop={(e) => { e.preventDefault(); setOver(false); onDrop(e.dataTransfer.files[0]); }}
            role="button" tabIndex={0}
          >
            <div className="dropzone-icon" style={{ width: 36, height: 36, marginBottom: 8 }}>
              {busy ? <Spinner size={16} /> : <Upload size={17} strokeWidth={1.8} />}
            </div>
            <div style={{ fontSize: 13.5, color: "var(--text)" }}>
              {busy ? "Processing new data…" : "Drop a file or click to add data to the stream"}
            </div>
            <input ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden
              onChange={(e) => { onDrop(e.target.files[0]); e.target.value = ""; }} />
          </div>

          <div className="row" style={{ gap: 8 }}>
            <Button size="sm" icon={RefreshCw} disabled={busy} onClick={() => refresh(streamId)}>Refresh output</Button>
            {status?.active ? (
              <Button size="sm" variant="danger" icon={Square} onClick={stop}>Stop stream</Button>
            ) : (
              <Button size="sm" icon={Radio} onClick={() => { setStreamId(null); setStatus(null); setRows([]); setError(""); }}>New stream</Button>
            )}
          </div>

          {status?.last_error && <Alert tone="bad" title="Last trigger failed">{status.last_error}</Alert>}

          <div>
            <div className="list-title">Output — {rows.length} row(s) in the sink</div>
            {rows.length > 0 ? (
              <div style={{ overflowX: "auto", border: "1px solid var(--border)", borderRadius: "var(--radius)" }}>
                <table className="preview-table">
                  <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                  <tbody>
                    {rows.slice(0, 50).map((r, i) => (
                      <tr key={i}>{cols.map((c) => <td key={c}>{String(r[c])}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="muted" style={{ fontSize: 13 }}>No output yet — add a file to generate results.</div>
            )}
          </div>
        </div>
      )}
    </Card>
  );
}
