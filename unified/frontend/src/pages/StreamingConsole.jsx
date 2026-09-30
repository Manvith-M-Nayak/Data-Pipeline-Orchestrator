import React, { useState, useRef, useEffect, useCallback } from "react";
import { Radio, Upload, Square, RefreshCw } from "lucide-react";
import { stream as streamApi } from "../api.js";

// The stream id is remembered so leaving the Manager tab doesn't orphan a
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

  return (
    <div style={S.card}>
      <div style={S.hdr}>
        <Radio size={16} color="var(--accent)" />
        Streaming Console
        <span style={S.sub}>availableNow · drop data → processed incrementally</span>
      </div>

      {!streamId ? (
        <button style={S.primary(busy)} disabled={busy} onClick={start}>
          <Radio size={13} /> {busy ? "Starting…" : "Start Streaming"}
        </button>
      ) : (
        <>
          <div style={S.metaRow}>
            <Pill label="stream" value={streamId} />
            <Pill label="state" value={status?.active ? (running ? "processing…" : "live") : "stopped"}
                  color={status?.active ? (running ? "var(--warn)" : "var(--ok)") : "var(--text-3)"} />
            <Pill label="triggers" value={status?.tick_count ?? 0} />
            <Pill label="sink" value={status?.sink_container} />
          </div>

          <div
            onClick={() => !busy && fileRef.current?.click()}
            onDragOver={(e) => e.preventDefault()}
            onDrop={(e) => { e.preventDefault(); onDrop(e.dataTransfer.files[0]); }}
            style={S.drop(busy)}
          >
            <Upload size={20} color="var(--text-4)" />
            <div style={{ marginTop: 6, fontSize: 13, color: "var(--text-2)" }}>
              {busy ? "Processing new data…" : "Drop or click to add data to the stream"}
            </div>
            <div style={{ fontSize: 11, color: "var(--text-3)" }}>
              each drop runs one incremental trigger (~1–2 min on Databricks)
            </div>
            <input
              ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden
              onChange={(e) => { onDrop(e.target.files[0]); e.target.value = ""; }}
            />
          </div>

          <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
            <button style={S.ghost} disabled={busy} onClick={() => refresh(streamId)}>
              <RefreshCw size={12} /> Refresh output
            </button>
            {status?.active ? (
              <button style={S.ghost} onClick={stop}><Square size={12} /> Stop stream</button>
            ) : (
              <button style={S.ghost} onClick={() => { setStreamId(null); setStatus(null); setRows([]); setError(""); }}>
                <Radio size={12} /> New stream
              </button>
            )}
          </div>

          {status?.last_error && (
            <div style={S.err}>Last tick error: {status.last_error}</div>
          )}

          <div style={{ marginTop: 12, fontSize: 12, color: "var(--text-2)" }}>
            Output — {rows.length} row(s) in sink
          </div>
          {rows.length > 0 ? (
            <div style={{ overflowX: "auto", marginTop: 6 }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr>{cols.map((c) => <th key={c} style={S.th}>{c}</th>)}</tr>
                </thead>
                <tbody>
                  {rows.slice(0, 50).map((r, i) => (
                    <tr key={i}>{cols.map((c) => <td key={c} style={S.td}>{String(r[c])}</td>)}</tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: "var(--text-3)", marginTop: 6 }}>
              No output yet — drop a file to generate results.
            </div>
          )}
        </>
      )}

      {error && <div style={S.err}>{error}</div>}
    </div>
  );
}

function Pill({ label, value, color = "var(--text-2)" }) {
  return (
    <div style={{ fontSize: 11, color: "var(--text-3)" }}>
      {label}: <span style={{ color, fontWeight: 600 }}>{String(value)}</span>
    </div>
  );
}

const S = {
  card: { background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 12, padding: 20, marginTop: 16 },
  hdr:  { display: "flex", alignItems: "center", gap: 8, fontSize: 15, fontWeight: 700, color: "var(--text)", marginBottom: 12 },
  sub:  { fontSize: 11, color: "var(--text-4)", fontWeight: 400 },
  metaRow: { display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 12 },
  primary: (d) => ({ display: "flex", alignItems: "center", gap: 7, padding: "10px 16px", fontSize: 13, fontWeight: 700,
    background: d ? "var(--accent-soft)" : "var(--accent)", color: "var(--accent-fg)", border: "none", borderRadius: 8, cursor: d ? "default" : "pointer" }),
  drop: (d) => ({ border: "1px dashed var(--border-strong)", borderRadius: 10, padding: 24, textAlign: "center",
    cursor: d ? "default" : "pointer", background: "var(--surface-2)", opacity: d ? 0.6 : 1 }),
  ghost: { display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", fontSize: 12, background: "transparent",
    color: "var(--text-2)", border: "1px solid var(--border)", borderRadius: 8, cursor: "pointer" },
  th: { textAlign: "left", padding: "6px 10px", borderBottom: "1px solid var(--border)", color: "var(--text-3)", whiteSpace: "nowrap" },
  td: { padding: "5px 10px", borderBottom: "1px solid var(--divider)", color: "var(--text-2)", whiteSpace: "nowrap" },
  err: { marginTop: 10, fontSize: 12, color: "var(--bad)", background: "var(--bad-soft)", padding: "8px 10px", borderRadius: 8 },
};
