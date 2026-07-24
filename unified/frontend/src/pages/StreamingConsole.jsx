import React, { useState, useRef, useEffect, useCallback } from "react";
import { Radio, Upload, Square, RefreshCw } from "lucide-react";
import { stream as streamApi } from "../api.js";

// Live streaming console: start a stream, drop data → it processes incrementally,
// outputs appear as each trigger completes. Shown only for streaming-mode plans.
export default function StreamingConsole({ config, schema, fileFormat = "csv" }) {
  const [streamId, setStreamId] = useState(null);
  const [status,   setStatus]   = useState(null);
  const [rows,     setRows]     = useState([]);
  const [busy,     setBusy]     = useState(false);
  const [error,    setError]    = useState("");
  const fileRef = useRef();
  const pollRef = useRef();

  const refresh = useCallback(async (sid) => {
    try {
      const s = await streamApi.get(sid);
      setStatus(s);
      const out = await streamApi.output(sid, 200);
      setRows(out.rows || []);
      return s;
    } catch (e) { setError(e.message); }
  }, []);

  // Poll while a tick is running so output updates when it finishes.
  useEffect(() => {
    if (!streamId) return;
    clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      const s = await refresh(streamId);
      if (s && !s.running) { setBusy(false); }
    }, 5000);
    return () => clearInterval(pollRef.current);
  }, [streamId, refresh]);

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
    if (!file || !streamId) return;
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
  }

  const cols = rows.length ? Object.keys(rows[0]) : [];
  const running = status?.running;

  return (
    <div style={S.card}>
      <div style={S.hdr}>
        <Radio size={16} color="#38bdf8" />
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
                  color={status?.active ? (running ? "#f59e0b" : "#34d399") : "#64748b"} />
            <Pill label="triggers" value={status?.tick_count ?? 0} />
            <Pill label="sink" value={status?.sink_container} />
          </div>

          <div
            onClick={() => fileRef.current?.click()}
            onDragOver={(e) => e.preventDefault()}
            onDrop={(e) => { e.preventDefault(); onDrop(e.dataTransfer.files[0]); }}
            style={S.drop(busy)}
          >
            <Upload size={20} color="#475569" />
            <div style={{ marginTop: 6, fontSize: 13, color: "#cbd5e1" }}>
              {busy ? "Processing new data…" : "Drop or click to add data to the stream"}
            </div>
            <div style={{ fontSize: 11, color: "#64748b" }}>
              each drop runs one incremental trigger (~1–2 min on Databricks)
            </div>
            <input
              ref={fileRef} type="file" accept=".csv,.json,.jsonl,.ndjson" hidden
              onChange={(e) => onDrop(e.target.files[0])}
            />
          </div>

          <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
            <button style={S.ghost} disabled={busy} onClick={() => refresh(streamId)}>
              <RefreshCw size={12} /> Refresh output
            </button>
            {status?.active && (
              <button style={S.ghost} onClick={stop}><Square size={12} /> Stop stream</button>
            )}
          </div>

          {status?.last_error && (
            <div style={S.err}>Last tick error: {status.last_error}</div>
          )}

          <div style={{ marginTop: 12, fontSize: 12, color: "#94a3b8" }}>
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
            <div style={{ fontSize: 12, color: "#64748b", marginTop: 6 }}>
              No output yet — drop a file to generate results.
            </div>
          )}
        </>
      )}

      {error && <div style={S.err}>{error}</div>}
    </div>
  );
}

function Pill({ label, value, color = "#94a3b8" }) {
  return (
    <div style={{ fontSize: 11, color: "#64748b" }}>
      {label}: <span style={{ color, fontWeight: 600 }}>{String(value)}</span>
    </div>
  );
}

const S = {
  card: { background: "#1e293b", border: "1px solid #334155", borderRadius: 12, padding: 20, marginTop: 16 },
  hdr:  { display: "flex", alignItems: "center", gap: 8, fontSize: 15, fontWeight: 700, color: "#f1f5f9", marginBottom: 12 },
  sub:  { fontSize: 11, color: "#475569", fontWeight: 400 },
  metaRow: { display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 12 },
  primary: (d) => ({ display: "flex", alignItems: "center", gap: 7, padding: "10px 16px", fontSize: 13, fontWeight: 700,
    background: d ? "#1e3a5f" : "#0ea5e9", color: "#fff", border: "none", borderRadius: 8, cursor: d ? "default" : "pointer" }),
  drop: (d) => ({ border: "1px dashed #334155", borderRadius: 10, padding: 24, textAlign: "center",
    cursor: d ? "default" : "pointer", background: "#0f172a", opacity: d ? 0.6 : 1 }),
  ghost: { display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", fontSize: 12, background: "transparent",
    color: "#94a3b8", border: "1px solid #334155", borderRadius: 8, cursor: "pointer" },
  th: { textAlign: "left", padding: "6px 10px", borderBottom: "1px solid #334155", color: "#64748b", whiteSpace: "nowrap" },
  td: { padding: "5px 10px", borderBottom: "1px solid #1e293b", color: "#cbd5e1", whiteSpace: "nowrap" },
  err: { marginTop: 10, fontSize: 12, color: "#fca5a5", background: "#3f1d1d", padding: "8px 10px", borderRadius: 8 },
};
