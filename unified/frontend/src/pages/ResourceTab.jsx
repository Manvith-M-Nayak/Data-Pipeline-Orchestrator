import React, { useState, useEffect, useCallback } from "react";
import {
  Cpu, MemoryStick, Zap, TrendingUp, AlertTriangle,
  CheckCircle, RefreshCw, BarChart3, GitBranch, Clock,
} from "lucide-react";
import { resource, monitor } from "../api.js";

// ── Styles ────────────────────────────────────────────────────────────────────
const S = {
  page:    { maxWidth: 960, margin: "0 auto" },
  heading: { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:     { fontSize: 13, color: "#64748b", marginBottom: 28 },
  grid2:   { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  card:    {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 12,
    padding: "16px 20px", marginBottom: 16,
  },
  cardHdr: {
    display: "flex", alignItems: "center", gap: 8,
    fontSize: 13, fontWeight: 700, color: "#f1f5f9", marginBottom: 14,
  },
  kv:      { display: "flex", flexDirection: "column", gap: 6 },
  kvRow:   {
    display: "flex", justifyContent: "space-between", alignItems: "center",
    fontSize: 12, color: "#94a3b8", paddingBottom: 6,
    borderBottom: "1px solid #1e293b",
  },
  kvVal:   { color: "#f1f5f9", fontWeight: 600 },
  badge:   (color) => ({
    display: "inline-block", padding: "2px 8px", borderRadius: 99,
    fontSize: 11, fontWeight: 700,
    background: color + "22", color: color,
  }),
  btn:     {
    padding: "8px 16px", background: "#0ea5e9", color: "#fff",
    border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
  },
  tag:     (color) => ({
    padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 700,
    background: color + "22", color: color, marginRight: 4,
  }),
  bar:     (pct, color) => ({
    height: 6, width: `${Math.min(pct, 100)}%`, background: color,
    borderRadius: 3, transition: "width 0.4s ease",
  }),
  barBg:   { height: 6, background: "#1e293b", borderRadius: 3, marginTop: 4, overflow: "hidden" },
  stageRow: {
    padding: "10px 0", borderBottom: "1px solid #1e293b",
    display: "flex", flexDirection: "column", gap: 4,
  },
  warn:    { display: "flex", gap: 6, fontSize: 11, color: "#f59e0b", marginTop: 6 },
  error:   { display: "flex", gap: 6, fontSize: 11, color: "#f87171", marginTop: 6 },
};

// ── Mini helpers ──────────────────────────────────────────────────────────────
function Badge({ text, color = "#38bdf8" }) {
  return <span style={S.badge(color)}>{text}</span>;
}

function StatCard({ label, value, sub, color = "#38bdf8", Icon }) {
  return (
    <div style={S.card}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        {Icon && <Icon size={14} color={color} />}
        <span style={{ fontSize: 12, color: "#64748b" }}>{label}</span>
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#475569", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

// ── Accuracy report section ───────────────────────────────────────────────────
function AccuracySection({ report }) {
  if (!report || report.total_records === 0) {
    return (
      <div style={{ ...S.card, textAlign: "center", color: "#475569", fontSize: 13, padding: "32px 20px" }}>
        No prediction history yet — run a pipeline through Central Manager to start collecting data.
      </div>
    );
  }

  const types = Object.entries(report.by_type || {});
  return (
    <div style={S.grid2}>
      {types.map(([stype, stats]) => {
        const accuracyPct = stats.accuracy_pct || 0;
        const ratioColor  = Math.abs(stats.mean_ratio - 1) < 0.2 ? "#4ade80"
                          : Math.abs(stats.mean_ratio - 1) < 0.5 ? "#f59e0b"
                          : "#f87171";
        return (
          <div key={stype} style={S.card}>
            <div style={S.cardHdr}>
              <BarChart3 size={14} color="#c084fc" />
              {stype.charAt(0).toUpperCase() + stype.slice(1)} stage accuracy
              <span style={{ marginLeft: "auto" }}>
                <Badge
                  text={`${accuracyPct}%`}
                  color={accuracyPct > 80 ? "#4ade80" : accuracyPct > 60 ? "#f59e0b" : "#f87171"}
                />
              </span>
            </div>
            <div style={S.kv}>
              <div style={S.kvRow}><span>Runs recorded</span><span style={S.kvVal}>{stats.count}</span></div>
              <div style={S.kvRow}>
                <span>Mean actual/predicted</span>
                <span style={{ ...S.kvVal, color: ratioColor }}>{stats.mean_ratio}×</span>
              </div>
              <div style={S.kvRow}>
                <span>Correction factor applied</span>
                <span style={S.kvVal}>{stats.correction_factor}×</span>
              </div>
            </div>
            <div style={{ marginTop: 10 }}>
              <div style={{ fontSize: 11, color: "#475569", marginBottom: 4 }}>
                Recent ratios (actual/predicted)
              </div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {(stats.recent_ratios || []).map((r, i) => (
                  <span key={i} style={S.tag(Math.abs(r - 1) < 0.2 ? "#4ade80" : "#f59e0b")}>
                    {r}×
                  </span>
                ))}
              </div>
            </div>
            <div style={{ marginTop: 8 }}>
              <div style={{ fontSize: 11, color: "#475569", marginBottom: 4 }}>
                Accuracy {accuracyPct}%
              </div>
              <div style={S.barBg}>
                <div style={S.bar(accuracyPct, accuracyPct > 80 ? "#4ade80" : "#f59e0b")} />
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── Live plan analysis section ─────────────────────────────────────────────────
function LiveAnalysis({ plan, allocations, feasible, violations, warnings, execGroups }) {
  if (!allocations || allocations.length === 0) return null;

  return (
    <div style={S.card}>
      <div style={S.cardHdr}>
        <Cpu size={14} color="#38bdf8" />
        Stage Allocations
        <span style={{ marginLeft: "auto" }}>
          <Badge text={feasible ? "Feasible" : "Infeasible"} color={feasible ? "#4ade80" : "#f87171"} />
        </span>
      </div>

      {violations?.length > 0 && violations.map((v, i) => (
        <div key={i} style={S.error}><AlertTriangle size={11} />{v}</div>
      ))}
      {warnings?.length > 0 && warnings.map((w, i) => (
        <div key={i} style={S.warn}><AlertTriangle size={11} />{w}</div>
      ))}

      {allocations.map((a) => {
        const isNotebook = a.stage_type === "notebook";
        const workerColor = a.workers === 0 ? "#64748b" : "#38bdf8";
        return (
          <div key={a.stage_name} style={S.stageRow}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: 13, fontWeight: 600, color: "#f1f5f9" }}>{a.stage_name}</span>
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                {a.right_sized && <span style={S.tag("#4ade80")}>right-sized</span>}
                {a.contention_adjusted && <span style={S.tag("#f59e0b")}>contention-adjusted</span>}
                <Badge
                  text={a.stage_type}
                  color={isNotebook ? "#c084fc" : "#f59e0b"}
                />
              </div>
            </div>
            <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#94a3b8", marginTop: 2, flexWrap: "wrap" }}>
              {isNotebook ? (
                <>
                  <span style={{ color: workerColor }}>{a.workers} worker{a.workers !== 1 ? "s" : ""}</span>
                  <span>{a.memory_gb} GB memory</span>
                  <span>{a.cpu} vCPU</span>
                </>
              ) : (
                <>
                  <span style={{ color: "#f59e0b" }}>{a.diu} DIU</span>
                  <span>{a.memory_gb} GB scratch</span>
                </>
              )}
              <span><Clock size={10} style={{ marginRight: 3, verticalAlign: "middle" }} />~{a.duration_s}s</span>
            </div>
          </div>
        );
      })}

      {execGroups && execGroups.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <div style={{ fontSize: 12, color: "#64748b", marginBottom: 6, display: "flex", gap: 6, alignItems: "center" }}>
            <GitBranch size={12} /> Execution groups after contention resolution
          </div>
          {execGroups.map((group, gi) => (
            <div key={gi} style={{ display: "flex", gap: 6, marginBottom: 4, alignItems: "center" }}>
              <span style={{ fontSize: 11, color: "#475569", minWidth: 60 }}>Group {gi + 1}</span>
              {group.map((name) => (
                <span key={name} style={S.tag(group.length > 1 ? "#38bdf8" : "#64748b")}>{name}</span>
              ))}
              {group.length > 1 && (
                <span style={{ fontSize: 10, color: "#38bdf8" }}>parallel</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Dynamic re-allocation panel ───────────────────────────────────────────────
function ReallocationPanel({ recs }) {
  if (!recs || recs.length === 0) return null;
  const colors = { ok: "#4ade80", scale_up: "#f59e0b", reclaim: "#38bdf8", investigate: "#f87171" };
  return (
    <div style={S.card}>
      <div style={S.cardHdr}><Zap size={14} color="#f59e0b" />Dynamic Re-allocation Recommendations</div>
      {recs.map((r, i) => (
        <div key={i} style={{ ...S.stageRow, flexDirection: "row", alignItems: "center", gap: 10 }}>
          <span style={S.tag(colors[r.action] || "#64748b")}>{r.action}</span>
          <span style={{ fontSize: 12, color: "#f1f5f9", flex: 1 }}>{r.stage}</span>
          <span style={{ fontSize: 11, color: "#64748b" }}>{r.reason}</span>
        </div>
      ))}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function ResourceTab() {
  const [accuracy, setAccuracy]           = useState(null);
  const [factors, setFactors]             = useState(null);
  const [recs, setRecs]                   = useState(null);
  const [liveRp, setLiveRp]              = useState(null);
  const [loading, setLoading]             = useState(false);
  const [reallocationLoading, setRlLoad] = useState(false);
  const [err, setErr]                     = useState("");

  const fetchAccuracy = useCallback(async () => {
    setLoading(true);
    try {
      const [acc, cf] = await Promise.all([resource.accuracy(), resource.correctionFactors()]);
      setAccuracy(acc);
      setFactors(cf);
      setErr("");
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchAccuracy(); }, [fetchAccuracy]);

  async function checkReallocate() {
    setRlLoad(true);
    try {
      const live = await monitor.getLiveRuns();
      if (!live || live.length === 0) {
        setRecs([]);
        return;
      }
      const allocs = liveRp?.allocations || [];
      if (allocs.length === 0) {
        setRecs([{ stage: "n/a", action: "ok", reason: "No active resource plan — run via Central Manager first." }]);
        return;
      }
      const result = await resource.reallocate(live, allocs, 0);
      setRecs(result.recommendations || []);
    } catch (e) {
      setErr(e.message);
    } finally {
      setRlLoad(false);
    }
  }

  const totalRecords = accuracy?.total_records || 0;
  const avgAccuracy  = accuracy?.by_type
    ? Object.values(accuracy.by_type).reduce((s, t) => s + (t.accuracy_pct || 0), 0) /
      Math.max(Object.keys(accuracy.by_type).length, 1)
    : null;

  return (
    <div style={S.page}>
      <div style={S.heading}>Resource Agent</div>
      <div style={S.sub}>
        Predicts compute requirements, proposes right-sized allocations, resolves contention,
        and self-corrects from historical run data.
      </div>

      {/* Summary stats */}
      <div style={S.grid3}>
        <StatCard
          label="Prediction records"
          value={totalRecords}
          sub="runs used for self-correction"
          Icon={BarChart3}
          color="#38bdf8"
        />
        <StatCard
          label="Avg prediction accuracy"
          value={avgAccuracy != null ? `${avgAccuracy.toFixed(1)}%` : "—"}
          sub="actual vs predicted duration"
          Icon={TrendingUp}
          color={avgAccuracy == null ? "#475569" : avgAccuracy > 80 ? "#4ade80" : "#f59e0b"}
        />
        <StatCard
          label="Correction factors"
          value={factors ? `${factors.copy}× / ${factors.notebook}×` : "—"}
          sub="copy / notebook (damped)"
          Icon={Zap}
          color="#c084fc"
        />
      </div>

      {/* Refresh + dynamic reallocation buttons */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
        <button style={S.btn} onClick={fetchAccuracy} disabled={loading}>
          <RefreshCw size={13} />
          {loading ? "Loading…" : "Refresh accuracy"}
        </button>
        <button
          style={{ ...S.btn, background: "#7c3aed" }}
          onClick={checkReallocate}
          disabled={reallocationLoading}
        >
          <Zap size={13} />
          {reallocationLoading ? "Checking…" : "Check live re-allocation"}
        </button>
      </div>

      {err && (
        <div style={{ ...S.card, border: "1px solid #f87171", color: "#f87171", fontSize: 12 }}>
          {err}
        </div>
      )}

      {/* Dynamic re-allocation */}
      <ReallocationPanel recs={recs} />

      {/* Live resource plan from manager context (if any) */}
      {liveRp && (
        <LiveAnalysis
          allocations={liveRp.allocations}
          feasible={liveRp.feasible}
          violations={liveRp.constraint_violations}
          warnings={liveRp.warnings}
          execGroups={liveRp.execution_groups}
        />
      )}

      {/* Accuracy history */}
      <div style={S.card}>
        <div style={S.cardHdr}>
          <TrendingUp size={14} color="#4ade80" />
          Prediction Accuracy History
          {totalRecords === 0 && (
            <span style={{ marginLeft: 8, fontSize: 11, color: "#475569", fontWeight: 400 }}>
              — no data yet
            </span>
          )}
        </div>
        <AccuracySection report={accuracy} />
      </div>

      {/* Student tier limits reference */}
      <div style={S.card}>
        <div style={S.cardHdr}><CheckCircle size={14} color="#64748b" />Student-Tier Hard Limits</div>
        <div style={S.kv}>
          <div style={S.kvRow}><span>Max Databricks workers</span><span style={S.kvVal}>4</span></div>
          <div style={S.kvRow}><span>Max ADF DIU</span><span style={S.kvVal}>8</span></div>
          <div style={S.kvRow}><span>Max parallel stages in one group</span><span style={S.kvVal}>3</span></div>
          <div style={S.kvRow}><span>Max total memory (parallel group)</span><span style={S.kvVal}>64 GB</span></div>
          <div style={S.kvRow}><span>Default node type</span><span style={S.kvVal}>Standard_D4s_v3 (4 vCPU / 16 GB)</span></div>
          <div style={{ ...S.kvRow, borderBottom: "none" }}>
            <span>ADF throughput per DIU</span>
            <span style={S.kvVal}>~5 MB/s</span>
          </div>
        </div>
      </div>
    </div>
  );
}
