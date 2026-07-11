// frontend/src/pages/LearningTab.jsx
// Dashboard tab for the Learning & Policy Update Agent.
// Follows the same structure/conventions as PerformancePredictionTab.jsx
// (plain inline-styled React, no new dependencies).
// Requires the `learning` export in frontend/src/api.js.

import { useEffect, useState } from "react";
import { learning } from "../api";

// ── Design tokens — matched to the app shell's actual dark theme
// (same hex values as App.jsx: page #0f172a, card/header #1e293b,
// border #334155, text #f1f5f9, accent #38bdf8) ─────────────────────────────
const color = {
  bg: "#1e293b",
  border: "#334155",
  text: "#f1f5f9",
  muted: "#94a3b8",
  good: "#4ade80",
  goodBg: "rgba(34, 197, 94, 0.15)",
  warn: "#fbbf24",
  warnBg: "rgba(217, 119, 6, 0.18)",
  bad: "#f87171",
  badBg: "rgba(220, 38, 38, 0.18)",
  info: "#3b82f6",
  infoBg: "rgba(59, 130, 246, 0.18)",
};

const card = {
  background: color.bg,
  border: `1px solid ${color.border}`,
  borderRadius: 10,
  padding: 16,
};
const label = { fontSize: 12, color: color.muted, marginBottom: 4, fontWeight: 600, letterSpacing: 0.2 };
const statValue = { fontSize: 24, fontWeight: 700, color: color.text, fontVariantNumeric: "tabular-nums" };
const mono = { fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" };

function pct(x) { return x == null ? "—" : `${(x * 100).toFixed(1)}%`; }
function num(x, d = 2) { return x == null ? "—" : Number(x).toFixed(d); }
function ts(t) {
  if (!t) return "—";
  const d = typeof t === "number" ? new Date(t * 1000) : new Date(t);
  return isNaN(d) ? String(t) : d.toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

// ── Badge: small status pill used throughout ────────────────────────────────
function Badge({ tone = "info", children }) {
  const map = {
    good: [color.good, color.goodBg],
    warn: [color.warn, color.warnBg],
    bad: [color.bad, color.badBg],
    info: [color.info, color.infoBg],
    neutral: [color.muted, "rgba(148, 163, 184, 0.15)"],
  };
  const [fg, bg] = map[tone] || map.info;
  return (
    <span style={{
      display: "inline-block", padding: "2px 8px", borderRadius: 999,
      fontSize: 12, fontWeight: 600, color: fg, background: bg,
    }}>
      {children}
    </span>
  );
}

// ── The signature element: a chronological timeline of what the agent
// actually did. This is the tab's one deliberate design risk — everything
// else stays quiet and consistent with sibling tabs, but "what did this
// agent do" is a genuine chronological sequence, so a timeline is the
// correct answer here, not decoration.
//
// Deliberately does NOT show raw technical reason text (mape percentages,
// "ml_model-sourced records", etc.) — that's audit-log detail for a dev
// console, not something a person reading this tab needs. Every entry is
// reduced to one plain-English sentence built from its actual old/new
// values. Entries for policies not shown elsewhere in this tab (resource
// headroom, the inactive cost flag) are filtered out entirely rather than
// left in half-explained. ───────────────────────────────────────────────
function Timeline({ entries }) {
  const visible = entries.filter((e) => {
    if (e.type === "policy_update" && typeof e.policy === "string") {
      if (e.policy.startsWith("resource_headroom_factors")) return false;
      if (e.policy === "cost_estimate_accuracy_flag") return false;
    }
    return true;
  });

  if (!visible.length) {
    return <div style={{ color: color.muted, fontSize: 14, padding: "12px 0" }}>
      Nothing logged yet — the agent waits for enough runs before it acts (see the status card above).
    </div>;
  }

  const toneFor = (entry) => {
    if (entry.type === "policy_review" && entry.action === "rolled_back") return "bad";
    if (entry.type === "policy_review" && entry.action === "confirmed") return "good";
    if (entry.type === "retrain_triggered") return "info";
    if (entry.type === "resource_agent_drift_flag") return "warn";
    if (entry.type === "manual_rollback") return "bad";
    return "neutral";
  };

  const scalePct = (x) => `${Math.round(x * 100)}%`;

  const titleFor = (entry) => {
    switch (entry.type) {
      case "policy_update":
        return entry.policy === "flagged_signatures"
          ? "Flagged a pipeline for review"
          : "Adjusted prediction scaling";
      case "policy_review":
        return entry.action === "rolled_back" ? "Undid a recent adjustment" : "Kept a recent adjustment";
      case "retrain_triggered":
        return "Retrained the prediction model";
      case "resource_agent_drift_flag":
        return "Flagged a resource sizing issue";
      case "manual_rollback":
        return "Restored a previous backup";
      default:
        return "Update";
    }
  };

  const detailFor = (entry) => {
    switch (entry.type) {
      case "policy_update":
        if (entry.policy === "flagged_signatures") {
          return `"${entry.new}" needs a closer look`;
        }
        return `Now scaling predictions to ${scalePct(entry.new)} of the raw estimate (was ${scalePct(entry.old)})`;
      case "policy_review":
        return entry.action === "rolled_back"
          ? `That change made things worse, so it reverted back to ${scalePct(entry.reverted_to)}`
          : `Scaling at ${scalePct(entry.value)} is holding up — keeping it`;
      case "retrain_triggered":
        return "Predictions had drifted too far from reality, so it retrained on the latest runs";
      case "resource_agent_drift_flag":
        return `${entry.stage_type} stage estimates have stayed off for a while — may need a closer look`;
      case "manual_rollback":
        return "Restored by request";
      default:
        return "";
    }
  };

  return (
    <div style={{ position: "relative", paddingLeft: 20 }}>
      <div style={{
        position: "absolute", left: 5, top: 4, bottom: 4, width: 2,
        background: color.border,
      }} />
      {[...visible].reverse().map((e, i) => {
        const tone = toneFor(e);
        const dotColor = { good: color.good, bad: color.bad, info: color.info, warn: color.warn, neutral: color.muted }[tone];
        return (
          <div key={i} style={{ position: "relative", paddingBottom: 18 }}>
            <div style={{
              position: "absolute", left: -20, top: 3, width: 10, height: 10,
              borderRadius: "50%", background: dotColor, border: `2px solid ${color.bg}`,
              boxShadow: `0 0 0 1px ${dotColor}`,
            }} />
            <div style={{ fontSize: 13, color: color.muted, ...mono }}>{ts(e.timestamp)}</div>
            <div style={{ fontSize: 14, fontWeight: 600, margin: "2px 0", color: color.text }}>{titleFor(e)}</div>
            <div style={{ fontSize: 13, color: color.muted, marginTop: 2, maxWidth: 640 }}>
              {detailFor(e)}
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default function LearningTab() {
  const [status, setStatus] = useState(null);
  const [log, setLog] = useState([]);
  const [driftFlags, setDriftFlags] = useState([]);
  const [cycleReport, setCycleReport] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);

  const refresh = async () => {
    try {
      const [s, l, d] = await Promise.all([
        learning.status(), learning.log(), learning.resourceDrift(),
      ]);
      setStatus(s);
      setLog(l.entries || []);
      setDriftFlags(d.flags || []);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => { refresh(); }, []);

  const runCycle = async () => {
    setBusy(true);
    try {
      const rep = await learning.cycle();
      setCycleReport(rep);
      await refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  if (!status) return <div style={{ padding: 24, color: color.muted }}>Loading learning agent…</div>;

  const m = status.metrics || {};
  const p = status.policies || {};
  const rt = status.retraining || {};
  const ml = (m.by_source || {}).ml_model || {};
  const flagged = Object.entries(p.flagged_signatures || {});
  const pendingReview = p._pending_reviews && p._pending_reviews.duration_correction_factor;

  const adjustmentCount = log.filter(
    (e) => e.type === "policy_update" && e.policy === "duration_correction_factor"
  ).length;
  const retrainCount = log.filter((e) => e.type === "retrain_triggered").length;
  const rolledBackCount = log.filter(
    (e) => e.type === "policy_review" && e.action === "rolled_back"
  ).length;

  return (
    <div style={{ padding: 24, display: "flex", flexDirection: "column", gap: 20, maxWidth: 900, margin: "0 auto" }}>
      <div>
        <h2 style={{ margin: 0, color: color.text }}>Learning &amp; Policy Updates</h2>
        <div style={{ color: color.muted, fontSize: 13, marginTop: 6 }}>
          Watches every run and adjusts predictions based on what actually happened — gradually,
          and only once a pattern shows up, never off a single run.
        </div>
      </div>

      <button onClick={runCycle} disabled={busy}
        style={{ padding: "10px 18px", borderRadius: 8, border: "none", width: "fit-content",
                 background: color.info, color: "#fff", cursor: "pointer", fontWeight: 600,
                 display: "flex", alignItems: "center", gap: 8 }}>
        {busy ? "Checking…" : "Check for updates now"}
      </button>

      <div style={{ color: color.muted, fontSize: 13, marginTop: -12 }}>
        {status.total_runs_recorded} runs recorded · next automatic check in{" "}
        {Math.max(0, status.cycle_every_n_runs - status.runs_since_last_cycle)} run(s)
      </div>

      {error && <div style={{ color: color.bad, fontSize: 14 }}>Couldn't load: {error}</div>}

      {/* stat cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
        <div style={card}>
          <div style={label}>DURATION ratio (actual ÷ predicted)</div>
          <div style={statValue}>{num(ml.duration_ratio)}</div>
          <div style={{ fontSize: 12, color: color.muted, marginTop: 2 }}>
            {ml.duration_ratio == null ? "not enough ML-path runs yet" :
             ml.duration_ratio < 1 ? "model predicts too high" : "model predicts too low"}
          </div>
        </div>
        <div style={card}>
          <div style={label}>Applied correction</div>
          <div style={statValue}>{num(p.duration_correction_factor, 3)}×</div>
          <div style={{ fontSize: 12, marginTop: 2 }}>
            {pendingReview
              ? <Badge tone="warn">awaiting review</Badge>
              : <Badge tone="good">confirmed</Badge>}
          </div>
        </div>
        <div style={card}>
          <div style={label}>ML runs measured</div>
          <div style={statValue}>{ml.runs ?? 0}</div>
          <div style={{ fontSize: 12, color: color.muted, marginTop: 2 }}>
            of {p.min_runs_for_update} needed to act
          </div>
        </div>
        <div style={card}>
          <div style={label}>Retraining</div>
          <div style={{ fontSize: 15, fontWeight: 700, marginTop: 2 }}>
            {rt.retraining_now ? <Badge tone="info">in progress</Badge> :
             rt.last_retrain?.deployed ? <Badge tone="good">deployed</Badge> :
             rt.last_retrain?.rolled_back ? <Badge tone="bad">rolled back</Badge> :
             <Badge tone="neutral">none yet</Badge>}
          </div>
          {rt.last_retrain && (
            <div style={{ fontSize: 12, color: color.muted, marginTop: 4 }}>
              MAE {num(rt.last_retrain.mae_before, 1)}s → {num(rt.last_retrain.mae_after, 1)}s
            </div>
          )}
        </div>
      </div>

      {/* flags for humans */}
      {(flagged.length > 0 || driftFlags.length > 0) && (
        <div style={{ ...card, borderColor: color.warn, background: color.warnBg }}>
          <div style={{ ...label, color: color.warn }}>Needs a human look</div>
          {flagged.map(([sig, info]) => (
            <div key={sig} style={{ fontSize: 14, marginBottom: 6, color: color.text }}>
              <b>{sig}</b>: {Object.entries(info.reasons || {}).map(([k, v]) => `${k} ${pct(v)}`).join(", ")}
              {" "}over {info.runs} runs
            </div>
          ))}
          {driftFlags.map((f, i) => (
            <div key={i} style={{ fontSize: 14, marginBottom: 6, color: color.text }}>
              <b>Resource Agent — {f.stage_type} stages</b>: correction stuck at {f.correction_factor}×
              after {f.records} records
            </div>
          ))}
        </div>
      )}

      {/* the timeline — what the agent has actually done, in order */}
      <div style={card}>
        <div style={label}>What it's done</div>
        <div style={{ fontSize: 14, color: color.text, marginBottom: 16 }}>
          {adjustmentCount} adjustment{adjustmentCount !== 1 ? "s" : ""} made
          {retrainCount > 0 && <> · {retrainCount} retrain{retrainCount !== 1 ? "s" : ""} triggered</>}
          {rolledBackCount > 0 && <> · {rolledBackCount} undone after making things worse</>}
          {" · "}currently scaling ML predictions to {Math.round((p.duration_correction_factor ?? 1) * 100)}%
          of the raw estimate
        </div>
        <Timeline entries={log} />
      </div>

    </div>
  );
}
