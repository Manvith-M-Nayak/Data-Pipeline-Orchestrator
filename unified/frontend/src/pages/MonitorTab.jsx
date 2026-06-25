import React from "react";
import { Activity } from "lucide-react";
import { useAppContext } from "../AppContext.jsx";
import LiveDashboard   from "./LiveDashboard.jsx";
import LogsPage        from "./LogsPage.jsx";
import AnomaliesPage   from "./AnomaliesPage.jsx";
import PredictionsPage from "./PredictionsPage.jsx";

const TABS = [
  { id: "live",        label: "Live Monitor" },
  { id: "logs",        label: "Run Logs" },
  { id: "anomalies",   label: "Anomalies" },
  { id: "predictions", label: "Predictions" },
];

const S = {
  page:   { maxWidth: 960, margin: "0 auto" },
  header: { marginBottom: 0 },
  agent:  { display: "flex", alignItems: "center", gap: 10, marginBottom: 6 },
  agentBadge: {
    padding: "4px 12px", background: "#0c1a2e", border: "1px solid #1e3a5f",
    borderRadius: 20, fontSize: 12, fontWeight: 700, color: "#38bdf8",
    display: "flex", alignItems: "center", gap: 6,
  },
  title:  { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:    { fontSize: 13, color: "#64748b", marginBottom: 20 },
  tabBar: {
    display: "flex", gap: 2, borderBottom: "1px solid #334155",
    marginBottom: 28,
  },
  tab:    (active) => ({
    padding: "10px 18px", fontSize: 13, fontWeight: active ? 700 : 400,
    color: active ? "#38bdf8" : "#64748b", cursor: "pointer",
    background: "transparent", border: "none",
    borderBottom: active ? "2px solid #38bdf8" : "2px solid transparent",
    marginBottom: -1, transition: "color 0.15s, border-color 0.15s",
  }),
};

export default function MonitorTab() {
  const { monitorTab: active, setMonitorTab: setActive } = useAppContext();

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div style={S.agent}>
          <span style={S.agentBadge}><Activity size={13} /> Monitor Agent</span>
        </div>
        <h1 style={S.title}>Monitor Agent</h1>
        <p style={S.sub}>Polls ADF every 20s · AI analysis on every completed run · anomaly detection.</p>
      </div>

      <div style={S.tabBar}>
        {TABS.map((t) => (
          <button key={t.id} style={S.tab(active === t.id)} onClick={() => setActive(t.id)}>
            {t.label}
          </button>
        ))}
      </div>

      {active === "live"        && <LiveDashboard />}
      {active === "logs"        && <LogsPage />}
      {active === "anomalies"   && <AnomaliesPage />}
      {active === "predictions" && <PredictionsPage />}
    </div>
  );
}
