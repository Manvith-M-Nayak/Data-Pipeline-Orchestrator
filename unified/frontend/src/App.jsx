import React, { useState } from "react";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import {
  Activity, Brain, Play, FileText, AlertTriangle, TrendingUp, RefreshCw,
} from "lucide-react";
import { monitor } from "./api.js";
import LiveDashboard   from "./pages/LiveDashboard.jsx";
import PlannerPage     from "./pages/PlannerPage.jsx";
import ExecutorPage    from "./pages/ExecutorPage.jsx";
import LogsPage        from "./pages/LogsPage.jsx";
import AnomaliesPage   from "./pages/AnomaliesPage.jsx";
import PredictionsPage from "./pages/PredictionsPage.jsx";

const NAV = [
  { to: "/",            label: "Live",        icon: Activity,      group: "Monitor" },
  { to: "/logs",        label: "Logs",        icon: FileText,      group: "Monitor" },
  { to: "/anomalies",   label: "Anomalies",   icon: AlertTriangle, group: "Monitor" },
  { to: "/predictions", label: "Predictions", icon: TrendingUp,    group: "Monitor" },
  { to: "/planner",     label: "Planner",     icon: Brain,         group: "Pipeline" },
  { to: "/executor",    label: "Executor",    icon: Play,          group: "Pipeline" },
];

const S = {
  shell:   { display: "flex", minHeight: "100vh" },
  sidebar: {
    width: 210, background: "#1e293b", display: "flex", flexDirection: "column",
    padding: "24px 0", borderRight: "1px solid #334155", flexShrink: 0,
  },
  logo: { padding: "0 20px 20px", fontSize: 14, fontWeight: 700, color: "#38bdf8", letterSpacing: 0.5 },
  logoSub: { fontSize: 11, color: "#475569", fontWeight: 400, marginTop: 2 },
  groupLabel: {
    padding: "14px 20px 6px", fontSize: 10, fontWeight: 700,
    color: "#334155", textTransform: "uppercase", letterSpacing: 1,
  },
  main:    { flex: 1, overflow: "auto", padding: 28 },
  syncBtn: {
    margin: "auto 12px 12px", padding: "8px 12px", background: "#0ea5e9",
    color: "#fff", border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 12, display: "flex", alignItems: "center", gap: 6,
  },
};

function NavItem({ to, label, Icon }) {
  return (
    <NavLink
      to={to}
      end={to === "/"}
      style={({ isActive }) => ({
        display: "flex", alignItems: "center", gap: 10,
        padding: "9px 20px", borderRadius: 8, margin: "0 8px",
        color: isActive ? "#38bdf8" : "#94a3b8",
        background: isActive ? "#0f172a" : "transparent",
        fontSize: 13, fontWeight: 500,
      })}
    >
      <Icon size={15} />
      {label}
    </NavLink>
  );
}

export default function App() {
  const [syncing, setSyncing] = useState(false);

  async function handleSync() {
    setSyncing(true);
    try { await monitor.sync(48); } finally { setSyncing(false); }
  }

  const groups = [...new Set(NAV.map((n) => n.group))];

  return (
    <BrowserRouter>
      <div style={S.shell}>
        <aside style={S.sidebar}>
          <div style={S.logo}>
            Unified Agent
            <div style={S.logoSub}>Planner · Executor · Monitor</div>
          </div>
          {groups.map((g) => (
            <div key={g}>
              <div style={S.groupLabel}>{g}</div>
              {NAV.filter((n) => n.group === g).map(({ to, label, icon: Icon }) => (
                <NavItem key={to} to={to} label={label} Icon={Icon} />
              ))}
            </div>
          ))}
          <button style={S.syncBtn} onClick={handleSync} disabled={syncing}>
            <RefreshCw size={13} />
            {syncing ? "Syncing…" : "Sync History (48h)"}
          </button>
        </aside>
        <main style={S.main}>
          <Routes>
            <Route path="/"            element={<LiveDashboard />} />
            <Route path="/logs"        element={<LogsPage />} />
            <Route path="/anomalies"   element={<AnomaliesPage />} />
            <Route path="/predictions" element={<PredictionsPage />} />
            <Route path="/planner"     element={<PlannerPage />} />
            <Route path="/executor"    element={<ExecutorPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
