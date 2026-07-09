import React, { useState } from "react";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { Home, Brain, Zap, Activity, GitBranch, Cpu, RefreshCw, TrendingUp, DollarSign } from "lucide-react";
import { monitor } from "./api.js";
import { AppProvider } from "./AppContext.jsx";
import HomePage    from "./pages/HomePage.jsx";
import PlannerTab  from "./pages/PlannerTab.jsx";
import ExecutorTab from "./pages/ExecutorTab.jsx";
import MonitorTab  from "./pages/MonitorTab.jsx";
import ManagerTab  from "./pages/ManagerTab.jsx";
import ResourceTab from "./pages/ResourceTab.jsx";
import PerformancePredictionTab from "./pages/PerformancePredictionTab.jsx";
import CostOptimizationTab from "./pages/CostOptimizationTab.jsx";

const TABS = [
  { to: "/",          label: "Home",              icon: Home,       exact: true  },
  { to: "/planner",   label: "Planner Agent",     icon: Brain,      exact: false },
  { to: "/manager",   label: "Central Manager",   icon: GitBranch,  exact: false },
  { to: "/resource",     label: "Resource Agent",        icon: Cpu,         exact: false },
  { to: "/performance",  label: "Performance Agent",     icon: TrendingUp,  exact: false },
  { to: "/cost",      label: "Cost Optimization", icon: DollarSign,  exact: false },
  { to: "/executor",  label: "Executor Agent",    icon: Zap,        exact: false },
  { to: "/monitor",   label: "Monitor Agent",     icon: Activity,   exact: false },
];

const S = {
  shell:   { display: "flex", flexDirection: "column", minHeight: "100vh", background: "#0f172a" },
  header:  {
    display: "flex", alignItems: "center", gap: 0,
    background: "#1e293b", borderBottom: "1px solid #334155",
    padding: "0 24px", height: 52, flexShrink: 0,
  },
  logo: {
    fontSize: 14, fontWeight: 700, color: "#f1f5f9",
    marginRight: 32, whiteSpace: "nowrap", letterSpacing: 0.3,
  },
  logoSub: { fontSize: 11, color: "#475569", fontWeight: 400 },
  tabs:    { display: "flex", alignItems: "stretch", gap: 2, flex: 1 },
  tab:     (active) => ({
    display: "flex", alignItems: "center", gap: 7, padding: "0 16px",
    fontSize: 13, fontWeight: active ? 700 : 400,
    color: active ? "#38bdf8" : "#64748b",
    background: "transparent", border: "none", cursor: "pointer",
    borderBottom: active ? "2px solid #38bdf8" : "2px solid transparent",
    textDecoration: "none", transition: "color 0.15s, border-color 0.15s",
    height: "100%",
  }),
  syncBtn: {
    marginLeft: "auto", padding: "6px 12px", background: "transparent",
    color: "#475569", border: "1px solid #334155", borderRadius: 8,
    cursor: "pointer", fontSize: 12, display: "flex", alignItems: "center", gap: 5,
    flexShrink: 0,
  },
  main: { flex: 1, padding: 32, overflowY: "auto" },
};

function TabLink({ to, label, Icon, exact }) {
  return (
    <NavLink
      to={to}
      end={exact}
      style={({ isActive }) => S.tab(isActive)}
    >
      <Icon size={14} />
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

  return (
    <BrowserRouter>
    <AppProvider>
      <div style={S.shell}>
        <header style={S.header}>
          <div style={S.logo}>
            Pipeline Orchestrator
            <div style={S.logoSub}>AI-powered · Azure ADF + Databricks</div>
          </div>
          <nav style={S.tabs}>
            {TABS.map(({ to, label, icon: Icon, exact }) => (
              <TabLink key={to} to={to} label={label} Icon={Icon} exact={exact} />
            ))}
          </nav>
          <button style={S.syncBtn} onClick={handleSync} disabled={syncing}>
            <RefreshCw size={12} />
            {syncing ? "Syncing…" : "Sync (48h)"}
          </button>
        </header>
        <main style={S.main}>
          <Routes>
            <Route path="/"          element={<HomePage />} />
            <Route path="/planner"   element={<PlannerTab />} />
            <Route path="/manager"   element={<ManagerTab />} />
            <Route path="/resource"      element={<ResourceTab />} />
            <Route path="/performance"   element={<PerformancePredictionTab />} />
            <Route path="/cost"      element={<CostOptimizationTab />} />
            <Route path="/executor"  element={<ExecutorTab />} />
            <Route path="/monitor"   element={<MonitorTab />} />
          </Routes>
        </main>
      </div>
    </AppProvider>
    </BrowserRouter>
  );
}
