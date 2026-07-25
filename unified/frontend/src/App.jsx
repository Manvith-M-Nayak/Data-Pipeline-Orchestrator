import React, { useState, Suspense, lazy } from "react";
import { BrowserRouter, Routes, Route, NavLink, useLocation } from "react-router-dom";
import { Home, Brain, Zap, Activity, GitBranch, Cpu, RefreshCw, TrendingUp, DollarSign, AlertTriangle, BarChart3 } from "lucide-react";
import { monitor } from "./api.js";
import { AppProvider, useAppContext } from "./AppContext.jsx";
import ErrorBoundary from "./ErrorBoundary.jsx";

// Route-level code splitting — each tab ships in its own chunk instead of one
// monolithic bundle, so the initial load only pulls the landing page.
const HomePage    = lazy(() => import("./pages/HomePage.jsx"));
const PlannerTab  = lazy(() => import("./pages/PlannerTab.jsx"));
const ExecutorTab = lazy(() => import("./pages/ExecutorTab.jsx"));
const MonitorTab  = lazy(() => import("./pages/MonitorTab.jsx"));
const ManagerTab  = lazy(() => import("./pages/ManagerTab.jsx"));
const ResourceTab = lazy(() => import("./pages/ResourceTab.jsx"));
const PerformancePredictionTab = lazy(() => import("./pages/PerformancePredictionTab.jsx"));
const CostOptimizationTab = lazy(() => import("./pages/CostOptimizationTab.jsx"));
const RunInsights = lazy(() => import("./pages/RunInsights.jsx"));

const TABS = [
  { to: "/",          label: "Home",              icon: Home,       exact: true  },
  { to: "/planner",   label: "Planner Agent",     icon: Brain,      exact: false },
  { to: "/manager",   label: "Central Manager",   icon: GitBranch,  exact: false },
  { to: "/resource",     label: "Resource Agent",        icon: Cpu,         exact: false },
  { to: "/performance",  label: "Performance Agent",     icon: TrendingUp,  exact: false },
  { to: "/cost",      label: "Cost Optimization", icon: DollarSign,  exact: false },
  { to: "/executor",  label: "Executor Agent",    icon: Zap,        exact: false },
  { to: "/monitor",   label: "Monitor Agent",     icon: Activity,   exact: false },
  { to: "/insights",  label: "Run Insights",      icon: BarChart3,  exact: false },
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
  banner: {
    display: "flex", alignItems: "center", gap: 8,
    background: "#422006", borderBottom: "1px solid #78350f", color: "#fcd34d",
    padding: "8px 24px", fontSize: 12.5, flexShrink: 0,
  },
  loading: { color: "#64748b", fontSize: 13, padding: 8 },
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

// Reload drops the in-memory File object but keeps derived state (schema/plan)
// in localStorage — warn the user their restored plan has no CSV to run against.
function CsvBanner() {
  const { csvFile, detectedSchema, csvName } = useAppContext();
  if (csvFile || !detectedSchema) return null;
  return (
    <div style={S.banner}>
      <AlertTriangle size={14} />
      Restored a saved plan{csvName ? ` for "${csvName}"` : ""}, but the data file
      was cleared by the page reload. Re-select it in the Planner tab before running.
    </div>
  );
}

function Shell() {
  const [syncing, setSyncing] = useState(false);
  const location = useLocation();

  async function handleSync() {
    setSyncing(true);
    try { await monitor.sync(48); } finally { setSyncing(false); }
  }

  return (
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
      <CsvBanner />
      <main style={S.main}>
        {/* Keyed by route so a crash in one tab clears when you navigate away. */}
        <ErrorBoundary key={location.pathname}>
          <Suspense fallback={<div style={S.loading}>Loading…</div>}>
            <Routes>
              <Route path="/"          element={<HomePage />} />
              <Route path="/planner"   element={<PlannerTab />} />
              <Route path="/manager"   element={<ManagerTab />} />
              <Route path="/resource"      element={<ResourceTab />} />
              <Route path="/performance"   element={<PerformancePredictionTab />} />
              <Route path="/cost"      element={<CostOptimizationTab />} />
              <Route path="/executor"  element={<ExecutorTab />} />
              <Route path="/monitor"   element={<MonitorTab />} />
              <Route path="/insights"  element={<RunInsights />} />
            </Routes>
          </Suspense>
        </ErrorBoundary>
      </main>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AppProvider>
        <Shell />
      </AppProvider>
    </BrowserRouter>
  );
}
