import React, { useEffect, useState, Suspense, lazy } from "react";
import { BrowserRouter, Routes, Route, NavLink, useLocation } from "react-router-dom";
import {
  LayoutGrid, Brain, Zap, Activity, GitBranch, Cpu, RefreshCw, Gauge,
  CircleDollarSign, AlertTriangle, BarChart3, Moon, Sun, Workflow,
} from "lucide-react";
import { monitor, health } from "./api.js";
import { AppProvider, useAppContext } from "./AppContext.jsx";
import ErrorBoundary from "./ErrorBoundary.jsx";
import { useTheme } from "./ui/theme.js";
import { Button, Dot, Spinner } from "./ui/components.jsx";

// Route-level code splitting — each page ships in its own chunk.
const HomePage    = lazy(() => import("./pages/HomePage.jsx"));
const PlannerTab  = lazy(() => import("./pages/PlannerTab.jsx"));
const ExecutorTab = lazy(() => import("./pages/ExecutorTab.jsx"));
const MonitorTab  = lazy(() => import("./pages/MonitorTab.jsx"));
const ManagerTab  = lazy(() => import("./pages/ManagerTab.jsx"));
const ResourceTab = lazy(() => import("./pages/ResourceTab.jsx"));
const PerformancePredictionTab = lazy(() => import("./pages/PerformancePredictionTab.jsx"));
const CostOptimizationTab = lazy(() => import("./pages/CostOptimizationTab.jsx"));
const RunInsights = lazy(() => import("./pages/RunInsights.jsx"));

// Navigation follows the work: build a plan → run it → watch it → tune agents.
const NAV = [
  { group: "Workspace", items: [
    { to: "/",          label: "Overview",        icon: LayoutGrid, element: <HomePage /> },
  ]},
  { group: "Build", items: [
    { to: "/planner",   label: "Planner",         icon: Brain,      element: <PlannerTab /> },
  ]},
  { group: "Run", items: [
    { to: "/manager",   label: "Central Manager", icon: GitBranch,  element: <ManagerTab /> },
    { to: "/executor",  label: "Executor",        icon: Zap,        element: <ExecutorTab /> },
  ]},
  { group: "Observe", items: [
    { to: "/monitor",   label: "Monitor",         icon: Activity,   element: <MonitorTab /> },
    { to: "/insights",  label: "Run Insights",    icon: BarChart3,  element: <RunInsights /> },
  ]},
  { group: "Agents", items: [
    { to: "/resource",    label: "Resource",      icon: Cpu,              element: <ResourceTab /> },
    { to: "/performance", label: "Performance",   icon: Gauge,            element: <PerformancePredictionTab /> },
    { to: "/cost",        label: "Cost",          icon: CircleDollarSign, element: <CostOptimizationTab /> },
  ]},
];
const ROUTES = NAV.flatMap((g) => g.items.map((i) => ({ ...i, group: g.group })));

// Reload drops the in-memory File object but keeps derived state (schema/plan)
// in localStorage — warn the user their restored plan has no file to run against.
function CsvBanner() {
  const { csvFile, csvRestoring, detectedSchema, csvName } = useAppContext();
  // Normally the file is restored from browser storage; this only shows when
  // that was impossible (file too large, private mode, storage cleared).
  if (csvFile || csvRestoring || !detectedSchema) return null;
  return (
    <div className="banner">
      <AlertTriangle size={14} strokeWidth={2} />
      Restored a saved plan{csvName ? ` for “${csvName}”` : ""}, but its data file could not be restored
      in this browser. Re-select it in the Planner before running.
    </div>
  );
}

// Backend reachability, polled — shown in the sidebar footer.
function useBackendStatus() {
  const [ok, setOk] = useState(null);
  useEffect(() => {
    let alive = true;
    const check = () => health().then(() => alive && setOk(true)).catch(() => alive && setOk(false));
    check();
    const t = setInterval(check, 30000);
    return () => { alive = false; clearInterval(t); };
  }, []);
  return ok;
}

function Sidebar({ theme, toggleTheme }) {
  const backendOk = useBackendStatus();
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark"><Workflow size={15} strokeWidth={2.2} /></div>
        <div>
          <div className="brand-name">Pipeline Orchestrator</div>
          <div className="brand-sub">ADF · Databricks</div>
        </div>
      </div>

      <nav aria-label="Main">
        {NAV.map((g) => (
          <div key={g.group} className="nav-group">
            <div className="nav-label">{g.group}</div>
            {g.items.map(({ to, label, icon: Icon }) => (
              <NavLink key={to} to={to} end={to === "/"} className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                <Icon size={15} strokeWidth={1.8} />
                {label}
              </NavLink>
            ))}
          </div>
        ))}
      </nav>

      <div className="sidebar-foot">
        <div className="row" style={{ gap: 8, padding: "4px 10px", fontSize: 12, color: "var(--text-3)" }}>
          <Dot tone={backendOk === null ? "neutral" : backendOk ? "ok" : "bad"} live={backendOk === true} />
          {backendOk === null ? "Checking backend…" : backendOk ? "Backend connected" : "Backend unreachable"}
        </div>
        <button className="nav-link" onClick={toggleTheme} style={{ border: 0, background: "none", cursor: "pointer", width: "100%" }}>
          {theme === "dark" ? <Sun size={15} strokeWidth={1.8} /> : <Moon size={15} strokeWidth={1.8} />}
          {theme === "dark" ? "Light theme" : "Dark theme"}
        </button>
      </div>
    </aside>
  );
}

function Topbar() {
  const location = useLocation();
  const route = ROUTES.find((r) => r.to === location.pathname);
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState("");

  async function handleSync() {
    setSyncing(true);
    setSyncError("");
    try { await monitor.sync(48); }
    catch (e) { setSyncError(e.message || "Sync failed"); }
    finally { setSyncing(false); }
  }

  return (
    <header className="topbar">
      <div className="crumbs">
        {route && <span>{route.group}</span>}
        {route && <span style={{ color: "var(--text-4)" }}>/</span>}
        <b>{route?.label || "Not found"}</b>
      </div>
      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
        {syncError && <span style={{ fontSize: 12, color: "var(--bad)" }} title={syncError}>Sync failed</span>}
        <Button size="sm" icon={RefreshCw} loading={syncing} onClick={handleSync}
          title="Pull the last 48 hours of ADF pipeline runs into the monitor">
          {syncing ? "Syncing" : "Sync ADF runs"}
        </Button>
      </div>
    </header>
  );
}

function Shell() {
  const location = useLocation();
  const { theme, toggle } = useTheme();
  return (
    <div className="shell">
      <Sidebar theme={theme} toggleTheme={toggle} />
      <div className="main">
        <Topbar />
        <CsvBanner />
        <main className="content" key={location.pathname}>
          {/* Keyed by route so a crash in one page clears when you navigate away. */}
          <ErrorBoundary key={location.pathname}>
            <Suspense fallback={<div className="row" style={{ gap: 8, color: "var(--text-3)" }}><Spinner /> Loading…</div>}>
              <Routes>
                {ROUTES.map((r) => <Route key={r.to} path={r.to} element={r.element} />)}
              </Routes>
            </Suspense>
          </ErrorBoundary>
        </main>
      </div>
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
