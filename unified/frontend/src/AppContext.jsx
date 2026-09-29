import React, { createContext, useContext, useMemo, useState } from "react";

function lsGet(key, fallback = null) {
  try { const v = localStorage.getItem(key); return v ? JSON.parse(v) : fallback; } catch { return fallback; }
}
function lsSet(key, val) {
  try { if (val === null || val === undefined) localStorage.removeItem(key); else localStorage.setItem(key, JSON.stringify(val)); } catch {}
}

const AppContext = createContext(null);

export function AppProvider({ children }) {
  // ── CSV file (File object — survives tab switch, not page reload)
  const [csvFile,         setCsvFileRaw]    = useState(null);
  // Name persists across reload so the UI can name the file the user must
  // re-select (the File object itself cannot be serialized to localStorage).
  const [csvName,         setCsvNameRaw]    = useState(() => lsGet("csv_name"));

  // ── Planner state (persisted to localStorage)
  const [detectedSchema,  setDetectedSchemaRaw]  = useState(() => lsGet("planner_schema"));
  const [plannerPrompt,   setPlannerPromptRaw]   = useState(() => lsGet("planner_prompt", ""));
  const [planResult,      setPlanResultRaw]      = useState(() => lsGet("last_plan"));

  // ── Executor state (persisted to localStorage)
  const [executorJobId,   setExecutorJobIdRaw]   = useState(() => lsGet("exec_job_id"));
  const [executorJobState,setExecutorJobStateRaw]= useState(() => lsGet("exec_job_state"));
  const [executorStep,    setExecutorStepRaw]    = useState(() => lsGet("exec_step", -1));

  // ── Central Manager state (persisted to localStorage)
  const [managerRunId,   setManagerRunIdRaw]   = useState(() => lsGet("mgr_run_id"));
  const [managerState,   setManagerStateRaw]   = useState(() => lsGet("mgr_state"));

  // ── Monitor sub-tab (session only)
  const [monitorTab, setMonitorTab] = useState("live");

  // ── Wrapped setters that also write localStorage. Built once: the raw
  // useState setters are stable, so consumers get stable function identities.
  const setters = useMemo(() => ({
    setCsvFile: (v) => {
      setCsvFileRaw(v);
      const name = v && v.name ? v.name : null;
      setCsvNameRaw(name);
      lsSet("csv_name", name);
    },
    setDetectedSchema:   (v) => { setDetectedSchemaRaw(v);   lsSet("planner_schema", v); },
    setPlannerPrompt:    (v) => { setPlannerPromptRaw(v);    lsSet("planner_prompt", v); },
    setPlanResult:       (v) => { setPlanResultRaw(v);       lsSet("last_plan", v); },
    setExecutorJobId:    (v) => { setExecutorJobIdRaw(v);    lsSet("exec_job_id", v); },
    setExecutorJobState: (v) => { setExecutorJobStateRaw(v); lsSet("exec_job_state", v); },
    setExecutorStep:     (v) => { setExecutorStepRaw(v);     lsSet("exec_step", v); },
    setManagerRunId:     (v) => { setManagerRunIdRaw(v);     lsSet("mgr_run_id", v); },
    setManagerState:     (v) => { setManagerStateRaw(v);     lsSet("mgr_state", v); },
    setMonitorTab,
  }), []);

  // Memoized so consumers only re-render when a value they read changes.
  const value = useMemo(() => ({
    ...setters,
    csvFile, csvName, detectedSchema, plannerPrompt, planResult,
    executorJobId, executorJobState, executorStep,
    managerRunId, managerState, monitorTab,
  }), [setters, csvFile, csvName, detectedSchema, plannerPrompt, planResult,
       executorJobId, executorJobState, executorStep,
       managerRunId, managerState, monitorTab]);

  return (
    <AppContext.Provider value={value}>
      {children}
    </AppContext.Provider>
  );
}

export function useAppContext() {
  return useContext(AppContext);
}
