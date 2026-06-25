import React, { createContext, useContext, useState } from "react";

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

  // ── Wrapped setters that also write localStorage
  const setCsvFile        = (v) => setCsvFileRaw(v);
  const setDetectedSchema = (v) => { setDetectedSchemaRaw(v);   lsSet("planner_schema", v); };
  const setPlannerPrompt  = (v) => { setPlannerPromptRaw(v);    lsSet("planner_prompt", v); };
  const setPlanResult     = (v) => { setPlanResultRaw(v);       lsSet("last_plan", v); };
  const setExecutorJobId  = (v) => { setExecutorJobIdRaw(v);    lsSet("exec_job_id", v); };
  const setExecutorJobState=(v) => { setExecutorJobStateRaw(v); lsSet("exec_job_state", v); };
  const setExecutorStep   = (v) => { setExecutorStepRaw(v);     lsSet("exec_step", v); };
  const setManagerRunId   = (v) => { setManagerRunIdRaw(v);     lsSet("mgr_run_id", v); };
  const setManagerState   = (v) => { setManagerStateRaw(v);     lsSet("mgr_state", v); };

  return (
    <AppContext.Provider value={{
      csvFile, setCsvFile,
      detectedSchema, setDetectedSchema,
      plannerPrompt, setPlannerPrompt,
      planResult, setPlanResult,
      executorJobId, setExecutorJobId,
      executorJobState, setExecutorJobState,
      executorStep, setExecutorStep,
      managerRunId, setManagerRunId,
      managerState, setManagerState,
      monitorTab, setMonitorTab,
    }}>
      {children}
    </AppContext.Provider>
  );
}

export function useAppContext() {
  return useContext(AppContext);
}
