import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { manager } from "./api.js";
import { loadDataFile, saveDataFile } from "./fileStore.js";

function lsGet(key, fallback = null) {
  try { const v = localStorage.getItem(key); return v ? JSON.parse(v) : fallback; } catch { return fallback; }
}
function lsSet(key, val) {
  try { if (val === null || val === undefined) localStorage.removeItem(key); else localStorage.setItem(key, JSON.stringify(val)); } catch { /* storage full or blocked — state still lives in memory */ }
}

// Keys from the old per-tab design. Each tab kept its own copy of the run
// (and the schema was duplicated in last_csv_schema), so after a reload the
// copies disagreed — "running" in one tab, "completed" in another.
const LEGACY_KEYS = ["exec_job_id", "exec_job_state", "exec_step", "mgr_state", "last_csv_schema"];
function migrateLegacy() {
  const oldRun = lsGet("mgr_run_id");
  if (oldRun && !lsGet("run_id")) lsSet("run_id", oldRun);
  ["mgr_run_id", ...LEGACY_KEYS].forEach((k) => lsSet(k, null));
}

export const TERMINAL = ["completed", "failed"];
export const isLive = (status) => !!status && !TERMINAL.includes(status);

const RUN_POLL_MS = 2500;
const LIST_POLL_MS = 5000;

const AppContext = createContext(null);

export function AppProvider({ children }) {
  const migrated = useRef(false);
  if (!migrated.current) { migrateLegacy(); migrated.current = true; }

  // ── Data file — kept in IndexedDB so it survives a reload ──────────────────
  const [csvFile,    setCsvFileRaw] = useState(null);
  const [csvName,    setCsvNameRaw] = useState(() => lsGet("csv_name"));
  const [csvRestoring, setCsvRestoring] = useState(() => !!lsGet("csv_name"));
  useEffect(() => {
    if (!lsGet("csv_name")) return;
    let alive = true;
    loadDataFile().then((blob) => {
      if (!alive) return;
      if (blob) {
        const name = lsGet("csv_name") || "data";
        // Some browsers hand back a Blob without its name — rewrap as a File.
        setCsvFileRaw(blob instanceof File ? blob : new File([blob], name, { type: blob.type }));
      }
      setCsvRestoring(false);
    });
    return () => { alive = false; };
  }, []);

  // ── Planner state ──────────────────────────────────────────────────────────
  const [detectedSchema, setDetectedSchemaRaw] = useState(() => lsGet("planner_schema"));
  const [plannerPrompt,  setPlannerPromptRaw]  = useState(() => lsGet("planner_prompt", ""));
  const [planResult,     setPlanResultRaw]     = useState(() => lsGet("last_plan"));

  // ── The run — ONE shared copy for every page ──────────────────────────────
  // Only the id is stored; the state always comes from the server, so a
  // reload can never show an old snapshot.
  const [runId,     setRunIdRaw]     = useState(() => lsGet("run_id"));
  const [runOrigin, setRunOriginRaw] = useState(() => lsGet("run_origin"));
  const [run,       setRun]          = useState(null);
  const [runError,  setRunError]     = useState("");
  const [runs,      setRuns]         = useState([]);   // /manager/runs, newest first
  const [runsLoaded, setRunsLoaded]  = useState(false);

  const [monitorTab, setMonitorTab] = useState("live");

  const setRunId = useCallback((id, origin = null) => {
    setRunIdRaw(id); lsSet("run_id", id);
    setRunOriginRaw(origin); lsSet("run_origin", origin);
    setRun(null);
    setRunError("");
  }, []);

  // Poll the current run: immediately, then while it is still in progress.
  const runIdRef = useRef(runId);
  runIdRef.current = runId;
  useEffect(() => {
    if (!runId) return undefined;
    let alive = true;
    let timer;
    const tick = async () => {
      try {
        const s = await manager.status(runId);
        if (!alive || runIdRef.current !== runId) return;
        setRun(s);
        setRunError("");
        if (isLive(s.status)) timer = setTimeout(tick, RUN_POLL_MS);
      } catch (e) {
        if (!alive) return;
        if (e?.status === 404) {
          setRunError("That run no longer exists on the server.");
          setRunIdRaw(null); lsSet("run_id", null); setRun(null);
        } else {
          timer = setTimeout(tick, RUN_POLL_MS);   // network blip — keep trying
        }
      }
    };
    tick();
    return () => { alive = false; clearTimeout(timer); };
  }, [runId]);

  // Poll the run list; follow a newer in-progress run when the current one
  // is finished (or none) — so a run started anywhere shows up everywhere.
  const runRef = useRef(run);
  runRef.current = run;
  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const list = await manager.listRuns();
        if (!alive) return;
        setRuns(list || []);
        setRunsLoaded(true);
        const live = (list || []).find((r) => isLive(r.status));
        const cur = runRef.current;
        const curBusy = runIdRef.current && (!cur || isLive(cur.status));
        if (live && live.run_id !== runIdRef.current && !curBusy) {
          setRunIdRaw(live.run_id); lsSet("run_id", live.run_id);
          setRunOriginRaw("external"); lsSet("run_origin", "external");
          setRun(null);
        }
      } catch { /* backend down — keep the last list */ }
    };
    load();
    const t = setInterval(load, LIST_POLL_MS);
    return () => { alive = false; clearInterval(t); };
  }, []);

  // Keep the list row of the followed run in step with its live state, so the
  // "Recent runs" list and the run panel can never disagree.
  const runsView = useMemo(() => (
    run ? runs.map((r) => (r.run_id === run.run_id
      ? { ...r, status: run.status, phase: run.phase, step: run.step, completed_at: run.completed_at }
      : r)) : runs
  ), [runs, run]);

  // Schema sent with a run — derived from the one detected schema, never a
  // second stored copy.
  const runSchema = useMemo(() => (detectedSchema ? {
    columns:     detectedSchema.columns,
    row_count:   detectedSchema.row_count ?? detectedSchema.row_count_sample ?? 0,
    size_hint:   detectedSchema.size_hint,
    file_format: detectedSchema.file_format,
    file_name:   detectedSchema.file_name,
  } : {}), [detectedSchema]);

  // ── Actions ────────────────────────────────────────────────────────────────
  const setters = useMemo(() => ({
    setCsvFile: (v) => {
      setCsvFileRaw(v);
      setCsvRestoring(false);
      const name = v && v.name ? v.name : null;
      setCsvNameRaw(name);
      lsSet("csv_name", name);
      saveDataFile(v || null);
    },
    setDetectedSchema: (v) => { setDetectedSchemaRaw(v); lsSet("planner_schema", v); },
    setPlannerPrompt:  (v) => { setPlannerPromptRaw(v);  lsSet("planner_prompt", v); },
    setPlanResult:     (v) => { setPlanResultRaw(v);     lsSet("last_plan", v); },
    setMonitorTab,
    followRun: (id) => setRunId(id, "viewed"),
    clearRun:  () => setRunId(null),
  }), [setRunId]);

  // Start a run through the Central Manager; every page then sees it.
  const startRun = useCallback(async ({ file, config, schema, request, origin }) => {
    const res = await manager.run(file, config, schema || {}, request || "");
    setRunId(res.run_id, origin);
    // Optimistic first frame until the first poll answers (a few hundred ms).
    setRun({
      run_id: res.run_id, status: "validating", phase: "validating",
      step: "Starting…", decisions: [], plan: config,
    });
    return res.run_id;
  }, [setRunId]);

  // Memoized so consumers only re-render when a value they read changes.
  const value = useMemo(() => ({
    ...setters, startRun,
    csvFile, csvName, csvRestoring, detectedSchema, plannerPrompt, planResult, runSchema,
    runId, run, runOrigin, runError, runs: runsView, runsLoaded,
    monitorTab,
  }), [setters, startRun, csvFile, csvName, csvRestoring, detectedSchema, plannerPrompt, planResult,
       runSchema, runId, run, runOrigin, runError, runsView, runsLoaded, monitorTab]);

  return (
    <AppContext.Provider value={value}>
      {children}
    </AppContext.Provider>
  );
}

export function useAppContext() {
  return useContext(AppContext);
}
