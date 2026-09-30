// Derive per-stage and per-agent status from a Central Manager run state.
// Everything is read from what the backend already reports — no new API:
//   state.status / state.phase      manager lifecycle
//   state.step                      executor progress text, e.g.
//                                   "Running stage group 2/3 (parallel): A, B"
//                                   "Monitoring Databricks run 123 (stage: A)"
//                                   "Waiting for ADF copy pipeline to complete"
//   state.executor_result           stages / stages_completed after execution
//   state.decisions                 "PHASE:<NAME>" entries (phase history)

export const STATUS = { idle: "idle", pending: "pending", running: "running", done: "done", failed: "failed" };

const PHASES = ["validating", "assuring_plan", "pre_checks", "executing", "assurance", "feedback", "completed"];

// record_feedback() used to move failed runs into "feedback"; the real failure
// point is the last phase entered before it (see ManagerTab failedPhase()).
export function failedPhaseOf(state) {
  const entered = (state?.decisions || [])
    .filter((d) => d.action?.startsWith("PHASE:"))
    .map((d) => d.action.slice(6).toLowerCase())
    .filter((p) => p !== "feedback");
  return entered.length ? entered[entered.length - 1] : state?.phase;
}

function phaseIndex(state) {
  if (!state) return -1;
  if (state.status === "completed") return PHASES.length - 1;
  const p = state.status === "failed" ? failedPhaseOf(state) : state.phase;
  return PHASES.indexOf(p);
}

// Execution order of compute (notebook/stream) stages, grouped exactly like the
// executor groups them: execution_groups filtered to compute stages, then any
// stage the groups missed as its own group.
export function computeGroups(plan) {
  const stages = plan?.stages || [];
  const compute = stages.filter((s) => s.type !== "copy");
  const names = new Set(compute.map((s) => s.name));
  const groups = [];
  const seen = new Set();
  for (let g of plan?.execution_groups || []) {
    if (!Array.isArray(g)) g = [g];
    const keep = g.filter((n) => names.has(n) && !seen.has(n));
    keep.forEach((n) => seen.add(n));
    if (keep.length) groups.push(keep);
  }
  compute.forEach((s) => { if (!seen.has(s.name)) groups.push([s.name]); });
  return groups;
}

// Map stage name → STATUS for one run of `plan`.
export function stageStatuses(plan, state) {
  const stages = plan?.stages || [];
  const out = {};
  const all = (st) => stages.forEach((s) => { out[s.name] = st; });

  if (!state) { all(STATUS.idle); return out; }
  if (state.status === "completed") { all(STATUS.done); return out; }

  const pIdx = phaseIndex(state);
  const execIdx = PHASES.indexOf("executing");
  if (pIdx < execIdx) { all(STATUS.pending); return out; }

  const copy = stages.filter((s) => s.type === "copy").map((s) => s.name);
  const groups = computeGroups(plan);
  const finished = new Set(state.executor_result?.stages_completed || state.executor_result?.stages || []);

  // Past execution (assurance/feedback) and not failed → every stage ran.
  if (pIdx > execIdx && state.status !== "failed") { all(STATUS.done); return out; }

  if (state.status === "failed") {
    // Stages the executor reports as finished are done. The failed stage is
    // the one the error names (by stage name or its filter text); if the
    // error names none, it is the first unfinished stage — but only when some
    // stage had finished, i.e. execution was genuinely under way. Failures
    // before any stage ran (notebook build, auth, upload) blame no stage.
    const order = [...copy, ...groups.flat()];
    const errText = `${state.error || ""} ${state.executor_result?.message || ""}`;
    const byName = Object.fromEntries(stages.map((s) => [s.name, s]));
    const named = order.find((n) => errText.includes(n)
      || (byName[n]?.filter_condition && errText.includes(byName[n].filter_condition)));
    const firstOpen = finished.size ? order.find((n) => !finished.has(n)) : undefined;
    const culprit = pIdx === execIdx ? (named || firstOpen) : undefined;
    order.forEach((n) => {
      out[n] = finished.has(n) ? STATUS.done : n === culprit ? STATUS.failed : STATUS.pending;
    });
    return out;
  }

  // Executing now — read the executor's progress text.
  const step = state.step || "";
  const low = step.toLowerCase();
  all(STATUS.pending);
  finished.forEach((n) => { if (n in out) out[n] = STATUS.done; });

  if (low.includes("copy pipeline") || low.includes("linked service")) {
    copy.forEach((n) => { out[n] = STATUS.running; });
    return out;
  }

  let running = [];
  const grp = /stage group (\d+)\/\d+[^:]*:\s*(.+)$/i.exec(step);
  const mon = /\(stage:\s*([^)]+)\)/i.exec(step);
  if (grp) running = grp[2].split(",").map((s) => s.trim());
  else if (mon) running = [mon[1].trim()];

  if (running.length) {
    copy.forEach((n) => { out[n] = STATUS.done; });            // compute runs after copy
    const gi = groups.findIndex((g) => g.some((n) => running.includes(n)));
    groups.forEach((g, i) => {
      g.forEach((n) => {
        if (i < gi) out[n] = STATUS.done;
        else if (i === gi) out[n] = running.includes(n) || grp ? STATUS.running : out[n];
      });
    });
  }
  return out;
}

// ── agents ───────────────────────────────────────────────────────────────────
// Which manager phase each agent node belongs to.
export const AGENT_PHASE = {
  planner:     null,            // before the run
  manager:     "validating",
  assurePlan:  "assuring_plan",
  resource:    "pre_checks",
  performance: "pre_checks",
  cost:        "pre_checks",
  executor:    "executing",
  monitor:     "executing",
  assureOut:   "assurance",
  learning:    "feedback",
};

export function agentStatuses(state, { hasPlan = false } = {}) {
  const out = {};
  const pIdx = phaseIndex(state);
  const failed = state?.status === "failed";
  for (const [agent, phase] of Object.entries(AGENT_PHASE)) {
    if (phase === null) { out[agent] = hasPlan || state ? STATUS.done : STATUS.idle; continue; }
    if (!state) { out[agent] = STATUS.idle; continue; }
    const i = PHASES.indexOf(phase);
    if (state.status === "completed") out[agent] = STATUS.done;
    else if (i < pIdx) out[agent] = STATUS.done;
    else if (i === pIdx) out[agent] = failed ? STATUS.failed : STATUS.running;
    else out[agent] = STATUS.pending;
  }
  // The Monitor watches executions; it never "fails" the run itself.
  if (out.monitor === STATUS.failed) out.monitor = STATUS.done;
  // Learning still records failed runs (feedback is written on failure too).
  if (failed && out.learning === STATUS.pending) out.learning = STATUS.done;
  return out;
}
