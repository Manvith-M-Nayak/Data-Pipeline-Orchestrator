/* eslint-env node */
// Run: node src/flows/status.test.mjs — checks how run state maps to stage/agent statuses.
import { stageStatuses, agentStatuses, computeGroups } from "./status.js";
const plan = { stages: [
  { name: "Ingest", type: "copy", sink_container: "raw" },
  { name: "A", type: "notebook", source_container: "raw", sink_container: "a" },
  { name: "B", type: "notebook", source_container: "raw", sink_container: "b" },
  { name: "C", type: "notebook", source_container: "a", sink_container: "c" },
], execution_groups: [["Ingest"], ["A", "B"], ["C"]] };
const t = (name, got, want) => { const ok = JSON.stringify(got) === JSON.stringify(want); console.log(ok ? "PASS" : "FAIL", name, ok ? "" : JSON.stringify(got)); if (!ok) process.exitCode = 1; };
t("groups", computeGroups(plan), [["A","B"],["C"]]);
t("idle", stageStatuses(plan, null), { Ingest:"idle", A:"idle", B:"idle", C:"idle" });
t("pre-checks", stageStatuses(plan, { status:"pre_checks", phase:"pre_checks" }), { Ingest:"pending", A:"pending", B:"pending", C:"pending" });
t("copy running", stageStatuses(plan, { status:"executing", phase:"executing", step:"Waiting for ADF copy pipeline to complete" }), { Ingest:"running", A:"pending", B:"pending", C:"pending" });
t("group 1 parallel", stageStatuses(plan, { status:"executing", phase:"executing", step:"Running stage group 1/2 (parallel): A, B" }), { Ingest:"done", A:"running", B:"running", C:"pending" });
t("monitor C", stageStatuses(plan, { status:"executing", phase:"executing", step:"Monitoring Databricks run 99 (stage: C)" }), { Ingest:"done", A:"done", B:"done", C:"running" });
t("completed", stageStatuses(plan, { status:"completed", phase:"completed" }), { Ingest:"done", A:"done", B:"done", C:"done" });
t("failed in exec", stageStatuses(plan, { status:"failed", phase:"executing", executor_result:{ stages_completed:["Ingest","A"] } }), { Ingest:"done", A:"done", B:"failed", C:"pending" });
t("failed legacy feedback", stageStatuses(plan, { status:"failed", phase:"feedback", decisions:[{action:"PHASE:EXECUTING"},{action:"PHASE:FEEDBACK"}], executor_result:{ stages_completed:[] } }), { Ingest:"pending", A:"pending", B:"pending", C:"pending" });
const planF = { ...plan, stages: plan.stages.map((s) => s.name === "C" ? { ...s, filter_condition: "predator IS TRUE" } : s) };
t("failed: error quotes C's filter", stageStatuses(planF, { status:"failed", phase:"executing", error:"filter_condition 'predator IS TRUE' could not be converted", executor_result:{ stages_completed:[] } }), { Ingest:"pending", A:"pending", B:"pending", C:"failed" });
t("failed: error names stage B", stageStatuses(plan, { status:"failed", phase:"executing", error:"Databricks run failed for stage B", executor_result:{ stages_completed:["Ingest"] } }), { Ingest:"done", A:"pending", B:"failed", C:"pending" });
t("failed before exec", stageStatuses(plan, { status:"failed", phase:"assuring_plan" }), { Ingest:"pending", A:"pending", B:"pending", C:"pending" });
const a = agentStatuses({ status:"executing", phase:"executing" }, { hasPlan: true });
t("agents executing", [a.planner,a.manager,a.resource,a.executor,a.monitor,a.assureOut,a.learning], ["done","done","done","running","running","pending","pending"]);
const f = agentStatuses({ status:"failed", phase:"feedback", decisions:[{action:"PHASE:VALIDATING"},{action:"PHASE:ASSURING_PLAN"},{action:"PHASE:FEEDBACK"}] }, { hasPlan: true });
t("agents failed at verify", [f.manager,f.assurePlan,f.resource,f.executor,f.learning], ["done","failed","pending","pending","done"]);
t("agents static", agentStatuses(null, { hasPlan: false }).planner, "idle");
