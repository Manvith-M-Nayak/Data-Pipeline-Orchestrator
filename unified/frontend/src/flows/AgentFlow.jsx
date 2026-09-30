import React, { useMemo } from "react";
import { ReactFlow, MarkerType } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import "./flows.css";
import { nodeTypes, Legend } from "./FlowNode.jsx";
import { STATUS, agentStatuses } from "./status.js";

// How one run moves through the agents. Positions are fixed: the lifecycle is
// always the same, only the statuses change.
const X = 186;
const AGENTS = [
  { id: "planner",     x: 0,     y: 0,    kind: "Planner",         name: "Design plan",    desc: "Stages from your prompt, self-checked", rail: "var(--violet)" },
  { id: "manager",     x: X,     y: 0,    kind: "Central Manager", name: "Validate",       desc: "Plan structure and settings",           rail: "var(--accent)" },
  { id: "assurePlan",  x: X * 2, y: 0,    kind: "Assurance",       name: "Verify plan",    desc: "Schema, columns, operations",           rail: "var(--ok)" },
  { id: "resource",    x: X * 3, y: -118, kind: "Resource",        name: "Size compute",   desc: "Workers, DIU, memory",                  rail: "var(--accent)" },
  { id: "performance", x: X * 3, y: 0,    kind: "Performance",     name: "Forecast",       desc: "Runtime and risk",                      rail: "var(--accent)" },
  { id: "cost",        x: X * 3, y: 118,  kind: "Cost",            name: "Optimise cost",  desc: "Cheaper safe settings",                 rail: "var(--accent)" },
  { id: "executor",    x: X * 4, y: 0,    kind: "Executor",        name: "Execute",        desc: "ADF copy + Databricks jobs",            rail: "var(--warn)" },
  { id: "monitor",     x: X * 4, y: 138,  kind: "Monitor",         name: "Watch runs",     desc: "Anomalies, AI analysis",                rail: "var(--orange)" },
  { id: "assureOut",   x: X * 5, y: 0,    kind: "Assurance",       name: "Verify output",  desc: "Stages, output, timing",                rail: "var(--ok)" },
  { id: "learning",    x: X * 6, y: 0,    kind: "Learning",        name: "Learn",          desc: "Update models from outcome",            rail: "var(--violet)" },
];

const LINKS = [
  ["planner", "manager"], ["manager", "assurePlan"],
  ["assurePlan", "resource"], ["assurePlan", "performance"], ["assurePlan", "cost"],
  ["resource", "executor"], ["performance", "executor"], ["cost", "executor"],
  ["executor", "assureOut"], ["assureOut", "learning"],
];

/**
 * props: runState (Central Manager state, optional), hasPlan, height.
 * Without a run it is a static map of the lifecycle.
 */
export default function AgentFlow({ runState, hasPlan = false, height = 330 }) {
  const statuses = useMemo(() => agentStatuses(runState, { hasPlan }), [runState, hasPlan]);

  const nodes = useMemo(() => AGENTS.map((a) => ({
    id: a.id, type: "flow", position: { x: a.x, y: a.y },
    data: { variant: "agent", kind: a.kind, name: a.name, desc: a.desc, rail: a.rail, status: statuses[a.id] },
  })), [statuses]);

  const edges = useMemo(() => {
    const cls = (to) => (statuses[to] === STATUS.done ? "is-done" : statuses[to] === STATUS.running ? "is-active" : "");
    const main = LINKS.map(([from, to]) => ({
      id: `${from}-${to}`, source: from, target: to, sourceHandle: "r", targetHandle: "l",
      type: "smoothstep", className: cls(to), animated: statuses[to] === STATUS.running,
      markerEnd: { type: MarkerType.ArrowClosed, width: 14, height: 14 },
    }));
    // The Monitor observes the execution rather than being a step in it.
    main.push({
      id: "executor-monitor", source: "executor", target: "monitor", sourceHandle: "b", targetHandle: "t",
      type: "straight", className: `is-dashed ${cls("monitor")}`, label: "observes",
    });
    return main;
  }, [statuses]);

  return (
    <div className="flow" style={{ height }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.08 }}
        nodeOrigin={[0, 0.5]}
        minZoom={0.3}
        maxZoom={1.4}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnScroll={false}
        panOnScroll={false}
        panOnDrag={false}
        zoomOnDoubleClick={false}
        preventScrolling={false}
        proOptions={{ hideAttribution: true }}
      />
      {runState && <Legend />}
    </div>
  );
}
