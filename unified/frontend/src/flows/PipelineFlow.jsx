import React, { useMemo } from "react";
import { ReactFlow, Controls, MarkerType } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import "./flows.css";
import { nodeTypes, Legend } from "./FlowNode.jsx";
import { STATUS, computeGroups, stageStatuses } from "./status.js";

const COL_W = 310;     // column pitch (room for container labels on edges)
const ROW_H = 150;     // row pitch inside a column
const RAIL = { copy: "var(--accent)", notebook: "var(--violet)", stream: "var(--ok)" };
const KIND = { copy: "ADF copy", notebook: "Notebook", stream: "Stream" };

function stageLines(s) {
  if (s.type === "copy") {
    return [
      <span key="d">Ingest unchanged · DIU <b>{s.diu ?? "auto"}</b></span>,
    ];
  }
  const lines = [];
  const tf = (s.transformations || []).filter((t) => t && t.trim());
  if (tf.length) lines.push(<span key="t"><b>{tf.length}</b> transform{tf.length > 1 ? "s" : ""} · {tf[0]}</span>);
  if (s.filter_condition) lines.push(<span key="f">filter <b>{s.filter_condition}</b></span>);
  const agg = s.aggregation;
  if (agg?.aggregations?.length) {
    lines.push(
      <span key="a">
        by <b>{(agg.group_by || []).join(", ") || "all"}</b> · {agg.aggregations.map((a) => `${a.op}(${a.column})`).join(", ")}
      </span>,
    );
  }
  if (s.type === "stream") lines.push(<span key="s">incremental · checkpointed</span>);
  if (!lines.length) lines.push(<span key="p" style={{ color: "var(--warn)" }}>pass-through (copies data)</span>);
  return lines;
}

// Copy stages name ADF datasets, not containers: the planner names a
// container's dataset "DS_" + container title-cased without "_"/"-"
// (planner_common._dataset_name). Compare both on the same normalised key.
const key = (c) => String(c || "").replace(/^DS_/, "").toLowerCase().replace(/[^a-z0-9]/g, "");
const sourceOf = (s) => s.source_container || s.source_dataset;
const sinkOf   = (s) => s.sink_container || s.sink_dataset;

function edgeClass(targetStatus) {
  if (targetStatus === STATUS.done) return "is-done";
  if (targetStatus === STATUS.running) return "is-active";
  return "";
}

// Build nodes/edges: input → [copy column] → compute groups (parallel stages
// stacked) → output. Edges follow the data (a stage's source_container is the
// container another stage writes); without container info, each column feeds
// the next.
function buildGraph(plan, statuses, inputLabel) {
  const stages = plan?.stages || [];
  const byName = Object.fromEntries(stages.map((s) => [s.name, s]));
  const copy = stages.filter((s) => s.type === "copy").map((s) => s.name);
  const columns = [...(copy.length ? [copy] : []), ...computeGroups(plan)];

  const nodes = [];
  const edges = [];
  const tallest = Math.max(1, ...columns.map((c) => c.length));

  const yFor = (j, k) => (j - (k - 1) / 2) * ROW_H;

  nodes.push({
    id: "__in", type: "flow", position: { x: 0, y: yFor(0, 1) },
    data: { variant: "terminal", kind: "Input", name: inputLabel || "your data file", status: statuses.__any },
  });

  columns.forEach((col, ci) => {
    const x = 250 + ci * COL_W;
    const isParallel = col.length > 1;
    const isCopyCol = copy.length && ci === 0;
    nodes.push({
      id: `__lbl${ci}`, type: "label", selectable: false,
      position: { x, y: yFor(0, col.length) - 72 },
      data: { label: isCopyCol ? "Ingest" : `Step ${copy.length ? ci : ci + 1}${isParallel ? " · parallel" : ""}` },
    });
    col.forEach((name, j) => {
      const s = byName[name];
      nodes.push({
        id: name, type: "flow", position: { x, y: yFor(j, col.length) },
        data: {
          kind: KIND[s?.type] || s?.type, name, rail: RAIL[s?.type],
          lines: s ? stageLines(s) : [], status: statuses[name],
        },
      });
    });
  });

  // data edges
  const producer = {};
  const containerName = {};   // normalised key → readable container name
  stages.forEach((s) => {
    if (sinkOf(s)) producer[key(sinkOf(s))] = s.name;
    [s.source_container, s.sink_container].forEach((c) => { if (c) containerName[key(c)] = c; });
  });
  const label = (c) => (c ? containerName[key(c)] || String(c).replace(/^DS_/, "").toLowerCase() : undefined);
  const hasContainers = stages.some((s) => sourceOf(s) || sinkOf(s));
  const consumed = new Set();
  const addEdge = (from, to, label) => {
    const id = `${from}->${to}`;
    if (edges.some((e) => e.id === id)) return;
    edges.push({
      id, source: from, target: to, sourceHandle: "r", targetHandle: "l",
      type: "smoothstep", label,
      className: edgeClass(to === "__out" ? statuses.__final : statuses[to]),
      animated: (to === "__out" ? statuses.__final : statuses[to]) === STATUS.running,
      markerEnd: { type: MarkerType.ArrowClosed, width: 14, height: 14 },
    });
    consumed.add(from);
  };

  columns.forEach((col, ci) => {
    col.forEach((name) => {
      const s = byName[name];
      const up = hasContainers && sourceOf(s) ? producer[key(sourceOf(s))] : undefined;
      if (up && up !== name) addEdge(up, name, label(sourceOf(s)));
      else if (ci === 0 || !hasContainers) {
        const prev = ci === 0 ? ["__in"] : columns[ci - 1];
        prev.forEach((p) => addEdge(p, name, ci === 0 ? label(sourceOf(s)) : undefined));
      } else {
        addEdge("__in", name, label(sourceOf(s)));
      }
    });
  });

  // output: stages nobody reads from
  const lastX = 250 + Math.max(columns.length - 1, 0) * COL_W + COL_W;
  const leaves = stages.map((s) => s.name).filter((n) => !consumed.has(n));
  const finalSinks = [...new Set(leaves.map((n) => label(byName[n] && sinkOf(byName[n]))).filter(Boolean))];
  nodes.push({
    id: "__out", type: "flow", position: { x: lastX, y: yFor(0, 1) },
    data: {
      variant: "terminal", kind: "Output",
      name: finalSinks.join(", ") || "result container",
      status: statuses.__final,
    },
  });
  leaves.forEach((n) => addEdge(n, "__out", undefined));

  return { nodes, edges, tallest };
}

/**
 * Pipeline graph for a plan.
 * props: plan (config with stages/execution_groups), runState (optional
 * Central Manager state → live statuses), inputLabel, height.
 */
export default function PipelineFlow({ plan, runState, inputLabel, height }) {
  const statuses = useMemo(() => {
    const st = stageStatuses(plan, runState);
    const vals = Object.values(st);
    // Input is "done" once any stage has started (the upload happens first);
    // output only once the whole run completed.
    st.__any = !runState ? STATUS.idle
      : vals.some((v) => v === STATUS.done || v === STATUS.running) ? STATUS.done : STATUS.pending;
    st.__final = !runState ? STATUS.idle
      : runState.status === "completed" ? STATUS.done : STATUS.pending;
    return st;
  }, [plan, runState]);

  const { nodes, edges, tallest } = useMemo(() => buildGraph(plan, statuses, inputLabel), [plan, statuses, inputLabel]);
  const layoutKey = (plan?.stages || []).map((s) => s.name).join("|") + "#" + JSON.stringify(plan?.execution_groups || []);

  if (!plan?.stages?.length) return null;
  const h = height || Math.max(220, tallest * ROW_H + 90);

  return (
    <div className="flow" style={{ height: h }}>
      <ReactFlow
        key={layoutKey}
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.12, maxZoom: 1 }}
        nodeOrigin={[0, 0.5]}
        minZoom={0.35}
        maxZoom={1.6}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnScroll={false}
        panOnScroll={false}
        preventScrolling={false}
        proOptions={{ hideAttribution: true }}
      >
        <Controls showInteractive={false} position="bottom-right" />
      </ReactFlow>
      {runState && <Legend />}
    </div>
  );
}
