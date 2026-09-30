import React from "react";
import { Handle, Position } from "@xyflow/react";
import { CheckCircle2, Circle, XCircle } from "lucide-react";
import { Spinner } from "../ui/components.jsx";
import { STATUS } from "./status.js";

export function StatusIcon({ status }) {
  if (status === STATUS.running) return <Spinner size={12} />;
  if (status === STATUS.done)    return <CheckCircle2 size={14} strokeWidth={2} style={{ color: "var(--ok)" }} />;
  if (status === STATUS.failed)  return <XCircle size={14} strokeWidth={2} style={{ color: "var(--bad)" }} />;
  if (status === STATUS.pending) return <Circle size={12} strokeWidth={2} style={{ color: "var(--text-4)" }} />;
  return null;
}

// Handles on all four sides; edges pick them by id (l/r for the main flow,
// t/b for the vertical Monitor link). Invisible — the diagram is read-only.
function Handles() {
  return (
    <>
      <Handle type="target" position={Position.Left}   id="l" isConnectable={false} />
      <Handle type="source" position={Position.Right}  id="r" isConnectable={false} />
      <Handle type="target" position={Position.Top}    id="t" isConnectable={false} />
      <Handle type="source" position={Position.Bottom} id="b" isConnectable={false} />
    </>
  );
}

// Pipeline stage / input / output, and agent nodes share one renderer.
export function FlowNode({ data }) {
  const { kind, name, lines = [], desc, status, rail, variant } = data;
  const cls = ["fnode", variant, status && status !== STATUS.idle ? status : ""].filter(Boolean).join(" ");
  return (
    <div className={cls} style={rail ? { "--rail": rail } : undefined} title={name}>
      <Handles />
      <div className="fnode-top">
        {kind && <span className="fnode-kind">{kind}</span>}
        <span className="fnode-status"><StatusIcon status={status} /></span>
      </div>
      {/* zero-width spaces after "_" so long snake_case names wrap at word joins */}
      <div className="fnode-name">{String(name ?? "").replace(/_/g, "_\u200b")}</div>
      {desc && <div className="fnode-desc">{desc}</div>}
      {lines.length > 0 && (
        <div className="fnode-lines">
          {lines.map((l, i) => <div key={i} className="fnode-line" title={typeof l === "string" ? l : undefined}>{l}</div>)}
        </div>
      )}
    </div>
  );
}

export function GroupLabel({ data }) {
  return <div className="fgroup-label">{data.label}</div>;
}

export const nodeTypes = { flow: FlowNode, label: GroupLabel };

export function Legend() {
  return (
    <div className="flow-legend">
      <span><Circle size={10} strokeWidth={2.4} style={{ color: "var(--text-4)" }} /> waiting</span>
      <span><Spinner size={10} /> running</span>
      <span><CheckCircle2 size={11} strokeWidth={2.2} style={{ color: "var(--ok)" }} /> done</span>
      <span><XCircle size={11} strokeWidth={2.2} style={{ color: "var(--bad)" }} /> failed</span>
    </div>
  );
}
