import React, { useState, useEffect, useCallback } from "react";
import {
  DollarSign, TrendingDown, TrendingUp, RefreshCw,
  AlertTriangle, CheckCircle, Cpu, Clock, BarChart3, Zap,
} from "lucide-react";
import { cost } from "../api.js";

const S = {
  page:    { maxWidth: 960, margin: "0 auto" },
  heading: { fontSize: 22, fontWeight: 700, color: "#f1f5f9", marginBottom: 4 },
  sub:     { fontSize: 13, color: "#64748b", marginBottom: 28 },
  grid2:   { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  card:    {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 12,
    padding: "16px 20px", marginBottom: 16,
  },
  cardHdr: {
    display: "flex", alignItems: "center", gap: 8,
    fontSize: 13, fontWeight: 700, color: "#f1f5f9", marginBottom: 14,
  },
  kv:      { display: "flex", flexDirection: "column", gap: 6 },
  kvRow:   {
    display: "flex", justifyContent: "space-between", alignItems: "center",
    fontSize: 12, color: "#94a3b8", paddingBottom: 6,
    borderBottom: "1px solid #1e293b",
  },
  kvVal:   { color: "#f1f5f9", fontWeight: 600 },
  badge:   (color) => ({
    display: "inline-block", padding: "2px 8px", borderRadius: 99,
    fontSize: 11, fontWeight: 700,
    background: color + "22", color: color,
  }),
  btn:     {
    padding: "8px 16px", background: "#0ea5e9", color: "#fff",
    border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
  },
  tag:     (color) => ({
    padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 700,
    background: color + "22", color: color, marginRight: 4,
  }),
  recRow:  {
    padding: "12px 0", borderBottom: "1px solid #1e293b",
    display: "flex", flexDirection: "column", gap: 6,
  },
};

function Badge({ text, color = "#38bdf8" }) {
  return <span style={S.badge(color)}>{text}</span>;
}

function StatCard({ label, value, sub, color = "#38bdf8", Icon }) {
  return (
    <div style={S.card}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        {Icon && <Icon size={14} color={color} />}
        <span style={{ fontSize: 12, color: "#64748b" }}>{label}</span>
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#475569", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

export default function CostOptimizationTab() {
  const [rates, setRates]       = useState(null);
  const [result, setResult]     = useState(null);
  const [loading, setLoading]   = useState(false);
  const [err, setErr]           = useState("");

  const fetchRates = useCallback(async () => {
    try {
      const r = await cost.nodeRates();
      setRates(r);
    } catch (e) {
      console.warn("node-rates fetch failed", e);
    }
  }, []);

  useEffect(() => { fetchRates(); }, [fetchRates]);

  async function runOptimize() {
    setLoading(true);
    setErr("");
    try {
      const plan = {
        stages: [
          { name: "ingest", type: "copy", source_dataset: "DS_Raw", sink_dataset: "DS_Bronze", diu: 8 },
          { name: "transform", type: "notebook",
            source_container: "bronze", sink_container: "silver",
            transformations: ["c1 = expr1", "c2 = expr2", "c3 = expr3"],
            filter_condition: "amount > 0" },
          { name: "aggregate", type: "notebook",
            source_container: "silver", sink_container: "gold",
            transformations: ["c1 = expr1"],
            aggregations: { group_by: ["grp"], agg_exprs: ["sum(amount)", "avg(amount)"] } },
        ],
        recommended_settings: { node_type: "Standard_D4s_v3", shuffle_partitions: 200 },
        schema: { row_count: 500000, columns: ["c1", "c2", "c3", "amount", "grp"], size_hint: "medium" },
        csv_size_bytes: 70 * 1024 * 1024,
      };

      const perf = { predicted_total_s: 367, throughput_mb_per_s: 15 };
      const rp   = {
        allocations: [
          { stage_name: "ingest", stage_type: "copy", workers: 0, diu: 2, memory_gb: 3, node_type: "Standard_D4s_v3", duration_s: 50 },
          { stage_name: "transform", stage_type: "notebook", workers: 1, diu: 0, memory_gb: 4.07, node_type: "Standard_D4s_v3", shuffle_partitions: 200, duration_s: 154 },
          { stage_name: "aggregate", stage_type: "notebook", workers: 4, diu: 0, memory_gb: 4.13, node_type: "Standard_D4s_v3", shuffle_partitions: 200, duration_s: 163 },
        ],
        peak_concurrent_workers: 4,
        estimated_total_s: 367,
        file_size_mb: 70,
      };

      const res = await cost.optimize(plan, perf, rp);
      setResult(res);
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }

  const costTotal = result?.estimated_cost?.total_usd;
  const recs      = result?.recommendations || [];
  const source    = result?.optimization_source || "—";

  return (
    <div style={S.page}>
      <div style={S.heading}>Cost Optimization Agent</div>
      <div style={S.sub}>
        Estimates pipeline dollar cost and recommends cost-saving configuration changes
        via ML model or heuristic fallback. Never breaks deadlines or drops below
        minimum resources.
      </div>

      {/* Engine banner */}
      {result && (
        <div style={{ ...S.card, display: "flex", alignItems: "center", gap: 10, padding: "12px 20px" }}>
          <Cpu size={15} color={source === "ml_model" ? "#c084fc" : "#f59e0b"} />
          <span style={{ fontSize: 13, color: "#f1f5f9", fontWeight: 600 }}>Optimization engine:</span>
          <Badge
            text={source === "ml_model" ? "ML model" : "Heuristic fallback"}
            color={source === "ml_model" ? "#c084fc" : "#f59e0b"}
          />
          {result?.estimated_cost && (
            <span style={{ fontSize: 11, color: "#64748b" }}>
              estimated ${costTotal?.toFixed(4)} total
            </span>
          )}
        </div>
      )}

      {/* Stats */}
      <div style={S.grid3}>
        <StatCard
          label="Estimated Cost"
          value={costTotal != null ? `$${costTotal.toFixed(4)}` : "—"}
          sub="USD (compute + DBU + ADF + storage)"
          Icon={DollarSign}
          color="#4ade80"
        />
        <StatCard
          label="Recommendations"
          value={recs.length}
          sub={recs.length > 0 ? `best saves ${recs[0]?.estimated_saving || "—"}` : "run optimize to generate"}
          Icon={TrendingDown}
          color={recs.length > 0 ? "#38bdf8" : "#475569"}
        />
        <StatCard
          label="Optimization source"
          value={source === "ml_model" ? "ML model" : source === "heuristic" ? "Heuristic" : "—"}
          sub={source === "ml_model" ? "HistGradientBoosting (trained)" : "rule-based fallback"}
          Icon={Zap}
          color={source === "ml_model" ? "#c084fc" : "#f59e0b"}
        />
      </div>

      {err && (
        <div style={{ ...S.card, border: "1px solid #f87171", color: "#f87171", fontSize: 12 }}>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <AlertTriangle size={13} />{err}
          </div>
        </div>
      )}

      {/* Run optimize button */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
        <button style={S.btn} onClick={runOptimize} disabled={loading}>
          {loading ? <RefreshCw size={13} style={{ animation: "spin 1s linear infinite" }} /> : <Zap size={13} />}
          {loading ? "Optimizing…" : "Run cost optimization"}
        </button>
      </div>

      {/* Cost breakdown */}
      {result?.estimated_cost && (
        <div style={S.card}>
          <div style={S.cardHdr}>
            <DollarSign size={14} color="#4ade80" />
            Cost Breakdown
          </div>
          <div style={S.kv}>
            <div style={S.kvRow}>
              <span>Compute (VM nodes)</span>
              <span style={S.kvVal}>${result.estimated_cost.compute_usd.toFixed(4)}</span>
            </div>
            <div style={S.kvRow}>
              <span>Databricks DBU</span>
              <span style={S.kvVal}>${result.estimated_cost.databricks_dbu_usd.toFixed(4)}</span>
            </div>
            <div style={S.kvRow}>
              <span>ADF activity</span>
              <span style={S.kvVal}>${result.estimated_cost.adf_usd.toFixed(4)}</span>
            </div>
            <div style={S.kvRow}>
              <span>Storage</span>
              <span style={S.kvVal}>${result.estimated_cost.storage_usd.toFixed(4)}</span>
            </div>
            <div style={{ ...S.kvRow, borderBottom: "none", fontSize: 14 }}>
              <span style={{ fontWeight: 700, color: "#f1f5f9" }}>Total</span>
              <span style={{ ...S.kvVal, color: "#4ade80", fontSize: 16 }}>
                ${result.estimated_cost.total_usd.toFixed(4)}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Recommendations */}
      {recs.length > 0 && (
        <div style={S.card}>
          <div style={S.cardHdr}>
            <TrendingDown size={14} color="#f59e0b" />
            Optimization Recommendations
            <span style={{ marginLeft: "auto" }}>
              <Badge text={recs.length.toString()} color="#38bdf8" />
            </span>
          </div>
          {recs.map((r, i) => {
            const riskColor = r.risk_level === "low" ? "#4ade80"
                           : r.risk_level === "medium" ? "#f59e0b"
                           : "#f87171";
            const sourceColor = r.source === "ml" ? "#c084fc" : "#38bdf8";
            return (
              <div key={i} style={S.recRow}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: "#f1f5f9", flex: 1 }}>
                    {r.change}
                  </span>
                  <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                    <Badge text={`save ${r.estimated_saving}`} color="#4ade80" />
                    <Badge text={r.risk_level} color={riskColor} />
                    <Badge text={r.source === "ml" ? "ML" : "rule"} color={sourceColor} />
                  </div>
                </div>
                <div style={{ fontSize: 11, color: "#94a3b8" }}>{r.trade_off}</div>
                <div style={{ fontSize: 11, color: "#64748b" }}>{r.reason}</div>
                {r.new_cost && (
                  <div style={{ fontSize: 11, color: "#475569", marginTop: 2 }}>
                    new cost: ${r.new_cost.total_usd?.toFixed(4)}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* Node rates reference */}
      {rates?.node_hourly_rates && (
        <div style={S.card}>
          <div style={S.cardHdr}>
            <BarChart3 size={14} color="#64748b" />
            Node Hourly Rates (Cost Model)
          </div>
          <div style={S.kv}>
            {Object.entries(rates.node_hourly_rates).map(([node, rate]) => (
              <div key={node} style={S.kvRow}>
                <span>{node}</span>
                <span style={S.kvVal}>${rate.toFixed(2)}/hr</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Assumptions */}
      <div style={S.card}>
        <div style={S.cardHdr}>
          <CheckCircle size={14} color="#64748b" />
          Cost Model Assumptions
        </div>
        <div style={{ fontSize: 11, color: "#64748b", lineHeight: 1.6 }}>
          All costs are estimates. No real billing data was available at build time.
          DBU pricing includes ~2.5× Databricks markup. Off-peak scheduling assumes
          30% discount. Storage priced at $0.018/GB/month, prorated by runtime.
        </div>
      </div>
    </div>
  );
}
