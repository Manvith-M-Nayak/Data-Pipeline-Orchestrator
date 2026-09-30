import React, { useState, useEffect, useCallback } from "react";
import {
  DollarSign, TrendingDown, RefreshCw,
  AlertTriangle, CheckCircle, Cpu, BarChart3, Zap,
} from "lucide-react";
import { cost, manager } from "../api.js";

const usd = (v) => (typeof v === "number" ? `$${v.toFixed(4)}` : "—");

// How many recent runs to look through for one that reached cost optimization
// (runs that fail validation or pre-checks never get that far).
const RUN_SCAN_LIMIT = 10;

const S = {
  page:    { maxWidth: 960, margin: "0 auto" },
  heading: { fontSize: 22, fontWeight: 700, color: "var(--text)", marginBottom: 4 },
  sub:     { fontSize: 13, color: "var(--text-3)", marginBottom: 28 },
  grid2:   { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 },
  grid3:   { display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 },
  card:    {
    background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 12,
    padding: "16px 20px", marginBottom: 16,
  },
  cardHdr: {
    display: "flex", alignItems: "center", gap: 8,
    fontSize: 13, fontWeight: 700, color: "var(--text)", marginBottom: 14,
  },
  kv:      { display: "flex", flexDirection: "column", gap: 6 },
  kvRow:   {
    display: "flex", justifyContent: "space-between", alignItems: "center",
    fontSize: 12, color: "var(--text-2)", paddingBottom: 6,
    borderBottom: "1px solid var(--divider)",
  },
  kvVal:   { color: "var(--text)", fontWeight: 600 },
  badge:   (color) => ({
    display: "inline-block", padding: "2px 8px", borderRadius: 99,
    fontSize: 11, fontWeight: 700,
    background: `color-mix(in srgb, ${color} 13%, transparent)`, color: color,
  }),
  btn:     {
    padding: "8px 16px", background: "var(--accent)", color: "var(--accent-fg)",
    border: "none", borderRadius: 8, cursor: "pointer",
    fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
  },
  tag:     (color) => ({
    padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 700,
    background: `color-mix(in srgb, ${color} 13%, transparent)`, color: color, marginRight: 4,
  }),
  recRow:  {
    padding: "12px 0", borderBottom: "1px solid var(--divider)",
    display: "flex", flexDirection: "column", gap: 6,
  },
};

function Badge({ text, color = "var(--accent)" }) {
  return <span style={S.badge(color)}>{text}</span>;
}

function StatCard({ label, value, sub, color = "var(--accent)", Icon }) {
  return (
    <div style={S.card}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        {Icon && <Icon size={14} color={color} />}
        <span style={{ fontSize: 12, color: "var(--text-3)" }}>{label}</span>
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "var(--text-4)", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

export default function CostOptimizationTab() {
  const [rates, setRates]       = useState(null);
  const [result, setResult]     = useState(null);
  const [runId, setRunId]       = useState(null);
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

  // The Central Manager runs the Cost Optimization Agent on every run's real
  // plan, resource plan and performance prediction (with its duration
  // ceiling). Show the most recent run's result instead of a made-up plan.
  const loadLatest = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const runs = await manager.listRuns();
      let found = null;
      for (const r of (runs || []).slice(0, RUN_SCAN_LIMIT)) {
        const st = await manager.status(r.run_id).catch(() => null);
        if (st?.cost_optimization?.estimated_cost) { found = st; break; }
      }
      setResult(found ? found.cost_optimization : null);
      setRunId(found ? found.run_id : null);
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchRates(); loadLatest(); }, [fetchRates, loadLatest]);

  const costTotal = result?.estimated_cost?.total_usd;
  const recs      = result?.recommendations || [];
  const source    = result?.optimization_source || "—";

  return (
    <div style={S.page}>
      <div style={S.heading}>Cost Optimization Agent</div>
      <div style={S.sub}>
        Estimates pipeline dollar cost and recommends cost-saving configuration changes
        via ML model or heuristic fallback. Runs automatically for every Central Manager
        run; never accepts a change that makes the run much slower than this pipeline
        usually takes, or drops below minimum resources.
      </div>

      {/* Engine banner */}
      {result && (
        <div style={{ ...S.card, display: "flex", alignItems: "center", gap: 10, padding: "12px 20px" }}>
          <Cpu size={15} color={source === "ml_model" ? "var(--violet)" : "var(--warn)"} />
          <span style={{ fontSize: 13, color: "var(--text)", fontWeight: 600 }}>Optimization engine:</span>
          <Badge
            text={source === "ml_model" ? "ML model" : "Heuristic fallback"}
            color={source === "ml_model" ? "var(--violet)" : "var(--warn)"}
          />
          {result?.estimated_cost && (
            <span style={{ fontSize: 11, color: "var(--text-3)" }}>
              estimated {usd(costTotal)} total
              {runId && <> · run <span style={{ fontFamily: "monospace" }}>{runId.slice(0, 8)}</span></>}
            </span>
          )}
        </div>
      )}

      {/* Stats */}
      <div style={S.grid3}>
        <StatCard
          label="Estimated Cost"
          value={usd(costTotal)}
          sub="USD (compute + DBU + ADF + storage)"
          Icon={DollarSign}
          color="var(--ok)"
        />
        <StatCard
          label="Recommendations"
          value={recs.length}
          sub={recs.length > 0 ? `best saves ${recs[0]?.estimated_saving || "—"}` : result ? "no cheaper safe option found" : "no run yet"}
          Icon={TrendingDown}
          color={recs.length > 0 ? "var(--accent)" : "var(--text-4)"}
        />
        <StatCard
          label="Optimization source"
          value={source === "ml_model" ? "ML model" : source === "heuristic" ? "Heuristic" : "—"}
          sub={source === "ml_model" ? "HistGradientBoosting (trained)" : "rule-based fallback"}
          Icon={Zap}
          color={source === "ml_model" ? "var(--violet)" : "var(--warn)"}
        />
      </div>

      {err && (
        <div style={{ ...S.card, border: "1px solid var(--bad)", color: "var(--bad)", fontSize: 12 }}>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <AlertTriangle size={13} />{err}
          </div>
        </div>
      )}

      {/* Refresh */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
        <button style={S.btn} onClick={loadLatest} disabled={loading}>
          <RefreshCw size={13} />
          {loading ? "Loading…" : "Refresh latest run"}
        </button>
      </div>

      {!loading && !result && !err && (
        <div style={{ ...S.card, textAlign: "center", color: "var(--text-4)", fontSize: 13, padding: "32px 20px" }}>
          No cost optimization yet — run a pipeline through the Central Manager; its
          cost analysis will appear here.
        </div>
      )}

      {result?.streaming_advice && (
        <div style={{ ...S.card, fontSize: 12, color: "var(--accent)" }}>{result.streaming_advice}</div>
      )}

      {/* Cost breakdown */}
      {result?.estimated_cost && (
        <div style={S.card}>
          <div style={S.cardHdr}>
            <DollarSign size={14} color="var(--ok)" />
            Cost Breakdown
          </div>
          <div style={S.kv}>
            <div style={S.kvRow}>
              <span>Compute (VM nodes)</span>
              <span style={S.kvVal}>{usd(result.estimated_cost.compute_usd)}</span>
            </div>
            <div style={S.kvRow}>
              <span>Databricks DBU</span>
              <span style={S.kvVal}>{usd(result.estimated_cost.databricks_dbu_usd)}</span>
            </div>
            <div style={S.kvRow}>
              <span>ADF activity</span>
              <span style={S.kvVal}>{usd(result.estimated_cost.adf_usd)}</span>
            </div>
            <div style={S.kvRow}>
              <span>Storage</span>
              <span style={S.kvVal}>{usd(result.estimated_cost.storage_usd)}</span>
            </div>
            <div style={{ ...S.kvRow, borderBottom: "none", fontSize: 14 }}>
              <span style={{ fontWeight: 700, color: "var(--text)" }}>Total</span>
              <span style={{ ...S.kvVal, color: "var(--ok)", fontSize: 16 }}>
                {usd(result.estimated_cost.total_usd)}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Recommendations */}
      {recs.length > 0 && (
        <div style={S.card}>
          <div style={S.cardHdr}>
            <TrendingDown size={14} color="var(--warn)" />
            Optimization Recommendations
            <span style={{ marginLeft: "auto" }}>
              <Badge text={recs.length.toString()} color="var(--accent)" />
            </span>
          </div>
          {recs.map((r, i) => {
            const riskColor = r.risk_level === "low" ? "var(--ok)"
                           : r.risk_level === "medium" ? "var(--warn)"
                           : "var(--bad)";
            const sourceColor = r.source === "ml" ? "var(--violet)" : "var(--accent)";
            return (
              <div key={i} style={S.recRow}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", flex: 1 }}>
                    {r.change}
                  </span>
                  <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                    <Badge text={`save ${r.estimated_saving}`} color="var(--ok)" />
                    <Badge text={r.risk_level} color={riskColor} />
                    <Badge text={r.source === "ml" ? "ML" : "rule"} color={sourceColor} />
                  </div>
                </div>
                <div style={{ fontSize: 11, color: "var(--text-2)" }}>{r.trade_off}</div>
                <div style={{ fontSize: 11, color: "var(--text-3)" }}>{r.reason}</div>
                {r.new_cost && (
                  <div style={{ fontSize: 11, color: "var(--text-4)", marginTop: 2 }}>
                    new cost: {usd(r.new_cost.total_usd)}
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
            <BarChart3 size={14} color="var(--text-3)" />
            Node Hourly Rates (Cost Model)
          </div>
          <div style={S.kv}>
            {Object.entries(rates.node_hourly_rates).map(([node, rate]) => (
              <div key={node} style={S.kvRow}>
                <span>{node}</span>
                <span style={S.kvVal}>{typeof rate === "number" ? `$${rate.toFixed(2)}/hr` : String(rate)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Assumptions */}
      <div style={S.card}>
        <div style={S.cardHdr}>
          <CheckCircle size={14} color="var(--text-3)" />
          Cost Model Assumptions
        </div>
        {/* Straight from the backend cost model (GET /cost-optimization/node-rates) */}
        {rates?.assumptions ? (
          <div style={S.kv}>
            {Object.entries(rates.assumptions)
              .filter(([k]) => k !== "node_hourly_rates" && k !== "note")
              .map(([k, v]) => (
                <div key={k} style={S.kvRow}>
                  <span>{k.replace(/_/g, " ")}</span>
                  <span style={S.kvVal}>{String(v)}</span>
                </div>
              ))}
            {rates.assumptions.note && (
              <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 4 }}>{rates.assumptions.note}</div>
            )}
          </div>
        ) : (
          <div style={{ fontSize: 11, color: "var(--text-3)" }}>All costs are estimates.</div>
        )}
      </div>
    </div>
  );
}
