import React, { useState, useEffect, useCallback } from "react";
import { BarChart3, CircleDollarSign, Cpu, Info, RefreshCw, TrendingDown } from "lucide-react";
import { cost, manager } from "../api.js";
import { Alert, Badge, Button, Card, Empty, KV, PageHeader, Stat } from "../ui/components.jsx";

const usd = (v) => (typeof v === "number" ? `$${v.toFixed(4)}` : "—");
const RISK_TONE = { low: "ok", medium: "warn", high: "bad" };

// How many recent runs to look through for one that reached cost optimization
// (runs that fail validation or pre-checks never get that far).
const RUN_SCAN_LIMIT = 10;

export default function CostOptimizationTab() {
  const [rates, setRates]     = useState(null);
  const [result, setResult]   = useState(null);
  const [runId, setRunId]     = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr]         = useState("");

  const fetchRates = useCallback(async () => {
    try {
      setRates(await cost.nodeRates());
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

  const est = result?.estimated_cost;
  const recs = result?.recommendations || [];
  const source = result?.optimization_source;
  const assumptions = Object.entries(rates?.assumptions || {}).filter(([k]) => k !== "node_hourly_rates" && k !== "note");

  return (
    <div>
      <PageHeader
        eyebrow="Agents" icon={CircleDollarSign}
        title="Cost agent"
        description="Estimates what a run costs on Azure and suggests cheaper settings. It runs on every Central Manager run and never accepts a change that makes the run much slower than this pipeline usually takes."
        actions={<Button size="sm" icon={RefreshCw} loading={loading} onClick={loadLatest}>Refresh</Button>}
      />
      {err && <Alert tone="bad" style={{ marginBottom: 14 }}>{err}</Alert>}

      {!loading && !result && !err ? (
        <Card style={{ marginBottom: 14 }}>
          <Empty icon={CircleDollarSign} title="No cost analysis yet">
            Run a pipeline through the Central Manager — its cost analysis appears here.
          </Empty>
        </Card>
      ) : result && (
        <>
          <div className="grid grid-3" style={{ marginBottom: 14 }}>
            <Stat icon={CircleDollarSign} label="Estimated cost" value={usd(est?.total_usd)}
              sub={runId ? <>run <span className="mono">{runId.slice(0, 8)}</span></> : "USD"} />
            <Stat icon={TrendingDown} label="Cheaper options" value={recs.length}
              tone={recs.length ? "ok" : undefined}
              sub={recs.length ? `best saves ${recs[0]?.estimated_saving || "—"}` : "no safe saving found"} />
            <Stat icon={Cpu} label="Engine" value={source === "ml_model" ? "ML model" : source === "heuristic" ? "Heuristic" : "—"}
              tone={source === "ml_model" ? "violet" : undefined}
              sub={source === "ml_model" ? "trained model" : "rule-based fallback"} />
          </div>

          {result.streaming_advice && <Alert tone="accent" style={{ marginBottom: 14 }}>{result.streaming_advice}</Alert>}

          <div className="grid grid-2" style={{ marginBottom: 14, alignItems: "start" }}>
            <Card title="Cost breakdown" icon={BarChart3}>
              <KV items={[
                ["Compute (VM nodes)", usd(est?.compute_usd)],
                ["Databricks DBU", usd(est?.databricks_dbu_usd)],
                ["ADF activity", usd(est?.adf_usd)],
                ["Storage", usd(est?.storage_usd)],
                result.cost_correction_applied ? ["Learned correction", `×${result.cost_correction_applied}`] : null,
                ["Total", <b key="t" style={{ color: "var(--text)" }}>{usd(est?.total_usd)}</b>],
              ]} />
            </Card>

            <Card title="Recommendations" icon={TrendingDown}>
              {recs.length === 0 ? (
                <div className="muted">The current settings are already the cheapest safe option.</div>
              ) : (
                <div className="list">
                  {recs.map((r, i) => (
                    <div key={i} className="list-row" style={{ alignItems: "flex-start", flexDirection: "column", gap: 4 }}>
                      <div className="row" style={{ gap: 6, width: "100%" }}>
                        <span style={{ color: "var(--text)", fontWeight: 500, flex: 1 }}>{r.change}</span>
                        <Badge tone="ok">save {r.estimated_saving}</Badge>
                        <Badge tone={RISK_TONE[r.risk_level] || "neutral"}>{r.risk_level} risk</Badge>
                      </div>
                      <div className="muted" style={{ fontSize: 12.5 }}>{r.reason}</div>
                      <div className="faint" style={{ fontSize: 12 }}>
                        {r.trade_off}{r.new_cost ? ` · new total ${usd(r.new_cost.total_usd)}` : ""} · {r.source === "ml" ? "ML" : "rule"}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </Card>
          </div>
        </>
      )}

      <div className="grid grid-2" style={{ alignItems: "start" }}>
        <Card title="Node hourly rates" icon={Cpu} subtitle="Used by the cost model">
          {rates?.node_hourly_rates ? (
            <KV items={Object.entries(rates.node_hourly_rates).map(([node, rate]) => [
              <span key={node} className="mono">{node}</span>,
              typeof rate === "number" ? `$${rate.toFixed(2)}/hr` : String(rate),
            ])} />
          ) : <div className="muted">Unavailable.</div>}
        </Card>
        <Card title="Cost model assumptions" icon={Info} subtitle="Straight from the backend">
          {assumptions.length ? (
            <>
              <KV items={assumptions.map(([k, v]) => [k.replaceAll("_", " "), String(v)])} />
              {rates.assumptions.note && <div className="faint" style={{ fontSize: 12, marginTop: 10 }}>{rates.assumptions.note}</div>}
            </>
          ) : <div className="muted">All costs are estimates.</div>}
        </Card>
      </div>
    </div>
  );
}
