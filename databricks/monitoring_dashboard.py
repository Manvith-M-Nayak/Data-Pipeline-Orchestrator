"""
monitoring_dashboard.py
=======================
Standalone Streamlit page for the Monitoring Agent.
Can be run as its own app:
    streamlit run monitoring_dashboard.py

Or integrated into databricks_dashboard.py by importing render_monitoring_page()
and calling it from stage_monitor().
"""

import streamlit as st
import sys
import os
import time
import json
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from monitoring_agent import monitoring_agent
from monitoring_integration import get_dashboard_monitor_data

# ── Ensure agent is running ────────────────────────────────────────────────────
if not monitoring_agent._running:
    monitoring_agent.start()


# ══════════════════════════════════════════════════════════════════════════════
# CSS (matches databricks_dashboard.py style)
# ══════════════════════════════════════════════════════════════════════════════

MONITOR_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');

html, body, [data-testid="stAppViewContainer"], .main {
    background: #f8fafc !important; color: #1e293b;
    font-family: 'IBM Plex Sans', sans-serif;
}
[data-testid="stHeader"], [data-testid="stToolbar"], #MainMenu, footer { display: none !important; }

.block-container { padding: 2rem 3rem 4rem !important; max-width: 1400px; }

/* Monitor grid */
.mon-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.9rem; margin-bottom: 1.5rem; }
.mon-card {
    background: #ffffff; border: 1px solid #e2e8f0;
    border-radius: 12px; padding: 1.1rem 1.3rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}
.mon-card-label {
    font-family: 'IBM Plex Mono', monospace; font-size: 0.58rem;
    letter-spacing: 0.14em; text-transform: uppercase; color: #64748b;
    margin-bottom: 0.4rem;
}
.mon-card-value { font-family: 'IBM Plex Mono', monospace; font-size: 1.5rem; font-weight: 700; color: #0f172a; }
.mon-card-sub { font-size: 0.65rem; color: #64748b; margin-top: 0.2rem; }
.mon-card-ok   { border-left: 4px solid #86efac !important; }
.mon-card-warn { border-left: 4px solid #fcd34d !important; }
.mon-card-err  { border-left: 4px solid #fca5a5 !important; }
.mon-card-blue { border-left: 4px solid #93c5fd !important; }

/* Section headers */
.sec-label {
    font-family: 'IBM Plex Mono', monospace; font-size: 0.62rem;
    font-weight: 600; letter-spacing: 0.18em; text-transform: uppercase;
    color: #e25a1c; margin: 1.2rem 0 0.7rem;
}

/* Alert rows */
.alert-row {
    background: #fff7ed; border: 1px solid #fdba74; border-left: 4px solid #ea580c;
    border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 0.5rem;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.73rem;
}
.alert-row.critical {
    background: #fee2e2; border-color: #fca5a5; border-left-color: #dc2626;
}
.alert-row-header { display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap; }
.alert-tag {
    display: inline-block; padding: 0.12rem 0.5rem; border-radius: 4px;
    font-size: 0.62rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em;
}
.tag-critical { background: #fee2e2; color: #dc2626; }
.tag-warning  { background: #fff7ed; color: #ea580c; }
.tag-type     { background: #e0e7ff; color: #6366f1; }
.alert-desc   { color: #475569; margin-top: 0.35rem; font-size: 0.70rem; }
.alert-action { color: #0891b2; font-size: 0.64rem; margin-top: 0.2rem; }

/* Event rows */
.event-row {
    border-bottom: 1px solid #f1f5f9; padding: 0.4rem 0;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.70rem; color: #475569;
}
.event-type { color: #0f172a; font-weight: 600; }
.event-ts   { color: #94a3b8; font-size: 0.63rem; }

/* Resource bar */
.res-bar-bg { background: #e2e8f0; border-radius: 4px; height: 8px; width: 100%; margin-top: 0.3rem; }
.res-bar-fill { height: 8px; border-radius: 4px; transition: width 0.3s; }

/* Pipeline table */
.pl-table { width: 100%; border-collapse: collapse; font-family: 'IBM Plex Mono', monospace; font-size: 0.73rem; }
.pl-table th {
    text-align: left; padding: 0.5rem 0.8rem; border-bottom: 2px solid #e2e8f0;
    font-size: 0.60rem; letter-spacing: 0.12em; text-transform: uppercase; color: #64748b;
}
.pl-table td { padding: 0.5rem 0.8rem; border-bottom: 1px solid #f1f5f9; color: #475569; }
.pl-table tr:hover td { background: #f8fafc; }

/* Pill */
.pill { display: inline-block; padding: 0.14rem 0.5rem; border-radius: 4px; font-size: 0.62rem; font-weight: 600; }
.pill-running   { background: #fff7ed; color: #ea580c; }
.pill-completed { background: #dcfce7; color: #16a34a; }
.pill-failed    { background: #fee2e2; color: #dc2626; }

/* Feedback loop diagram */
.feedback-loop {
    display: flex; align-items: center; justify-content: center; gap: 0.5rem;
    padding: 1rem 1.5rem; background: #f8fafc; border: 1px solid #e2e8f0;
    border-radius: 10px; margin-bottom: 1.2rem; flex-wrap: wrap;
}
.loop-node {
    background: #1e293b; color: #f8fafc; border-radius: 8px;
    padding: 0.5rem 1rem; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.70rem; font-weight: 600; letter-spacing: 0.05em;
}
.loop-node.active { background: #e25a1c; }
.loop-arrow { color: #94a3b8; font-size: 1rem; }
</style>
"""


# ══════════════════════════════════════════════════════════════════════════════
# RENDER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _bar(value: float, max_val: float = 100, color: str = "#3b82f6") -> str:
    pct = min(max(value / max_val * 100, 0), 100)
    return (
        f'<div class="res-bar-bg">'
        f'<div class="res-bar-fill" style="width:{pct:.1f}%;background:{color};"></div>'
        f'</div>'
    )


def _pill(status: str) -> str:
    cls = f"pill-{status.lower()}"
    return f'<span class="pill {cls}">{status}</span>'


def render_monitoring_page():
    """Main render function. Call from dashboard or standalone."""
    st.markdown(MONITOR_CSS, unsafe_allow_html=True)

    # ── Page header ──
    st.markdown("""
    <div style="display:flex;align-items:center;gap:0.8rem;padding:1rem 0 0.5rem;border-bottom:1px solid #e2e8f0;margin-bottom:1.5rem;">
        <div style="width:32px;height:32px;background:linear-gradient(135deg,#0891b2,#0284c7);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:1rem;">📡</div>
        <div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:0.60rem;letter-spacing:0.2em;text-transform:uppercase;color:#64748b;">Databricks Pipeline Orchestrator</div>
            <div style="font-size:1.3rem;font-weight:700;letter-spacing:-0.02em;color:#0f172a;">Monitoring Agent</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Auto-refresh control ──
    col_ref, col_int, _ = st.columns([1, 1, 4])
    with col_ref:
        auto_refresh = st.checkbox("Auto-refresh", value=False)
    with col_int:
        refresh_interval = st.selectbox("Interval", [5, 10, 30, 60], index=1, label_visibility="collapsed")

    if st.button("↻  Refresh Now", use_container_width=False):
        st.rerun()

    # ── Pull live data ──
    data = get_dashboard_monitor_data()
    metrics = data["metrics"]
    alerts  = data["alerts"]
    anomalies = data["anomalies"]
    cards   = data["cards"]

    # ── Feedback loop diagram ──
    st.markdown("""
    <div class="feedback-loop">
        <div class="loop-node active">⚡ Execution</div>
        <div class="loop-arrow">→</div>
        <div class="loop-node active">📡 Monitoring</div>
        <div class="loop-arrow">→</div>
        <div class="loop-node">🔍 Analysis</div>
        <div class="loop-arrow">→</div>
        <div class="loop-node">⚙️ Optimization</div>
        <div class="loop-arrow">→</div>
        <div class="loop-node">🔁 Re-execution</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Summary metric cards ──
    st.markdown('<div class="mon-grid">', unsafe_allow_html=True)
    for card in cards:
        st.markdown(f"""
        <div class="mon-card {card['class']}">
            <div class="mon-card-label">{card['label']}</div>
            <div class="mon-card-value">{card['value']}</div>
            <div class="mon-card-sub">{card['sub']}</div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Tabs ──
    tab_overview, tab_pipelines, tab_anomalies, tab_events, tab_agents = st.tabs([
        "📊 Resource Overview",
        "🔄 Pipelines & Jobs",
        "🚨 Anomalies & Alerts",
        "📋 Event Log",
        "🤝 Agent Interfaces",
    ])

    # ──────────────────────────────────────────────────────────────────────────
    with tab_overview:
        _render_resource_overview(metrics)

    # ──────────────────────────────────────────────────────────────────────────
    with tab_pipelines:
        _render_pipelines_and_jobs(metrics)

    # ──────────────────────────────────────────────────────────────────────────
    with tab_anomalies:
        _render_anomalies_and_alerts(anomalies, alerts)

    # ──────────────────────────────────────────────────────────────────────────
    with tab_events:
        _render_events()

    # ──────────────────────────────────────────────────────────────────────────
    with tab_agents:
        _render_agent_interfaces()

    # ── Auto-refresh ──
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
def _render_resource_overview(metrics: dict):
    st.markdown('<div class="sec-label">Real-time Resource Utilization</div>', unsafe_allow_html=True)

    res = metrics.get("current_resource", {})
    sys_m = metrics.get("system", {})

    cpu   = res.get("cpu_percent", 0)
    mem   = res.get("memory_percent", 0)
    mem_mb = res.get("memory_mb", 0)

    c1, c2 = st.columns(2)
    with c1:
        cpu_color = "#dc2626" if cpu > 80 else ("#d97706" if cpu > 60 else "#16a34a")
        st.markdown(f"""
        <div class="mon-card">
            <div class="mon-card-label">CPU Usage</div>
            <div class="mon-card-value" style="color:{cpu_color};">{cpu:.1f}%</div>
            {_bar(cpu, 100, cpu_color)}
            <div class="mon-card-sub" style="margin-top:0.4rem;">Avg (5m): {sys_m.get('cluster_cpu_avg_pct', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        mem_color = "#dc2626" if mem > 85 else ("#d97706" if mem > 70 else "#16a34a")
        st.markdown(f"""
        <div class="mon-card">
            <div class="mon-card-label">Memory Usage</div>
            <div class="mon-card-value" style="color:{mem_color};">{mem:.1f}%</div>
            {_bar(mem, 100, mem_color)}
            <div class="mon-card-sub" style="margin-top:0.4rem;">{mem_mb:.0f} MB used | Avg (5m): {sys_m.get('cluster_memory_avg_pct', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)

    c3, c4, c5, c6 = st.columns(4)
    with c3:
        st.metric("Disk Read", f"{res.get('disk_read_mb', 0):.2f} MB")
    with c4:
        st.metric("Disk Write", f"{res.get('disk_write_mb', 0):.2f} MB")
    with c5:
        st.metric("Net Sent", f"{res.get('net_sent_mb', 0):.3f} MB")
    with c6:
        st.metric("Net Recv", f"{res.get('net_recv_mb', 0):.3f} MB")

    st.markdown('<div class="sec-label">System Counters</div>', unsafe_allow_html=True)
    c7, c8, c9 = st.columns(3)
    with c7:
        st.metric("Active Threads", sys_m.get("active_threads", 0))
    with c8:
        st.metric("Sched. Latency Avg", f"{sys_m.get('scheduling_latency_avg_s', 0):.2f}s")
    with c9:
        st.metric("Total Jobs Tracked", sys_m.get("number_of_jobs", 0))

    # Resource history chart
    history = monitoring_agent.get_resource_history(last_n=60)
    if len(history) >= 2:
        st.markdown('<div class="sec-label">Resource History (Last 5 min)</div>', unsafe_allow_html=True)
        import pandas as pd
        df = pd.DataFrame([
            {"time": h["timestamp"][-8:-1], "CPU %": h["cpu_percent"], "Memory %": h["memory_percent"]}
            for h in history
        ])
        st.line_chart(df.set_index("time"))


# ──────────────────────────────────────────────────────────────────────────────
def _render_pipelines_and_jobs(metrics: dict):
    st.markdown('<div class="sec-label">Pipeline Status</div>', unsafe_allow_html=True)

    pipelines = metrics.get("pipelines", {})
    if not pipelines:
        st.info("No pipelines tracked yet. Run a pipeline to see metrics here.")
    else:
        rows = []
        for p in pipelines.values():
            rows.append(f"""
            <tr>
                <td>{p['pipeline_name']}</td>
                <td>{_pill(p['status'])}</td>
                <td>{p.get('start_time', 'N/A')}</td>
                <td>{f"{p['total_duration_s']:.1f}s" if p.get('total_duration_s') else 'Running...'}</td>
                <td>{p.get('total_jobs', 0)}</td>
                <td>{f"{p['success_rate']:.0f}%" if p.get('success_rate') is not None else 'N/A'}</td>
                <td>{f"{p['throughput_rps']:.1f}" if p.get('throughput_rps') else 'N/A'}</td>
            </tr>
            """)
        st.markdown(f"""
        <table class="pl-table">
            <thead><tr>
                <th>Pipeline</th><th>Status</th><th>Started</th>
                <th>Duration</th><th>Jobs</th><th>Success Rate</th><th>Throughput (r/s)</th>
            </tr></thead>
            <tbody>{''.join(rows)}</tbody>
        </table>
        """, unsafe_allow_html=True)

    st.markdown('<div class="sec-label">Job Metrics</div>', unsafe_allow_html=True)

    jobs = metrics.get("jobs", {})
    if not jobs:
        st.info("No job runs tracked yet.")
    else:
        rows = []
        for j in list(jobs.values())[-20:]:  # last 20
            rows.append(f"""
            <tr>
                <td style="color:#0f172a;font-weight:600;">{j['job_name']}</td>
                <td>{_pill(j['status'])}</td>
                <td>{f"{j['execution_time_s']:.1f}s" if j.get('execution_time_s') else 'Running...'}</td>
                <td>{f"{j['queue_time_s']:.1f}s" if j.get('queue_time_s') else '0s'}</td>
                <td>{j.get('retry_count', 0)}</td>
                <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{j.get('failure_reason') or '—'}</td>
            </tr>
            """)
        st.markdown(f"""
        <table class="pl-table">
            <thead><tr>
                <th>Job Name</th><th>Status</th><th>Exec Time</th>
                <th>Queue Time</th><th>Retries</th><th>Failure Reason</th>
            </tr></thead>
            <tbody>{''.join(rows)}</tbody>
        </table>
        """, unsafe_allow_html=True)

    # Cost section
    costs = metrics.get("costs", {})
    if costs:
        st.markdown('<div class="sec-label">Cost Metrics</div>', unsafe_allow_html=True)
        total = sum(c.get("cost_per_pipeline", 0) for c in costs.values())
        idle  = sum(c.get("idle_resource_cost", 0) for c in costs.values())
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Total Pipeline Cost", f"${total:.4f}")
        with c2:
            st.metric("Idle Resource Waste", f"${idle:.4f}")
        with c3:
            waste_pct = (idle / total * 100) if total > 0 else 0
            st.metric("Waste %", f"{waste_pct:.1f}%")


# ──────────────────────────────────────────────────────────────────────────────
def _render_anomalies_and_alerts(anomalies: list, alerts: list):
    # Active alerts
    if alerts:
        st.markdown(f'<div class="sec-label">⚠️ Active Alerts ({len(alerts)})</div>', unsafe_allow_html=True)
        for alert in alerts:
            sev_class = "critical" if alert["severity"] == "critical" else ""
            st.markdown(f"""
            <div class="alert-row {sev_class}">
                <div class="alert-row-header">
                    <span class="alert-tag tag-{alert['severity']}">{alert['severity'].upper()}</span>
                    <span class="alert-tag tag-type">{alert['type']}</span>
                    <span style="color:#94a3b8;font-size:0.63rem;">{alert['timestamp']}</span>
                </div>
                <div class="alert-desc">{alert['description']}</div>
                <div class="alert-action">→ Suggested action: <strong>{alert['action']}</strong></div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("No active alerts. System is healthy.")

    # Anomaly history
    st.markdown('<div class="sec-label">Anomaly History</div>', unsafe_allow_html=True)

    sev_filter = st.selectbox("Filter by severity", ["All", "critical", "warning"], index=0)
    all_anomalies = monitoring_agent.get_anomalies(
        severity=None if sev_filter == "All" else sev_filter,
        limit=50,
    )

    if not all_anomalies:
        st.info("No anomalies detected. Keep it up! 🟢")
    else:
        for a in reversed(all_anomalies):
            sev_class = "critical" if a["severity"] == "critical" else ""
            st.markdown(f"""
            <div class="alert-row {sev_class}" style="margin-bottom:0.4rem;">
                <div class="alert-row-header">
                    <span class="alert-tag tag-{a['severity']}">{a['severity'].upper()}</span>
                    <span class="alert-tag tag-type">{a['anomaly_type']}</span>
                    <span style="color:#94a3b8;font-size:0.63rem;">{a['timestamp']}</span>
                    {"<span style='color:#64748b;font-size:0.62rem;'>pipeline: " + a['pipeline_id'] + "</span>" if a.get('pipeline_id') else ""}
                </div>
                <div class="alert-desc">{a['description']}</div>
            </div>
            """, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
def _render_events():
    st.markdown('<div class="sec-label">Event Bus (Latest 50)</div>', unsafe_allow_html=True)

    events = monitoring_agent.get_recent_events(limit=50)
    if not events:
        st.info("No events recorded yet.")
        return

    event_type_colors = {
        "pipeline_started":    "#16a34a",
        "pipeline_completed":  "#16a34a",
        "pipeline_failed":     "#dc2626",
        "job_started":         "#0891b2",
        "job_succeeded":       "#16a34a",  # Note: also "job_succeeded" from agent
        "job_failed":          "#dc2626",
        "anomaly_detected":    "#d97706",
        "resource_scaled_up":  "#7c3aed",
        "resource_scaled_down":"#7c3aed",
        "log_line":            "#94a3b8",
    }

    rows = []
    for e in reversed(events):
        color = event_type_colors.get(e["event_type"], "#64748b")
        meta_str = json.dumps(e.get("metadata", {}), default=str)[:80]
        rows.append(f"""
        <div class="event-row">
            <span class="event-type" style="color:{color};">{e['event_type']}</span>
            <span style="color:#94a3b8;"> | </span>
            <span class="event-ts">{e['timestamp']}</span>
            {"<span style='color:#64748b;'> | pipeline: " + e['pipeline_id'] + "</span>" if e.get('pipeline_id') else ""}
            {"<span style='color:#94a3b8;font-size:0.62rem;'> | " + meta_str + "</span>" if meta_str != '{}' else ""}
        </div>
        """)
    st.markdown(
        f'<div style="font-family:\'IBM Plex Mono\',monospace;font-size:0.72rem;background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:1rem;max-height:450px;overflow-y:auto;">{"".join(rows)}</div>',
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
def _render_agent_interfaces():
    st.markdown('<div class="sec-label">Data Feeds to Other Agents</div>', unsafe_allow_html=True)

    tab_planner, tab_optimizer, tab_executor = st.tabs([
        "🗓️ Planner Context",
        "⚙️ Optimizer Context",
        "⚡ Executor Alerts",
    ])

    with tab_planner:
        st.markdown("Data the Monitoring Agent sends to the **Planner Agent** for intelligent pipeline planning.")
        ctx = monitoring_agent.get_planner_context()
        st.json(ctx)

    with tab_optimizer:
        st.markdown("Data the Monitoring Agent sends to the **Optimizer Agent** for cost + performance optimization.")
        ctx = monitoring_agent.get_optimizer_context()
        hints = ctx.get("optimization_hints", [])
        if hints:
            st.markdown('<div class="sec-label">Optimization Hints</div>', unsafe_allow_html=True)
            for h in hints:
                st.markdown(f"""
                <div class="alert-row" style="background:#f0fdf4;border-color:#86efac;border-left-color:#16a34a;">
                    <div class="alert-row-header">
                        <span class="alert-tag" style="background:#dcfce7;color:#16a34a;">{h['hint']}</span>
                    </div>
                    <div class="alert-desc">{h['reason']}</div>
                </div>
                """, unsafe_allow_html=True)
        st.json(ctx)

    with tab_executor:
        st.markdown("Real-time actionable alerts the Monitoring Agent sends to the **Execution Agent**.")
        alerts = monitoring_agent.get_executor_alerts()
        if alerts:
            for alert in alerts:
                st.markdown(f"""
                <div class="alert-row {'critical' if alert['severity'] == 'critical' else ''}">
                    <div class="alert-row-header">
                        <span class="alert-tag tag-{alert['severity']}">{alert['severity'].upper()}</span>
                        <span class="alert-tag tag-type">{alert['type']}</span>
                    </div>
                    <div class="alert-desc">{alert['description']}</div>
                    <div class="alert-action">→ Action: <strong>{alert['action']}</strong></div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.success("No executor alerts. All systems normal.")
        st.json(alerts)


# ══════════════════════════════════════════════════════════════════════════════
# STANDALONE ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    st.set_page_config(
        page_title="Monitoring Agent",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    render_monitoring_page()