"""ADF copy-activity metrics for the saved live runs (read-only, no new runs).

For every saved run state with an ADF run id, asks Azure Data Factory for the
copy activity's own numbers (queue time, transfer time, bytes, throughput,
DIUs actually used) and pairs them with the DIU the Resource Agent requested
and its copy-duration estimate. Used to recalibrate ADF_MB_PER_DIU_PER_S
(resource_agent.py), which was a rough 5 MB/s per DIU.

    python scripts/paper_eval/adf_copy_metrics.py out.json [states_glob]

ADF keeps run history for 45 days, so this only works for recent runs.
"""
import asyncio
import glob
import json
import os
import statistics as st
import sys
from datetime import datetime, timedelta, timezone

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

import httpx  # noqa: E402

from monitor_agent.services.adf_service import ADFService  # noqa: E402


async def activity_runs(svc, run_id, started):
    t0 = datetime.fromisoformat(started.replace("Z", "+00:00"))
    if t0.tzinfo is None:
        t0 = t0.replace(tzinfo=timezone.utc)
    body = {"lastUpdatedAfter": (t0 - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lastUpdatedBefore": (t0 + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")}
    async with httpx.AsyncClient() as client:
        r = await client.post(svc._factory_url(f"/pipelineRuns/{run_id}/queryActivityruns"),
                              headers=await svc._headers(), json=body, timeout=30)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    return r.json().get("value", []), None


def copy_row(d, act):
    out = act.get("output") or {}
    det = (out.get("executionDetails") or [{}])[0]
    dur = det.get("detailedDurations") or {}
    rp = (d.get("resource_plan") or {}).get("allocations") or []
    copy_alloc = next((a for a in rp if a.get("stage_type") == "copy"), {})
    return {
        "run": d["run_id"][:8], "started_at": d.get("started_at"),
        "csv_mb": round((d.get("csv_size_bytes") or 0) / 1e6, 3),
        "requested_diu": copy_alloc.get("diu"),
        "resource_estimate_s": copy_alloc.get("duration_s"),
        "used_diu": out.get("usedDataIntegrationUnits"),
        "activity_s": round((act.get("durationInMs") or 0) / 1000, 1),
        "copy_duration_s": out.get("copyDuration"),
        "queue_s": dur.get("queuingDuration"),
        "transfer_s": dur.get("transferDuration"),
        "bytes_read": out.get("dataRead"),
        "throughput_kbps": out.get("throughput"),
        "status": act.get("status"),
    }


async def main():
    out_path = os.path.join(_CALLER_CWD, sys.argv[1])
    pattern = sys.argv[2] if len(sys.argv) > 2 else "data/paper_eval/live*/states/*.json"
    svc = ADFService()
    rows, missing = [], []
    for f in sorted(glob.glob(pattern)):
        d = json.load(open(f))
        adf = (d.get("executor_result") or {}).get("adf_run_id")
        if not adf or not d.get("started_at"):
            continue
        acts, err = await activity_runs(svc, adf, d["started_at"])
        copies = [a for a in (acts or []) if a.get("activityType") == "Copy"]
        if not copies:
            missing.append({"run": d["run_id"][:8], "why": err or "no copy activity returned"})
            continue
        rows.extend(copy_row(d, a) for a in copies)

    ok = [r for r in rows if r["status"] == "Succeeded" and r["copy_duration_s"]]
    by_diu = {}
    for r in ok:
        by_diu.setdefault(r["used_diu"], []).append(r)
    summary = {
        "copy_activities": len(rows), "succeeded": len(ok), "not_found": len(missing),
        "by_used_diu": {str(k): {"n": len(v),
                                 "mb_median": round(st.median(x["csv_mb"] for x in v), 2),
                                 "activity_s_median": st.median(x["activity_s"] for x in v),
                                 "queue_s_median": st.median(x["queue_s"] or 0 for x in v),
                                 "transfer_s_median": st.median(x["transfer_s"] or 0 for x in v)}
                        for k, v in sorted(by_diu.items(), key=lambda kv: kv[0] or 0)},
    }
    json.dump({"summary": summary, "rows": rows, "missing": missing}, open(out_path, "w"), indent=1)
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
