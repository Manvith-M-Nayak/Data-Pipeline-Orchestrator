import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Set, Any

from fastapi import WebSocket

from .adf_service  import ADFService
from .db_service   import DBService
from .groq_service import GroqService

POLL_INTERVAL_SEC    = 20
ANALYSIS_CONCURRENCY = 3


class MonitorService:
    def __init__(self, adf: ADFService, db: DBService, groq: GroqService):
        self._adf     = adf
        self._db      = db
        self._groq    = groq
        self._tracked: Dict[str, Dict] = {}
        # run_id -> verdict for runs already logged as anomalous — prevents
        # re-logging (and re-calling Groq) on every 20s poll while the run
        # remains slow, and keeps the warning visible in live updates
        self._anomaly_verdicts: Dict[str, str] = {}
        self._sem     = asyncio.Semaphore(ANALYSIS_CONCURRENCY)
        self.ws_clients: Set[WebSocket] = set()

    async def _broadcast(self, payload: Dict[str, Any]):
        dead = set()
        for ws in self.ws_clients:
            try:
                await ws.send_text(json.dumps(payload))
            except Exception:
                dead.add(ws)
        self.ws_clients -= dead

    async def start_polling(self):
        while True:
            try:
                await self._poll_cycle()
            except Exception as exc:
                await self._broadcast({"event": "error", "message": str(exc)})
            await asyncio.sleep(POLL_INTERVAL_SEC)

    async def _poll_cycle(self):
        live_runs = await self._adf.get_active_pipeline_runs()
        live_ids  = {r["runId"] for r in live_runs}

        for run_id in set(self._tracked) - live_ids:
            self._anomaly_verdicts.pop(run_id, None)
            asyncio.create_task(self._handle_completed_run(self._tracked.pop(run_id)))

        for run in live_runs:
            run_id = run["runId"]
            self._tracked[run_id] = run
            await self._db.upsert_run(run)

            if run_id in self._anomaly_verdicts:
                continue
            stats = await self._db.get_historical_stats(run.get("pipelineName", ""))
            if stats["count"] >= 3 and run.get("runStart"):
                elapsed = self._elapsed(run["runStart"])
                if elapsed > stats["p95"] * 1.2:
                    verdict = await self._groq.detect_anomaly(run["pipelineName"], elapsed, stats)
                    if verdict.get("is_anomaly"):
                        self._anomaly_verdicts[run_id] = verdict.get("verdict", "")
                        await self._db.log_anomaly(
                            run_id, run["pipelineName"], elapsed,
                            stats["avg"], stats["p95"], verdict.get("verdict", ""),
                        )

        await self._broadcast({
            "event": "live_update",
            "runs": [
                {
                    "runId":        r.get("runId"),
                    "pipelineName": r.get("pipelineName"),
                    "status":       r.get("status"),
                    "runStart":     r.get("runStart"),
                    "elapsedSec":   self._elapsed(r.get("runStart", "")),
                    "anomaly":      self._anomaly_verdicts.get(r["runId"]),
                }
                for r in live_runs
            ],
        })

    async def _handle_completed_run(self, run: Dict):
        run_id, pipeline_name = run.get("runId"), run.get("pipelineName", "")
        try:
            final     = await self._adf.get_pipeline_run(run_id)
            activities = await self._adf.get_activity_runs(run_id)
            stats     = await self._db.get_historical_stats(pipeline_name)
            await self._db.upsert_run(final)
        except Exception as exc:
            print(f"[monitor] completed-run fetch failed for {run_id}: {exc}")
            return
        asyncio.create_task(
            self._analyze(run_id, pipeline_name, final, activities, stats)
        )

    async def _analyze(self, run_id, pipeline_name, run, activities, stats):
        async with self._sem:
            try:
                analysis    = await self._groq.analyze_pipeline_run(run, activities, stats)
                explanation = await self._groq.explain_duration(pipeline_name, run, activities)
                await self._db.save_analysis(run_id, pipeline_name, analysis, explanation)
                await self._broadcast({
                    "event": "run_completed", "runId": run_id,
                    "pipelineName": pipeline_name, "status": run.get("status"),
                    "severity": analysis.get("severity", "low"),
                    "summary":  analysis.get("status_summary", ""),
                })
            except Exception as exc:
                print(f"[monitor] analysis failed for {run_id}: {exc}")

    async def sync_historical(self, hours: int = 48) -> int:
        runs = await self._adf.get_recent_pipeline_runs(hours)
        for run in runs:
            await self._db.upsert_run(run)
            if run.get("status") in ("Succeeded", "Failed"):
                if not await self._db.analysis_exists(run["runId"]):
                    asyncio.create_task(self._handle_completed_run(run))
        return len(runs)

    async def backfill_missing_analyses(self, limit: int = 50):
        await asyncio.sleep(5)
        for run in await self._db.get_runs_missing_analysis(limit):
            try:
                full  = await self._adf.get_pipeline_run(run["run_id"])
                acts  = await self._adf.get_activity_runs(run["run_id"])
                stats = await self._db.get_historical_stats(run["pipeline_name"])
                asyncio.create_task(
                    self._analyze(run["run_id"], run["pipeline_name"], full, acts, stats)
                )
            except Exception:
                continue

    def get_live_runs(self):
        return [
            {
                "runId":        r.get("runId"),
                "pipelineName": r.get("pipelineName"),
                "status":       r.get("status"),
                "runStart":     r.get("runStart"),
                "elapsedSec":   self._elapsed(r.get("runStart", "")),
            }
            for r in self._tracked.values()
        ]

    @staticmethod
    def _elapsed(run_start: str) -> float:
        if not run_start:
            return 0.0
        try:
            start = datetime.fromisoformat(run_start.replace("Z", "+00:00"))
            return (datetime.now(timezone.utc) - start).total_seconds()
        except Exception:
            return 0.0
