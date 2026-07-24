import os
import json
import aiosqlite
from typing import Optional, List, Dict, Any

# Default is anchored to the project root (unified/), not the CWD, so the
# server finds the same DB regardless of where it was launched from.
_DEFAULT_DB = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "adf_monitor.db"
))
DB_PATH = os.getenv("DB_PATH", _DEFAULT_DB)


class DBService:
    def __init__(self):
        db_dir = os.path.dirname(DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    async def initialize(self):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id        TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    status        TEXT,
                    run_start     TEXT,
                    run_end       TEXT,
                    duration_ms   INTEGER,
                    message       TEXT,
                    raw_json      TEXT,
                    created_at    TEXT DEFAULT (datetime('now'))
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_analyses (
                    run_id               TEXT PRIMARY KEY,
                    pipeline_name        TEXT,
                    status_summary       TEXT,
                    anomalies            TEXT,
                    root_cause           TEXT,
                    performance_insights TEXT,
                    suggestions          TEXT,
                    severity             TEXT,
                    explanation          TEXT,
                    created_at           TEXT DEFAULT (datetime('now'))
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_log (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id        TEXT,
                    pipeline_name TEXT,
                    elapsed_sec   REAL,
                    avg_sec       REAL,
                    p95_sec       REAL,
                    groq_verdict  TEXT,
                    logged_at     TEXT DEFAULT (datetime('now'))
                )
            """)
            # Central-manager run state — persisted so runs survive a restart
            # (in-memory _runs is lost) and the frontend never 404s on resume.
            await db.execute("""
                CREATE TABLE IF NOT EXISTS manager_runs (
                    run_id       TEXT PRIMARY KEY,
                    status       TEXT,
                    phase        TEXT,
                    step         TEXT,
                    started_at   TEXT,
                    completed_at TEXT,
                    retries      INTEGER,
                    stage_count  INTEGER,
                    state_json   TEXT,
                    updated_at   TEXT DEFAULT (datetime('now'))
                )
            """)
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_name)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status)"
            )
            await db.commit()

    # ── Central-manager run persistence ──────────────────────────────────────
    _MANAGER_TERMINAL = ("completed", "failed")

    async def save_manager_run(self, state: Dict[str, Any]):
        """Upsert a full manager RunState snapshot (state == get_state_dict())."""
        if not state or not state.get("run_id"):
            return
        plan = state.get("plan") or {}
        stage_count = len(plan.get("stages", []))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO manager_runs
                    (run_id, status, phase, step, started_at, completed_at,
                     retries, stage_count, state_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status, phase=excluded.phase, step=excluded.step,
                    completed_at=excluded.completed_at, retries=excluded.retries,
                    stage_count=excluded.stage_count, state_json=excluded.state_json,
                    updated_at=excluded.updated_at
                """,
                (
                    state.get("run_id"), state.get("status"), state.get("phase"),
                    state.get("step"), state.get("started_at"), state.get("completed_at"),
                    state.get("retries", 0), stage_count, json.dumps(state),
                ),
            )
            await db.commit()

    async def get_manager_run(self, run_id: str) -> Optional[Dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT state_json FROM manager_runs WHERE run_id=?", (run_id,)
            ) as cur:
                row = await cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except (ValueError, TypeError):
            return None

    async def list_manager_runs(self, limit: int = 100) -> List[Dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT run_id, status, phase, step, started_at, completed_at,
                       retries, stage_count
                FROM manager_runs ORDER BY started_at DESC LIMIT ?
                """,
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def mark_interrupted_manager_runs(self) -> int:
        """On startup, fail any run left non-terminal by a crash/restart — its
        asyncio task is gone, so it can never complete."""
        placeholders = ",".join("?" * len(self._MANAGER_TERMINAL))
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"SELECT run_id, state_json FROM manager_runs "
                f"WHERE status NOT IN ({placeholders})",
                self._MANAGER_TERMINAL,
            ) as cur:
                rows = await cur.fetchall()
            for r in rows:
                try:
                    st = json.loads(r["state_json"])
                except (ValueError, TypeError):
                    st = {"run_id": r["run_id"]}
                st["status"] = "failed"
                st["step"] = "Failed: interrupted by server restart"
                st["error"] = st.get("error") or "Run interrupted by server restart"
                await db.execute(
                    "UPDATE manager_runs SET status='failed', step=?, "
                    "state_json=?, updated_at=datetime('now') WHERE run_id=?",
                    (st["step"], json.dumps(st), r["run_id"]),
                )
            await db.commit()
            return len(rows)

    async def upsert_run(self, run: Dict[str, Any]):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO pipeline_runs
                    (run_id, pipeline_name, status, run_start, run_end, duration_ms, message, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status, run_end=excluded.run_end,
                    duration_ms=excluded.duration_ms, message=excluded.message,
                    raw_json=excluded.raw_json
                """,
                (
                    run.get("runId"), run.get("pipelineName"), run.get("status"),
                    run.get("runStart"), run.get("runEnd"), run.get("durationMs"),
                    run.get("message", ""), json.dumps(run),
                ),
            )
            await db.commit()

    async def save_analysis(
        self, run_id: str, pipeline_name: str, analysis: Dict, explanation: str
    ):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO pipeline_analyses
                    (run_id, pipeline_name, status_summary, anomalies, root_cause,
                     performance_insights, suggestions, severity, explanation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status_summary=excluded.status_summary, anomalies=excluded.anomalies,
                    root_cause=excluded.root_cause,
                    performance_insights=excluded.performance_insights,
                    suggestions=excluded.suggestions, severity=excluded.severity,
                    explanation=excluded.explanation
                """,
                (
                    run_id, pipeline_name,
                    analysis.get("status_summary", ""),
                    json.dumps(analysis.get("anomalies", [])),
                    analysis.get("root_cause", ""),
                    json.dumps(analysis.get("performance_insights", [])),
                    json.dumps(analysis.get("suggestions", [])),
                    analysis.get("severity", "low"),
                    explanation,
                ),
            )
            await db.commit()

    async def log_anomaly(
        self, run_id: str, pipeline_name: str,
        elapsed_sec: float, avg_sec: float, p95_sec: float, groq_verdict: str
    ):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO anomaly_log (run_id, pipeline_name, elapsed_sec, avg_sec, p95_sec, groq_verdict)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, pipeline_name, elapsed_sec, avg_sec, p95_sec, groq_verdict),
            )
            await db.commit()

    async def get_historical_stats(self, pipeline_name: str) -> Dict:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT duration_ms FROM pipeline_runs
                WHERE pipeline_name=? AND status='Succeeded' AND duration_ms IS NOT NULL
                ORDER BY run_start DESC LIMIT 100
                """,
                (pipeline_name,),
            ) as cur:
                rows = await cur.fetchall()
        if not rows:
            return {"avg": 0, "min": 0, "max": 0, "p95": 0, "count": 0}
        durations = sorted(r["duration_ms"] / 1000 for r in rows)
        n = len(durations)
        return {
            "avg": sum(durations) / n,
            "min": durations[0],
            "max": durations[-1],
            "p95": durations[min(int(n * 0.95), n - 1)],
            "count": n,
        }

    async def get_pipeline_runs(
        self,
        status: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        conditions, params = [], []
        if status:
            conditions.append("r.status=?")
            params.append(status)
        if pipeline_name:
            conditions.append("r.pipeline_name=?")
            params.append(pipeline_name)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"""
                SELECT r.*, a.status_summary, a.anomalies, a.root_cause,
                       a.performance_insights, a.suggestions, a.severity, a.explanation
                FROM pipeline_runs r
                LEFT JOIN pipeline_analyses a ON r.run_id = a.run_id
                {where}
                ORDER BY r.run_start DESC LIMIT ?
                """,
                params,
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def analysis_exists(self, run_id: str) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM pipeline_analyses WHERE run_id=?", (run_id,)
            ) as cur:
                return await cur.fetchone() is not None

    async def get_runs_missing_analysis(self, limit: int = 50) -> List[Dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT r.* FROM pipeline_runs r
                LEFT JOIN pipeline_analyses a ON r.run_id = a.run_id
                WHERE r.status IN ('Succeeded', 'Failed') AND a.run_id IS NULL
                ORDER BY r.run_start DESC LIMIT ?
                """,
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_anomaly_log(self, limit: int = 100) -> List[Dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM anomaly_log ORDER BY logged_at DESC LIMIT ?", (limit,)
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_historical_runs_for_prediction(
        self, pipeline_name: str, limit: int = 30
    ) -> List[Dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT run_id, status, run_start, run_end, duration_ms FROM pipeline_runs
                WHERE pipeline_name=? AND status IN ('Succeeded', 'Failed')
                  AND duration_ms IS NOT NULL
                ORDER BY run_start DESC LIMIT ?
                """,
                (pipeline_name, limit),
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_known_pipeline_names(self) -> List[str]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name"
            ) as cur:
                rows = await cur.fetchall()
        return [r[0] for r in rows]
