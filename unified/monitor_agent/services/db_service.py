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

# How long a connection waits for another writer's lock before raising
# "database is locked". Several writers run concurrently (monitor poll, manager
# phase persistence, anomaly events), so waiting briefly is the right default.
BUSY_TIMEOUT_S = 10.0


def _connect():
    """Open a connection with the busy timeout. WAL mode itself is persistent
    in the database file and is switched on once in initialize()."""
    return aiosqlite.connect(DB_PATH, timeout=BUSY_TIMEOUT_S)


class DBService:
    def __init__(self):
        db_dir = os.path.dirname(DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    async def initialize(self):
        async with _connect() as db:
            # WAL: readers never block the writer and vice versa, so the 20s
            # monitor poll and run-state writes stop colliding.
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
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
            # Real-time anomaly events: one row per detected anomaly KIND per
            # run (a run can raise several — e.g. slow_runtime + sla_breach).
            # Written by monitor_agent/services/anomaly_detector.py.
            await db.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_events (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id        TEXT,
                    pipeline_name TEXT,
                    kind          TEXT,      -- slow_runtime|failure|timeout|...
                    severity      TEXT,      -- low|medium|high
                    detail        TEXT,
                    metrics_json  TEXT,      -- the numbers behind the verdict
                    detected_at   TEXT DEFAULT (datetime('now'))
                )
            """)
            # Per-run metrics history — baseline source for cost/duration
            # comparisons (cost_spike, cold_start gap, etc.).
            await db.execute("""
                CREATE TABLE IF NOT EXISTS run_metrics (
                    run_id        TEXT PRIMARY KEY,
                    pipeline_name TEXT,
                    duration_s    REAL,
                    cost_usd      REAL,
                    rows_written  INTEGER,
                    retries       INTEGER,
                    status        TEXT,
                    created_at    TEXT DEFAULT (datetime('now'))
                )
            """)
            # Last-seen input schema per pipeline — schema_drift detection.
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_schemas (
                    pipeline_name TEXT PRIMARY KEY,
                    columns_json  TEXT,
                    updated_at    TEXT DEFAULT (datetime('now'))
                )
            """)
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_name)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_anomaly_events_kind ON anomaly_events(kind)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_metrics_pipeline "
                "ON run_metrics(pipeline_name, status, created_at)"
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
        async with _connect() as db:
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
        async with _connect() as db:
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
        async with _connect() as db:
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

    async def recent_manager_states(self, limit: int = 200) -> List[Dict]:
        """Full persisted RunState dicts, newest first."""
        async with _connect() as db:
            async with db.execute(
                "SELECT state_json FROM manager_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
        states = []
        for (raw,) in rows:
            try:
                states.append(json.loads(raw))
            except (TypeError, ValueError):
                continue
        return states

    async def mark_interrupted_manager_runs(self) -> int:
        """On startup, fail any run left non-terminal by a crash/restart — its
        asyncio task is gone, so it can never complete."""
        placeholders = ",".join("?" * len(self._MANAGER_TERMINAL))
        async with _connect() as db:
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
        async with _connect() as db:
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
        async with _connect() as db:
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
        async with _connect() as db:
            await db.execute(
                """
                INSERT INTO anomaly_log (run_id, pipeline_name, elapsed_sec, avg_sec, p95_sec, groq_verdict)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, pipeline_name, elapsed_sec, avg_sec, p95_sec, groq_verdict),
            )
            await db.commit()

    async def get_historical_stats(self, pipeline_name: str) -> Dict:
        async with _connect() as db:
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
        run_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        conditions, params = [], []
        if status:
            conditions.append("r.status=?")
            params.append(status)
        if pipeline_name:
            conditions.append("r.pipeline_name=?")
            params.append(pipeline_name)
        if run_id:
            conditions.append("r.run_id=?")
            params.append(run_id)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)
        async with _connect() as db:
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
        async with _connect() as db:
            async with db.execute(
                "SELECT 1 FROM pipeline_analyses WHERE run_id=?", (run_id,)
            ) as cur:
                return await cur.fetchone() is not None

    async def get_runs_missing_analysis(self, limit: int = 50) -> List[Dict]:
        async with _connect() as db:
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

    # ── Real-time anomaly events (anomaly_detector.py) ───────────────────────
    async def log_anomaly_event(
        self, run_id: str, pipeline_name: str, kind: str,
        severity: str, detail: str, metrics: Dict,
    ):
        async with _connect() as db:
            await db.execute(
                """
                INSERT INTO anomaly_events
                    (run_id, pipeline_name, kind, severity, detail, metrics_json)
                VALUES (?,?,?,?,?,?)
                """,
                (run_id, pipeline_name, kind, severity, detail,
                 json.dumps(metrics or {})),
            )
            await db.commit()

    async def get_anomaly_events(
        self, kind: Optional[str] = None, limit: int = 100
    ) -> List[Dict]:
        where, params = "", []
        if kind:
            where = "WHERE kind=?"
            params.append(kind)
        params.append(limit)
        async with _connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"SELECT * FROM anomaly_events {where} "
                f"ORDER BY id DESC LIMIT ?",
                params,
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def save_run_metrics(
        self, run_id: str, pipeline_name: str, duration_s: float,
        cost_usd: Optional[float], rows_written: Optional[int],
        retries: int, status: str,
    ):
        async with _connect() as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO run_metrics
                    (run_id, pipeline_name, duration_s, cost_usd,
                     rows_written, retries, status)
                VALUES (?,?,?,?,?,?,?)
                """,
                (run_id, pipeline_name, duration_s, cost_usd,
                 rows_written, retries, status),
            )
            await db.commit()

    async def get_metric_history(
        self, pipeline_name: str, exclude_run_id: str = "", limit: int = 50,
        include_failed: bool = False,
    ) -> List[Dict]:
        """Prior runs' metrics, newest first (baseline source). Successful runs
        only unless include_failed."""
        status_clause = "" if include_failed else "AND status='ok'"
        async with _connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"""
                SELECT * FROM run_metrics
                WHERE pipeline_name=? AND run_id != ? {status_clause}
                ORDER BY created_at DESC, rowid DESC LIMIT ?
                """,
                (pipeline_name, exclude_run_id, limit),
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_last_schema(self, pipeline_name: str) -> Optional[List[str]]:
        async with _connect() as db:
            async with db.execute(
                "SELECT columns_json FROM pipeline_schemas WHERE pipeline_name=?",
                (pipeline_name,),
            ) as cur:
                row = await cur.fetchone()
        return json.loads(row[0]) if row else None

    async def save_schema(self, pipeline_name: str, columns: List[str]):
        async with _connect() as db:
            await db.execute(
                """
                INSERT INTO pipeline_schemas (pipeline_name, columns_json, updated_at)
                VALUES (?,?,datetime('now'))
                ON CONFLICT(pipeline_name) DO UPDATE SET
                    columns_json=excluded.columns_json, updated_at=datetime('now')
                """,
                (pipeline_name, json.dumps(columns)),
            )
            await db.commit()

    async def get_anomaly_log(self, limit: int = 100) -> List[Dict]:
        async with _connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM anomaly_log ORDER BY logged_at DESC LIMIT ?", (limit,)
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_historical_runs_for_prediction(
        self, pipeline_name: str, limit: int = 30
    ) -> List[Dict]:
        async with _connect() as db:
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
        async with _connect() as db:
            async with db.execute(
                "SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name"
            ) as cur:
                rows = await cur.fetchall()
        return [r[0] for r in rows]
