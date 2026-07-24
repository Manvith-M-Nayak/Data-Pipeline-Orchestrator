"""
Streaming console backend.

A streaming pipeline runs in availableNow (incremental-then-stop) mode, so
"continuously processing" means re-triggering it as new data arrives. This
manager owns that lifecycle:

  * start(config, schema)      register a live stream (its source/sink/checkpoint
                               containers come from the plan)
  * add_data(id, bytes)        drop new data into the source, then tick now
  * tick(id)                   run ONE incremental pass (executor skip_input_upload)
  * auto-poll (optional)       a background loop ticks every interval_s to pick up
                               data added by external producers
  * output_preview(id)         merged rows currently in the sink

Each Databricks tick spins up a serverless cluster (~90s + cost), so the primary
trigger is drop-driven; the interval poll is coarse (minutes), off by default.
State is in-memory — a server restart clears active streams.
"""

import asyncio
import io
import json
import os
import time
import uuid
from typing import Dict, Optional

from executor_agent import executor as _ex

_OUTPUT_EXTS = (".csv", ".json", ".jsonl", ".ndjson")


class StreamManager:
    def __init__(self):
        self._streams: Dict[str, dict] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def start(
        self, config: dict, schema: dict,
        file_format: str = "csv", interval_s: int = 0,
    ) -> dict:
        if (config.get("mode") or "batch").lower() != "streaming":
            raise ValueError("start() requires a streaming-mode plan")
        stream_stages = [s for s in config.get("stages", []) if s.get("type") == "stream"]
        if not stream_stages:
            raise ValueError("plan has no stream stage")
        clist = config.get("containers_to_create") or []
        source = stream_stages[0].get("source_container") or (clist[0] if clist else None)
        sink = stream_stages[0].get("sink_container") or (clist[-1] if clist else None)

        sid = uuid.uuid4().hex[:8]
        self._streams[sid] = {
            "stream_id": sid,
            "config": config,
            "schema": schema,
            "file_format": (file_format or "csv").lower(),
            "interval_s": int(interval_s or 0),
            "source_container": source,
            "sink_container": sink,
            "active": True,
            "running": False,
            "tick_count": 0,
            "rows_seen": 0,
            "ticks": [],
            "created_at": time.time(),
            "last_error": None,
        }
        self._locks[sid] = asyncio.Lock()
        if interval_s and int(interval_s) > 0:
            self._tasks[sid] = asyncio.ensure_future(self._poll_loop(sid))
        return self.get(sid)

    async def _poll_loop(self, sid: str):
        while self._streams.get(sid, {}).get("active"):
            try:
                await asyncio.sleep(self._streams[sid]["interval_s"])
                if not self._streams.get(sid, {}).get("active"):
                    break
                await self.tick(sid, reason="interval")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[StreamManager] poll loop {sid} non-fatal: {exc}")

    async def add_data(self, sid: str, data: bytes, filename: str) -> dict:
        st = self._require(sid)
        blob = f"stream-{int(time.time())}-{os.path.basename(filename or 'data')}"
        await asyncio.to_thread(self._upload_bytes, st["source_container"], blob, data)
        return await self.tick(sid, reason="drop")

    async def tick(self, sid: str, reason: str = "manual") -> dict:
        st = self._streams.get(sid)
        if not st or not st.get("active"):
            return {"ok": False, "reason": "stream inactive"}
        lock = self._locks[sid]
        if lock.locked():
            return {"ok": False, "reason": "a tick is already running"}
        async with lock:
            st["running"] = True
            try:
                result = await asyncio.to_thread(
                    _ex.execute_pipeline,
                    None, st["config"], st["schema"], None, True, st["file_format"],
                )
            except Exception as exc:
                result = {"status": "failed", "message": str(exc)}
            finally:
                st["running"] = False
            st["tick_count"] += 1
            if result.get("status") == "failed":
                st["last_error"] = result.get("message", "")
            entry = {
                "n": st["tick_count"],
                "reason": reason,
                "ts": time.time(),
                "status": result.get("status"),
                "message": result.get("message", ""),
            }
            st["ticks"] = (st["ticks"] + [entry])[-25:]
            return {"ok": result.get("status") == "ok", "tick": entry}

    async def stop(self, sid: str) -> dict:
        st = self._require(sid)
        st["active"] = False
        t = self._tasks.pop(sid, None)
        if t:
            t.cancel()
        return self.get(sid)

    # ── reads ───────────────────────────────────────────────────────────────
    def get(self, sid: str) -> dict:
        st = self._require(sid)
        return {k: st[k] for k in (
            "stream_id", "file_format", "interval_s", "source_container",
            "sink_container", "active", "running", "tick_count", "created_at",
            "last_error", "ticks",
        )}

    def list(self) -> list:
        return [self.get(sid) for sid in self._streams]

    def output_preview(self, sid: str, limit: int = 200) -> dict:
        st = self._require(sid)
        rows = self._read_sink_rows(st["sink_container"], st["file_format"], limit)
        return {
            "stream_id": sid,
            "sink_container": st["sink_container"],
            "file_format": st["file_format"],
            "row_count": len(rows),
            "rows": rows,
        }

    # ── helpers ───────────────────────────────────────────────────────────────
    def _require(self, sid: str) -> dict:
        st = self._streams.get(sid)
        if not st:
            raise KeyError(sid)
        return st

    def _upload_bytes(self, container: str, blob: str, data: bytes):
        client = _ex._blob_service_client().get_container_client(container)
        try:
            client.create_container()
        except Exception:
            pass  # already exists
        client.upload_blob(name=blob, data=data, overwrite=True)

    def _read_sink_rows(self, container: str, file_format: str, limit: int) -> list:
        """Merge the sink's output/part-* blobs into a row list (schema-matched)."""
        try:
            client = _ex._blob_service_client().get_container_client(container)
            names = sorted(
                b.name for b in client.list_blobs()
                if b.name.lower().startswith("output/")
                and b.name.lower().endswith(_OUTPUT_EXTS) and b.size
            )
        except Exception:
            return []
        rows: list = []
        header = None
        for name in names:
            if len(rows) >= limit:
                break
            raw = client.get_blob_client(name).download_blob().readall().decode(
                "utf-8", errors="replace"
            ).lstrip("﻿")
            if file_format == "json":
                try:
                    doc = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                rows.extend(doc if isinstance(doc, list) else [doc])
            else:
                lines = [l for l in raw.splitlines() if l != ""]
                if not lines:
                    continue
                if header is None:
                    header = lines[0]
                    cols = header.split(",")
                    rows.extend(dict(zip(cols, l.split(","))) for l in lines[1:])
                elif lines[0] == header:
                    cols = header.split(",")
                    rows.extend(dict(zip(cols, l.split(","))) for l in lines[1:])
                # else: different schema (polluted container) — skip
        return rows[:limit]


stream_manager = StreamManager()
