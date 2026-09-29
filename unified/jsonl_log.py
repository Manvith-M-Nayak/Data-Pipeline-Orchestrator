"""
Append-only JSONL feedback logs (manager_feedback.jsonl, resource_feedback.jsonl).

* append_jsonl() rotates the live file once it passes MAX_BYTES, keeping KEEP
  numbered archives (file.jsonl.1 is the newest), so the logs stay bounded.
* read_jsonl() returns every record, oldest first, across the archives and the
  live file. Parsed records are cached per file and only re-read when that
  file's mtime/size changes, so hot paths (every Resource Agent analyze(),
  every Run Insights request) don't re-parse the whole history each call.

Readers always see archives too, so the learning agents never lose history
to a rotation.
"""

import json
import os
import threading
from typing import Dict, List, Tuple

MAX_BYTES = int(os.getenv("FEEDBACK_LOG_MAX_BYTES", str(20 * 1024 * 1024)))
KEEP = int(os.getenv("FEEDBACK_LOG_KEEP", "5"))

_lock = threading.Lock()
_cache: Dict[str, Tuple[Tuple[int, int], List[dict]]] = {}


def _parse(path: str) -> List[dict]:
    try:
        st = os.stat(path)
    except FileNotFoundError:
        _cache.pop(path, None)
        return []
    key = (st.st_mtime_ns, st.st_size)
    hit = _cache.get(path)
    if hit and hit[0] == key:
        return hit[1]
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # one corrupt line never hides the rest
    _cache[path] = (key, records)
    return records


def read_jsonl(path: str) -> List[dict]:
    """All records, oldest first. Returns fresh dict copies, so callers may
    mutate them without corrupting the cache."""
    with _lock:
        out: List[dict] = []
        for i in range(KEEP, 0, -1):
            out.extend(_parse(f"{path}.{i}"))
        out.extend(_parse(path))
        return [dict(r) for r in out]


def append_jsonl(path: str, record: dict) -> None:
    with _lock:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        try:
            too_big = os.path.getsize(path) >= MAX_BYTES
        except FileNotFoundError:
            too_big = False
        if too_big:
            oldest = f"{path}.{KEEP}"
            if os.path.exists(oldest):
                os.remove(oldest)
            for i in range(KEEP - 1, 0, -1):
                if os.path.exists(f"{path}.{i}"):
                    os.replace(f"{path}.{i}", f"{path}.{i + 1}")
            os.replace(path, f"{path}.1")
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
