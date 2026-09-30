"""
One canonical shape for the input-data schema every agent receives.

Clients have sent the schema in two shapes:
  * detect-style:  {"columns": {col: type}, "row_count": N, "size_hint": "...", ...}
  * flat map:      {col: type}   (older frontends stored only the column map)
and some callers sent nothing useful at all. Agents read `row_count`,
`size_hint` and `columns`; with a flat map every one of those was missing, so
the Resource model saw 0 rows / 0 columns and schema-drift detection never ran.

normalize_run_schema() returns:
    {"columns": {col: type}, "row_count": int, "size_hint": str, **other keys}
When the uploaded file is available, row_count and size_hint are measured from
it — the server's own numbers win over whatever the client claimed.
"""

import csv
import io
import json
from typing import Optional

_TYPE_NAMES = {"string", "integer", "double", "long", "float", "boolean", "timestamp", "date"}


def size_hint_for(nbytes: int) -> str:
    """Same buckets and labels as POST /api/schema/detect."""
    if nbytes < 5_242_880:
        return "small (< 5MB)"
    if nbytes < 52_428_800:
        return "medium (5–50MB)"
    if nbytes < 209_715_200:
        return "large (50–200MB)"
    return "xlarge (> 200MB)"


def count_rows(contents: bytes, filename: str = "") -> Optional[int]:
    """Data rows in an uploaded CSV / JSON array / NDJSON file; None if unparseable."""
    text = contents.decode("utf-8", errors="replace").lstrip("﻿")
    name = (filename or "").lower()
    stripped = text.lstrip()
    is_json = name.endswith((".json", ".jsonl", ".ndjson")) or stripped[:1] in ("{", "[")
    try:
        if is_json:
            try:
                doc = json.loads(stripped)
                return len(doc) if isinstance(doc, list) else 1
            except json.JSONDecodeError:
                return sum(1 for line in io.StringIO(stripped) if line.strip())
        rows = csv.reader(io.StringIO(text))
        next(rows, None)  # header
        return sum(1 for r in rows if r)
    except (csv.Error, ValueError):
        return None


def _looks_like_flat_map(raw: dict) -> bool:
    return bool(raw) and "columns" not in raw and all(
        isinstance(v, str) and v.lower() in _TYPE_NAMES for v in raw.values()
    )


def normalize_run_schema(raw, contents: bytes = None, filename: str = "") -> dict:
    raw = raw if isinstance(raw, dict) else {}
    if _looks_like_flat_map(raw):
        out = {"columns": dict(raw)}
    else:
        out = dict(raw)
        cols = raw.get("columns")
        if isinstance(cols, list):
            types = raw.get("inferred_types") or {}
            out["columns"] = {c: types.get(c, "string") for c in cols}
        elif not isinstance(cols, dict):
            out["columns"] = dict(raw.get("inferred_types") or {})

    if contents is not None:
        rows = count_rows(contents, filename)
        if rows is not None:
            out["row_count"] = rows
        out["size_hint"] = size_hint_for(len(contents))
    else:
        out["row_count"] = int(out.get("row_count") or out.get("row_count_sample") or 0)
        out.setdefault("size_hint", "medium")
    out.setdefault("row_count", 0)
    return out
