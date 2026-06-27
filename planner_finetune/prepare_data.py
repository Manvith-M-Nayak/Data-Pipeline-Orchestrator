#!/usr/bin/env python3
"""
Stage 2 — Data prep.

Reads the planner dataset (one JSON object per line) and emits MLX chat-format
files into ``mlx_data/``:

    {"messages": [
        {"role": "system",    "content": <task + DSL contract>},
        {"role": "user",      "content": <schema + request>},
        {"role": "assistant", "content": <compact wrapped config JSON>}
    ]}

Two input record shapes are accepted:

  * ``{schema, user_prompt, config}``  (this repo's synthetic dataset) — the
    system/user messages are built here and the assistant target is the wrapped
    config ``{"config": <config>, "used_fallback": false}``.
  * ``{instruction, input, output}``   (generic SFT triples) — system=instruction,
    user=input, assistant=output (compacted to one line if it is JSON).

The assistant content is always compacted to a single line so the model learns a
clean, parseable target. Output is split 90/10 into train/valid (valid only when
there are >= 10 rows). Every emitted line is validated; any failure exits non-zero.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys

# ── The system prompt mirrors the real planner contract. Keep it IDENTICAL at
#    inference time (evaluate.py imports it from here) or quality drops. ──────────
SYSTEM_PROMPT = (
    "You are a hybrid Azure Data Factory + Databricks pipeline architect. "
    "ADF orchestrates (control plane); Databricks computes (execution plane). "
    "Given a CSV schema and a user request, output exactly ONE JSON object of the "
    'form {"config": <config>, "used_fallback": <bool>} describing a multi-stage '
    "medallion pipeline (raw -> bronze -> silver -> ...).\n\n"
    "Stage types:\n"
    "  - 'copy'    : ADF Copy Activity, ingestion ONLY (stage0 -> stage1), no transforms.\n"
    "  - 'notebook': Databricks PySpark; does transformations, filter, aggregation.\n\n"
    "Transformation DSL ('output_col = expr'): upper/lower/trim/initCap/length, "
    "toInteger/toDouble/toString/toTimestamp, concat/substring/regexReplace, "
    "year/month/dayOfMonth, currentTimestamp(), and arithmetic (+ - * /). "
    "ALWAYS add 'processed_time = currentTimestamp()' to every notebook stage.\n\n"
    "Filter ('filter_condition'): equals/notEquals/greater/less(toInteger(col), n), "
    "equals(col, 'value'), isNull(col), or 'col > n'.\n\n"
    "Aggregation (optional, notebook stages only): "
    '{"group_by": [...], "aggregations": [{"op": avg|sum|min|max|count, '
    '"column": <col or \'*\'>, "alias": <name>}]}. '
    "avg/sum require numeric columns; group_by and aggregated columns MUST exist "
    "in the schema.\n\n"
    "The config object has exactly these 9 keys: containers, containers_to_create, "
    "datasets, stages, execution_order, num_containers, recommended_settings, "
    "editable_settings, reasoning. recommended_settings has exactly: diu, "
    "num_workers, shuffle_partitions, node_type.\n\n"
    "Rules: first stage is 'copy', the rest are 'notebook'; reference ONLY columns "
    "that exist in the schema; preserve column names exactly. "
    "Output ONLY the JSON object — no markdown, no commentary."
)

DEFAULT_INPUT = "planner_config_dataset.jsonl"
OUT_DIR = "mlx_data"
SEED = 3407


def build_user_content(rec: dict) -> str:
    """Schema + request user message for a {schema, user_prompt, config} record."""
    s, cfg = rec["schema"], rec["config"]
    return (
        f"CSV Columns: {s['columns']}\n"
        f"Column Types: {s['inferred_types']}\n"
        f"Row Count: {s['row_count']}\n"
        f"File Size: {s['size_hint']}\n\n"
        f"Sample Data:\n{json.dumps(s['samples'][:3], indent=2)}\n\n"
        f'User Prompt: "{rec["user_prompt"]}"\n\n'
        f"Number of stages: {cfg['num_containers']}\n"
        f"Container names: {cfg['containers_to_create']}\n\n"
        "Design the complete unified ADF+Databricks pipeline configuration JSON:"
    )


def _compact(text_or_obj) -> str:
    """Single-line JSON. Accepts a dict/list or a JSON string; passes through plain text."""
    if isinstance(text_or_obj, (dict, list)):
        return json.dumps(text_or_obj, separators=(",", ":"), ensure_ascii=False)
    try:
        return json.dumps(json.loads(text_or_obj), separators=(",", ":"), ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        return str(text_or_obj)


def record_to_messages(rec: dict) -> list[dict]:
    """Convert one source record (either shape) to a 3-turn chat."""
    if "schema" in rec and "config" in rec:
        assistant = _compact({"config": rec["config"], "used_fallback": False})
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_content(rec)},
            {"role": "assistant", "content": assistant},
        ]
    if "instruction" in rec and "input" in rec and "output" in rec:
        return [
            {"role": "system", "content": rec["instruction"]},
            {"role": "user", "content": rec["input"]},
            {"role": "assistant", "content": _compact(rec["output"])},
        ]
    raise ValueError(
        f"unrecognized record keys: {sorted(rec)} "
        "(need {schema,user_prompt,config} or {instruction,input,output})"
    )


SAMPLE_RECORDS = [
    {
        "schema": {
            "columns": ["order_id", "amount", "status"],
            "inferred_types": {"order_id": "int", "amount": "double", "status": "str"},
            "row_count": 1200,
            "size_hint": "small",
            "samples": [
                {"order_id": 1, "amount": 19.99, "status": "paid"},
                {"order_id": 2, "amount": 5.0, "status": "refunded"},
                {"order_id": 3, "amount": 42.5, "status": "paid"},
            ],
        },
        "user_prompt": "Keep only paid orders and total the amount.",
        "config": {
            "containers": {"stage0": "raw", "stage1": "bronze"},
            "containers_to_create": ["raw", "bronze"],
            "datasets": [
                {"name": "orders_src", "container": "raw", "role": "source"},
                {"name": "orders_out", "container": "bronze", "role": "sink"},
            ],
            "stages": [
                {"name": "stage0", "activity": "copy"},
                {
                    "name": "stage1",
                    "activity": "notebook",
                    "filter_condition": "equals(status, 'paid')",
                    "transformations": ["processed_time = currentTimestamp()"],
                },
            ],
            "execution_order": ["stage0", "stage1"],
            "num_containers": 2,
            "recommended_settings": {
                "diu": 4,
                "num_workers": 2,
                "shuffle_partitions": 8,
                "node_type": "Standard_DS3_v2",
            },
            "editable_settings": {
                "diu": [2, 4, 8],
                "num_workers": [2, 4],
                "shuffle_partitions": [8, 16],
                "node_type": ["Standard_DS3_v2", "Standard_DS4_v2"],
            },
            "reasoning": "Single ingest then one notebook stage to filter paid and total.",
        },
    },
    {
        "schema": {
            "columns": ["sensor_id", "temp_c", "ts"],
            "inferred_types": {"sensor_id": "str", "temp_c": "double", "ts": "str"},
            "row_count": 50000,
            "size_hint": "medium",
            "samples": [
                {"sensor_id": "a1", "temp_c": 20.1, "ts": "2026-01-01T00:00:00"},
                {"sensor_id": "a2", "temp_c": 25.6, "ts": "2026-01-01T00:00:05"},
                {"sensor_id": "a1", "temp_c": 19.8, "ts": "2026-01-01T00:00:10"},
            ],
        },
        "user_prompt": "Convert Celsius to Fahrenheit and drop readings below 0C.",
        "config": {
            "containers": {"stage0": "raw", "stage1": "bronze"},
            "containers_to_create": ["raw", "bronze"],
            "datasets": [
                {"name": "iot_src", "container": "raw", "role": "source"},
                {"name": "iot_out", "container": "bronze", "role": "sink"},
            ],
            "stages": [
                {"name": "stage0", "activity": "copy"},
                {
                    "name": "stage1",
                    "activity": "notebook",
                    "filter_condition": "greater(toInteger(temp_c), 0)",
                    "transformations": [
                        "temp_f = temp_c * 9 / 5 + 32",
                        "processed_time = currentTimestamp()",
                    ],
                },
            ],
            "execution_order": ["stage0", "stage1"],
            "num_containers": 2,
            "recommended_settings": {
                "diu": 4,
                "num_workers": 4,
                "shuffle_partitions": 16,
                "node_type": "Standard_DS4_v2",
            },
            "editable_settings": {
                "diu": [2, 4, 8],
                "num_workers": [2, 4, 8],
                "shuffle_partitions": [8, 16, 32],
                "node_type": ["Standard_DS3_v2", "Standard_DS4_v2"],
            },
            "reasoning": "Ingest then a notebook stage for the unit conversion and filter.",
        },
    },
    {
        "schema": {
            "columns": ["user", "country", "spend"],
            "inferred_types": {"user": "str", "country": "str", "spend": "double"},
            "row_count": 8000,
            "size_hint": "small",
            "samples": [
                {"user": "u1", "country": "US", "spend": 12.0},
                {"user": "u2", "country": "IN", "spend": 3.5},
                {"user": "u3", "country": "US", "spend": 7.2},
            ],
        },
        "user_prompt": "Average spend per country.",
        "config": {
            "containers": {"stage0": "raw", "stage1": "bronze"},
            "containers_to_create": ["raw", "bronze"],
            "datasets": [
                {"name": "spend_src", "container": "raw", "role": "source"},
                {"name": "spend_out", "container": "bronze", "role": "sink"},
            ],
            "stages": [
                {"name": "stage0", "activity": "copy"},
                {
                    "name": "stage1",
                    "activity": "notebook",
                    "aggregation": {
                        "group_by": ["country"],
                        "aggregations": [
                            {"op": "avg", "column": "spend", "alias": "avg_spend"}
                        ],
                    },
                    "transformations": ["processed_time = currentTimestamp()"],
                },
            ],
            "execution_order": ["stage0", "stage1"],
            "num_containers": 2,
            "recommended_settings": {
                "diu": 2,
                "num_workers": 2,
                "shuffle_partitions": 8,
                "node_type": "Standard_DS3_v2",
            },
            "editable_settings": {
                "diu": [2, 4],
                "num_workers": [2, 4],
                "shuffle_partitions": [8, 16],
                "node_type": ["Standard_DS3_v2", "Standard_DS4_v2"],
            },
            "reasoning": "Ingest then one notebook stage that groups by country and averages spend.",
        },
    },
]


def load_records(path: str) -> list[dict]:
    """Read JSONL records, or create a tiny 3-example sample file if missing."""
    if not os.path.exists(path):
        print(f"!! {path} not found — writing a 3-example sample so the pipeline still runs.")
        with open(path, "w", encoding="utf-8") as f:
            for rec in SAMPLE_RECORDS:
                f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    records = []
    with open(path, encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                sys.exit(f"ERROR: {path}:{ln} is not valid JSON: {e}")
    if not records:
        sys.exit(f"ERROR: {path} has no records.")
    return records


def validate_line(line: str, where: str) -> None:
    """Every emitted line must parse, have non-empty messages, end on assistant."""
    obj = json.loads(line)  # raises if not JSON
    msgs = obj.get("messages")
    if not isinstance(msgs, list) or not msgs:
        sys.exit(f"ERROR: {where}: missing/empty 'messages'.")
    for m in msgs:
        if not m.get("content", "").strip():
            sys.exit(f"ERROR: {where}: empty content in role '{m.get('role')}'.")
    if msgs[-1].get("role") != "assistant":
        sys.exit(f"ERROR: {where}: last message role is '{msgs[-1].get('role')}', not 'assistant'.")


def write_split(rows: list[str], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for i, line in enumerate(rows, 1):
            validate_line(line, f"{path}:{i}")
            f.write(line + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Prepare MLX chat data for the planner agent.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="source JSONL (default: %(default)s)")
    ap.add_argument("--out-dir", default=OUT_DIR, help="output dir (default: %(default)s)")
    ap.add_argument("--val-frac", type=float, default=0.10, help="validation fraction")
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    os.chdir(here)

    records = load_records(args.input)
    print(f"==> Loaded {len(records)} source records from {args.input}")

    lines: list[str] = []
    for i, rec in enumerate(records):
        try:
            msgs = record_to_messages(rec)
        except ValueError as e:
            sys.exit(f"ERROR: record {i}: {e}")
        lines.append(json.dumps({"messages": msgs}, ensure_ascii=False))

    random.Random(args.seed).shuffle(lines)

    os.makedirs(args.out_dir, exist_ok=True)
    if len(lines) >= 10:
        n_val = max(1, int(round(len(lines) * args.val_frac)))
        valid, train = lines[:n_val], lines[n_val:]
    else:
        valid, train = [], lines
        print(f"!! only {len(lines)} rows (<10): skipping valid.jsonl, all rows go to train.")

    train_path = os.path.join(args.out_dir, "train.jsonl")
    write_split(train, train_path)
    print(f"==> Wrote {len(train)} train rows -> {train_path}")

    if valid:
        valid_path = os.path.join(args.out_dir, "valid.jsonl")
        write_split(valid, valid_path)
        print(f"==> Wrote {len(valid)} valid rows -> {valid_path}")

    print("==> Data prep OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
