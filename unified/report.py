#!/usr/bin/env python3
"""
Standalone diversity report for planner_config_dataset.jsonl.

Prints counts per domain, stage-count, container-scheme, size bucket, column
count, and transform type — to prove the dataset is spread, not templated.

This reuses validate_dataset.report() so the two stay in sync.

    python report.py [path]
"""
import json
import sys

from validate_dataset import report


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "planner_config_dataset.jsonl"
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    report(records)
    return 0


if __name__ == "__main__":
    sys.exit(main())
