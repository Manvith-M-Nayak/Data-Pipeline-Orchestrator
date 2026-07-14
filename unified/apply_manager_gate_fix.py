"""
One-time patch for central_manager_agent/manager.py — v2, more robust.

Fixes the missing ML-path-only gate on the Learning Agent's duration
correction factor. Without this gate, the learned factor gets applied to
BOTH the ML and formula prediction paths — but the formula path already
self-corrects on its own (see performance_agent.py's _compute_adjustment),
so applying the Learning Agent's factor on top of that double-corrects.

Unlike the first version of this script, this one does NOT try to match a
large block of comment text (fragile — any difference in dash characters,
wording, or whitespace breaks it). It anchors only on two small, exact code
lines that are very unlikely to have been hand-edited:
    from learning_policy_agent import get_learning_agent
    result["learning_correction_applied"] = factor
and re-indents everything between them by one extra level, inserting the
"if result.get(...) == 'ml_model':" guard right after the enclosing `try:`.

Usage (from the unified/ directory):
    python3 apply_manager_gate_fix_v2.py

Makes a backup at central_manager_agent/manager.py.bak before writing.
Safe to re-run: detects if the gate is already present and does nothing.
"""

import shutil
import sys

PATH = "central_manager_agent/manager.py"

IMPORT_LINE = "from learning_policy_agent import get_learning_agent"
LAST_LINE = 'result["learning_correction_applied"] = factor'
GUARD_LINE = 'if result.get("prediction_source") == "ml_model":'


def leading_ws(line: str) -> str:
    return line[: len(line) - len(line.lstrip(" "))]


def main():
    try:
        with open(PATH, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"ERROR: {PATH} not found. Run this from the unified/ directory.")
        sys.exit(1)

    # LAST_LINE is unique in the file — only predict_performance()'s
    # correction block sets this exact key. Find it first, then search
    # BACKWARD from it for the nearest import line, rather than searching
    # the whole file for import matches (that line legitimately appears
    # again elsewhere — in record_feedback()'s separate on_run_recorded()
    # hook — so a global search over-matches).
    last_idxs = [i for i, l in enumerate(lines) if l.strip() == LAST_LINE]

    if not last_idxs:
        print(f"ERROR: couldn't find the expected anchor line: {LAST_LINE!r}")
        print("No changes made. The method may have been edited further — "
              "send the maintainer a fresh dump of predict_performance().")
        sys.exit(1)
    if len(last_idxs) > 1:
        print("ERROR: found multiple matches for the (expected-unique) closing "
              "anchor line — refusing to guess which one to patch. No changes made.")
        sys.exit(1)

    last_idx = last_idxs[0]

    import_idx = None
    for i in range(last_idx - 1, max(last_idx - 15, -1), -1):
        if lines[i].strip() == IMPORT_LINE:
            import_idx = i
            break
    if import_idx is None:
        print(f"ERROR: couldn't find {IMPORT_LINE!r} within 15 lines above the "
              "closing anchor. No changes made.")
        sys.exit(1)

    # Find the enclosing `try:` — search a few lines above the import line.
    try_idx = None
    for i in range(import_idx - 1, max(import_idx - 6, -1), -1):
        if lines[i].strip() == "try:":
            try_idx = i
            break
    if try_idx is None:
        print("ERROR: couldn't find the enclosing 'try:' above the import line. "
              "No changes made.")
        sys.exit(1)

    # Idempotency check: already patched?
    next_line = lines[try_idx + 1].strip()
    if next_line == GUARD_LINE:
        print("Already patched — the ml_model guard is already present. "
              "No changes made.")
        sys.exit(0)

    try_indent = leading_ws(lines[try_idx])
    guard_indent = try_indent + "    "
    block_indent_add = "    "  # one extra level for everything inside the guard

    # Build the new lines: unchanged up to and including try:, then the new
    # guard line, then the (re-indented) original block from import_idx
    # through last_idx inclusive, then everything after unchanged.
    new_lines = lines[: try_idx + 1]
    new_lines.append(guard_indent + GUARD_LINE + "\n")

    for i in range(import_idx, last_idx + 1):
        original = lines[i]
        if original.strip() == "":
            new_lines.append(original)
        else:
            new_lines.append(block_indent_add + original)

    new_lines.extend(lines[last_idx + 1 :])

    shutil.copy(PATH, PATH + ".bak")
    print(f"Backup written to {PATH}.bak")

    with open(PATH, "w") as f:
        f.writelines(new_lines)

    print(f"Patched {PATH} successfully.")
    print("\nVerify with:")
    print("  grep -n 'prediction_source.*ml_model' central_manager_agent/manager.py")
    print("Should now print a matching line.")
    print("\nAlso sanity-check the file still parses:")
    print("  python3 -c \"import ast; ast.parse(open('central_manager_agent/manager.py').read()); print('syntax ok')\"")


if __name__ == "__main__":
    main()