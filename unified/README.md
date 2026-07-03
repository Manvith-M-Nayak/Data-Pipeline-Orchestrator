# Planner-Agent synthetic dataset

A **configurable, seeded generator** and an **independent validator** for the
planner fine-tuning dataset. Output format is unchanged from the previous
dataset (the training notebook still parses it) — the *content* is now varied
and logically correct, so the model learns to reason instead of memorizing one
`raw→bronze→silver→gold` template.

## Files

| File | Purpose |
|------|---------|
| `generate_dataset.py` | Seeded, reproducible. Writes `planner_config_dataset.jsonl`. Driven entirely by the `CONFIG` block at the top. Imports the canonical renderers/settings/ranges from `validate_dataset.py` so it can't drift from the rules. |
| `validate_dataset.py` | **Source of truth for correctness.** Self-contained (imports no generator/planner code). Checks every row against all rules; exits non-zero on any failure, printing the offending row index + reason. Also prints a diversity report. |
| `report.py` | Standalone diversity report (reuses `validate_dataset.report`). |

### Single source of truth

To make prompt⇄config and rules un-drift-able, three things live **only** in
`validate_dataset.py` and are imported by the generator:

- `render_prompt(config)` — the `user_prompt` is a *pure function* of `config`.
  The generator builds the config, then renders the prompt from it; the
  validator re-renders and asserts char-for-char equality. A prompt can never
  describe an operation the config doesn't have, or vice-versa.
- `expected_settings(size_hint, has_aggregation)` — the one deterministic
  resource mapping (below).
- `column_range` / `RANGES` — the realistic per-column value ranges.

> **Note:** the generator writes `planner_config_dataset.jsonl`. The training
> notebook builder (`build_finetune_notebook.py`) currently points at
> `synthetic_planner_dataset.jsonl` — update its `DATASET` path to
> `planner_config_dataset.jsonl` (or symlink) to pick this up.

## Quick start

```bash
python generate_dataset.py --rows 2000        # -> planner_config_dataset.jsonl
python validate_dataset.py                     # 100% pass + diversity report
python report.py                               # diversity report only
```

Same seed ⇒ byte-identical output. Override per run:

```bash
python generate_dataset.py --rows 5000 --seed 7 --out my.jsonl
python validate_dataset.py my.jsonl
```

## Output format (one JSON object per line)

```
{"schema": {...}, "user_prompt": "...", "config": {...}}
```

- **schema**: `columns`, `inferred_types` (col→`string|integer|double|timestamp`),
  `row_count`, `size_hint` (one of the four buckets), `samples` (3 rows whose
  values match the columns/types).
- **config**: `containers`, `containers_to_create`, `datasets`, `stages`
  (stage 0 `copy`, the rest `notebook`), `execution_order`, `num_containers`,
  `recommended_settings`, `editable_settings`, `reasoning`.

## What the validator enforces (every row, every rule)

**Structural (S1–S8):**

1. `num_containers == len(containers) == len(containers_to_create)`
2. `len(stages) == num_containers - 1`
3. `execution_order == [s.name for s in stages]`
4. stage 0 is `copy`; all others `notebook`
5. copy stage source/sink datasets and each notebook stage's source/sink
   containers are the correct consecutive containers
6. one dataset per container, in order; roles source→intermediate…→sink
7. every column used in a filter / transform / `group_by` / aggregation column
   is a real schema column **or** a derived column created earlier (lineage
   accumulates forward); `sum`/`avg`/`max`/`min` aggregate a numeric column;
   `count` uses `*`
8. `samples` keys == columns and each value parses to its declared type

**Quality (F1–F6):**

- **F1 — deterministic settings.** `recommended_settings` is exactly
  `f(size_hint, has_aggregation)` (table below), equals the per-stage settings
  (copy `diu`; notebook `num_workers`/`shuffle_partitions`), and is monotone
  non-decreasing small→medium→large→xlarge. Two rows with the same
  `(size_hint, has_aggregation)` are guaranteed identical. `row_count` is in the
  size bucket's range.
- **F2 — one filter grammar.** Every `filter_condition` is SQL-style
  (`=`, `!=`, `<`, `<=`, `>`, `>=`, `between … and …`, `in (…)`). Function-call
  predicates (`equals(`, `upper(` …) are rejected inside filters (derived-column
  transforms may still use functions).
- **F3 — prompt ⇄ config exact.** `user_prompt` must equal the canonical render
  of `config` character-for-character.
- **F4 — no equality on floats.** No `=`/`!=`/`in` on a `double` column; only
  range predicates.
- **F5 — realistic values.** Every sample value and every numeric filter
  threshold falls within its column's configured range.
- **F6 — few dead stages.** Dataset-wide pass-through notebook ratio ≤
  `max_passthrough_ratio` (default 0.25).

**Semantic (FA–FE):**

- **FA — no contradictory filter chains.** Each column's effective constraint is
  simulated as it flows through the stages (discrete allow/exclude set ∩ numeric
  interval); a chain that becomes infeasible (e.g. `currency = 'USD'` then
  `currency in ('CAD','JPY')`, or `amount <= 1467` then `amount > 4025`) is
  rejected.
- **FB — no dominated/duplicate ops.** A later same-direction threshold that
  doesn't tighten the bound (`> 842` then `> 627`) is rejected, as is the same
  derived column name defined twice in a pipeline.
- **FC — no identity renames.** A transformation whose RHS is a bare existing
  column (`x_renamed = x`) is rejected — it copies a column unchanged.
- **FD — domain-specific ranges.** Ranges can depend on the domain (e.g.
  `duration_sec` is ≤ 2h for telecom calls but ≤ 24h for pipeline runs) via
  `DOMAIN_OVERRIDES`; samples and thresholds are bounded to them.
- **FE — no all-pass-through pipelines.** Every pipeline must contain at least
  one real operation (filter, derivation, or aggregation) somewhere.

### F1 deterministic settings table

| size_hint | diu | num_workers | shuffle_partitions | node_type |
|-----------|-----|-------------|--------------------|-----------|
| small  | 2  | 1 | 8  | Standard_DS3_v2 |
| medium | 4  | 2 | 16 | Standard_D4s_v3 |
| large  | 8  | 4 | 32 | Standard_D8s_v3 |
| xlarge | 16 | 8 | 64 | Standard_D16s_v3 |

If the pipeline contains an aggregation (shuffle-heavy), `num_workers` and
`shuffle_partitions` each bump one tier (e.g. small+agg → workers 2,
shuffle 16). `diu`/`node_type` are unchanged.

## CONFIG knobs (top of `generate_dataset.py`)

| Knob | What it controls |
|------|------------------|
| `num_rows` | rows to generate (CLI `--rows` overrides) |
| `seed` | RNG seed for reproducibility (CLI `--seed` overrides) |
| `output_path` | output file (CLI `--out` overrides) |
| `size_dist` | probability weights over the four `size_hint` buckets |
| `num_containers_dist` | weights over container counts (3–6 ⇒ 2–5 stages) |
| `container_schemes` | named container sequences: medallion / lakehouse / elt / generic. Add or edit schemes here. |
| `scheme_weights` | how often each scheme is chosen (only schemes long enough for the drawn container count are eligible) |
| `agg_prob` | probability the **final** notebook stage is an aggregation |
| `work_prob` | probability an *earlier* notebook stage does real work (F6: keeps pass-throughs rare and varies which stage works) |
| `final_work_prob` | probability the final notebook stage does real work |
| `max_passthrough_ratio` | F6 ceiling — validator fails if the dataset-wide pass-through ratio exceeds this |
| `processed_time_prob` | how often a work stage also stamps `processed_time` (kept low so it isn't the template crutch) |
| `op_weights` | relative weights of the non-aggregation work ops (numeric/flag/catint/category filters, unit conversion, rounding, cast, normalization, concat, rename, dedup, sort) |
| `row_range_by_size` | `row_count` range per size bucket |
| `value_ranges` | F5 per-column realistic ranges (`(substring, lo, hi)`, first match wins; per-type default otherwise). Imported from `validate_dataset.RANGES`; edit there or override here. Bounds both samples and numeric filter thresholds. |
| `domain_overrides` | FD per-(domain, column) range overrides for names whose realistic range depends on the domain (keyed by the domain's first column). Imported from `validate_dataset.DOMAIN_OVERRIDES`. |
| `editable_settings` | master editable lists emitted in every row (imported from `validate_dataset.EDITABLE_SETTINGS`; each must contain the recommended value) |
| `domains` | the domain catalog. Each domain is `cols` (a list of `(name, type, role)`) plus `pools` (value pools for category columns). |

> Settings themselves are **not** a CONFIG knob anymore — they're the
> deterministic `expected_settings()` mapping in `validate_dataset.py` (F1).
> Filter operators and thresholds are derived from each column's type + range,
> so there's no operator/threshold pool to tune.

### Adding a domain

Append to `CONFIG["domains"]`. Give each column a **role** so the generator only
emits logical ops:

| Role | Meaning | Ops it enables |
|------|---------|----------------|
| `id` | cosmetic key | none (samples only) |
| `cat` | low-cardinality string (needs a `pools` entry) | category equals/in filter, normalize, group_by |
| `catint` | code-like int (needs a `pools` entry) | `==`/`in` filter, group_by |
| `flag` | int 0/1 | `== 0/1` filter |
| `madd` | additive numeric measure | sum/avg/min/max, threshold filter, unit conversion |
| `mlvl` | level numeric measure | avg/min/max (no sum), threshold filter, conversion |
| `name` / `email` / `text` | free strings | concat source; samples only |
| `ts` | timestamp | samples only |

Make the **first column unique** across domains — the diversity report keys on it.

## Acceptance (verified)

- `generate_dataset.py` → `validate_dataset.py`: **100% of rows pass, 0 violations**.
- No single domain/shape exceeds ~6% of rows; all four size buckets and all
  stage counts (2–5) present; all four container schemes used.
- Reproducible from the seed; `num_rows` configurable (tested at 2,000).
