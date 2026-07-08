# Original (Raw) Datasets — Overview

Analysis of the five raw Excel exports in `Datasets/Datasets/Original/`. These are the pre-cleaning sources for the files in `Datasets/Datasets/Cleaned/`.

## Overview Summary

|Dataset|Rows|File Size|Date Range|Domain|Success Rate|
|---|---|---|---|---|---|
|`DBQuery_Statistics.xlsx`|35|27 KB|Mar 31 – Apr 1, 2026 (snapshot)|SQL Server Session Monitoring|N/A|
|`databricks_pipeline_runs_march2026.xlsx`|449|32 KB|Mar 1 – Apr 1, 2026|Pipeline Orchestration Logs|17.2% COMPLETED (21.1% of labeled rows)|
|`databricks_job_runs_march2026.xlsx`|836,700|55 MB|Feb 28 – Apr 1, 2026|Job Execution Logs|95.65% SUCCEEDED (99.0% of labeled rows)|
|`databricks_queries_march2026.xlsx`|60,000|4.6 MB|Mar 6, 2026 (single evening)|SQL Query Execution Logs|99.77% FINISHED|
|`Utilization.xlsx`|43,296|3.7 MB|Feb 28 – Mar 31, 2026|Azure Cloud Resource Metrics|N/A|

**Total records across all datasets:** 940,480 rows
**Combined storage:** ~63 MB (xlsx, compressed)

Compared to the cleaned CSVs: the originals carry **57,231 more rows** than survive cleaning (449 vs 316 pipeline runs; 836,700 vs 779,449 job runs), contain **null outcome labels** for in-flight runs, and include **raw SQL/statement text** that the cleaned files reduce to boolean flags.

---

## 1. `DBQuery_Statistics.xlsx`

### What This Dataset Represents

Point-in-time snapshot of 35 active SQL Server sessions, captured around **2026-04-01 07:15 UTC** (session `login_time` values span Mar 31 20:01 → Apr 1 07:14). Each row is one active session with resource counters, wait state, and — unlike the cleaned version — the **full SQL statement text** and the executing stored procedure name. The workload is a cloud-billing ETL: `OPENJSON` shredding of `[staging].[ResourceWiseBillingDaily_*]` tables and `[PowerBIprod].[etl].*` stored procedures for Azure/AWS/GCP cost assessment.

### Schema (20 Columns)

|Column|Type|Range / Values|
|---|---|---|
|`session_id`|Integer|54–153, all unique|
|`status`|Categorical|`runnable` (24), `running` (7), `suspended` (4)|
|`Blk by`|Integer|Always 0 (no blocking observed)|
|`statement_text`|String (raw SQL)|28–5,986 chars, 26 unique statements|
|`command_text`|String|`<Adhoc Batch>` (18), 7 stored procs e.g. `[PowerBIprod].[target].[usp_Load_ResourceWiseBillingDailyAWSV3]` (8)|
|`wait_type`|String|28 null; `PREEMPTIVE_HTTP_REQUEST` (3), `CXCONSUMER` (2), `WAITFOR` (1), `CXSYNC_PORT` (1)|
|`wait_resource`|Float|**100% null** — dead column|
|`Wait M`|Integer|Always 0|
|`cpu_time`|Integer|19 ms – 5,169,415 ms|
|`logical_reads`|Integer|3 – 152,785,535|
|`reads`|Integer|0 – 44,707,666|
|`writes`|Integer|0 – 2,288,462|
|`Elaps M`|Integer|0 – 673 minutes|
|`command`|Categorical|`SELECT` (17), `INSERT` (11), `MERGE` (4), `SELECT INTO` (2), `WAITFOR` (1)|
|`login_name`|String|`usrAdmin` (27), `usrProdInt` (8)|
|`host_name`|String|4 machines: `B0208859FC3E` (25), `cs-prd-us-app-01` (8), 2 others|
|`program_name`|String|28 unique: `Data Integration-<uuid>`, `Microsoft JDBC Driver for SQL Server`|
|`last_request_end_time`|Datetime|2026-03-31 20:01 – 2026-04-01 07:15|
|`login_time`|Datetime|2026-03-31 20:01 – 2026-04-01 07:14|
|`open_transaction_count`|Integer|0 (4), 1 (16), 2 (15)|

### Advantages

**1. Full SQL Statement Text — Biggest Edge Over Cleaned Version** `statement_text` (up to 5,986 chars) contains complete queries: table names (`[staging].[ResourceWiseBillingDaily_AzureCSP_V3_25634_202602]`), `OPENJSON` shredding patterns, CTEs, `MERGE` targets. Root-cause analysis at the query level is actually possible here — the cleaned CSV discards this in favor of `stmt_has_*` boolean flags.

**2. Stored Procedure Attribution** `command_text` names the executing proc (`[PowerBIprod].[etl].[usp_Load_AssessmentResourcesAfterDedupe]` etc.), separating ad-hoc batches (18 rows) from cataloged ETL procedures (17 rows). This is the business-process linkage the cleaned dataset lacks.

**3. Physical vs Logical I/O Separation** `reads`/`writes` (physical) vs `logical_reads` (buffer) per session — enables cache-efficiency diagnosis.

**4. Session Lifecycle Timestamps** Raw `login_time` and `last_request_end_time` datetimes (millisecond precision) allow deriving session age and idle time exactly, rather than trusting pre-computed columns.

**5. Blocking and Transaction State** `Blk by` + `open_transaction_count` (31 of 35 sessions hold open transactions) support blocking-chain and long-transaction analysis.

### Limitations & Drawbacks

**1. Single Snapshot, 35 Rows** Same critical weakness as the cleaned version: one instant in time. No temporal patterns, no baseline, no way to tell typical from anomalous. Statistically unusable alone.

**2. `wait_resource` Is 100% Null** The column exists but every value is missing — wait-target diagnosis (which page/lock/latch) is impossible.

**3. `Wait M` Always Zero Despite Non-Null Wait Types** 7 sessions have a `wait_type` but zero wait minutes — resolution/rounding bug carried straight into the cleaned data.

**4. Non-Standard Column Names** `Blk by`, `Wait M`, `Elaps M` — spaces and abbreviations that break naive `df.column` access and SQL ingestion; require renaming before any pipeline use (the cleaning step does exactly this).

**5. No Query Hash / Plan Data** No `query_hash`, `plan_handle`, or execution plan. Parameterized-query grouping and plan-regression detection impossible.

**6. No Outcome** Snapshot of active sessions — final success/failure of each statement unknown.

**7. Two Logins Only** `usrAdmin` and `usrProdInt`; per-team attribution impossible.

---

## 2. `databricks_pipeline_runs_march2026.xlsx`

### What This Dataset Represents

449 pipeline run records over 31 days (Mar 1 – Apr 1, 2026) across 2 workspaces and 32 unique pipelines. Raw orchestration log — **133 more rows than the cleaned CSV** (449 vs 316), the difference being null-outcome rows and workspace2 records dropped in cleaning. No derived columns: no duration, no environment flag, no temporal features — just 8 raw fields.

### Schema (8 Columns)

|Column|Type|Range / Values|
|---|---|---|
|`result_state`|Categorical|`FAILED` (203), `CANCELED` (85), **null (84)**, `COMPLETED` (77)|
|`workspace_name`|String|`workspace1` (302), `workspace2` (147)|
|`workspace_id`|Integer|2 distinct IDs|
|`trigger_type`|Categorical|`RETRY_ON_FAILURE` (149), `USER_ACTION` (122), `SERVICE_UPGRADE` (87), `JOB_TASK` (42), `API_CALL` (20), `INFRASTRUCTURE_MAINTENANCE` (18), `SETTINGS_CHANGE` (8), `SCHEMA_CHANGE` (3)|
|`pipeline_id`|UUID|32 unique|
|`pipeline_name`|String|28 unique; 125 rows carry `[dev <username>]` prefix, 324 prod|
|`period_start_time`|ISO 8601 string|2026-03-01 04:10 – 2026-03-31|
|`period_end_time`|ISO 8601 string|→ 2026-04-01 04:28|

Derived (not stored): duration ranges **4.5 s to 1,358,144 s (15.7 days)**, median 164.6 s.

### Advantages

**1. Complete Record Including In-Flight/Unresolved Runs** The 84 null `result_state` rows (mostly `SERVICE_UPGRADE` 41 and `INFRASTRUCTURE_MAINTENANCE` 15 triggers) are visible here but absent from the cleaned file. They reveal system-driven runs that never received a terminal state — important for understanding orchestrator behavior during maintenance windows.

**2. workspace2 Fully Represented** 147 workspace2 rows (33%) vs only 34 in the cleaned CSV. The raw data shows workspace2's real story: 69 CANCELED, 73 unresolved, 3 FAILED, only 2 COMPLETED — a workspace where almost nothing finishes, which the cleaned dataset largely hides.

**3. Extra Trigger Category** `INFRASTRUCTURE_MAINTENANCE` (18 rows) exists in the raw data but not in the cleaned file's category list — cleaning dropped an entire trigger class.

**4. Full-Precision ISO Timestamps** Microsecond-precision, timezone-aware start/end times allow exact duration computation and retry-chain reconstruction by temporal adjacency.

**5. Dev/Prod Derivable** `[dev username]` prefix identifies 125 dev rows including the individual developer's identity — finer attribution than the cleaned `environment` flag.

### Limitations & Drawbacks

**1. 18.7% of Rows Have No Outcome** 84/449 null `result_state`. Any success-rate metric depends on how these are treated (excluded? counted as failures?). The cleaned dataset silently resolved this by dropping them.

**2. Worst-in-Collection Success Rate** 77 COMPLETED of 449 (17.2%), or 21.1% of labeled rows. FAILED alone is 45.2%. Without retry-chain linkage (no parent-run ID) true end-to-end reliability is uncomputable — same flaw as cleaned.

**3. No Derived Columns At All** No `duration_seconds`, no `pipeline_name_clean`, no hour/weekday features. Every analysis must first parse ISO strings — and the timestamps are stored as **text, not Excel datetimes**.

**4. Extreme Hung-Run Outlier** Max derived duration 1,358,144 s ≈ 15.7 days — more than double the cleaned file's max (625,132 s). At least one run spans nearly half the observation window; means/std-devs are meaningless without trimming.

**5. Tiny Volume** 449 rows / 32 pipelines ≈ 14 runs per pipeline — no per-pipeline statistical baseline possible.

**6. No Error Detail, No Stage Granularity** Same as cleaned: failure cause and failing stage are unrecorded.

---

## 3. `databricks_job_runs_march2026.xlsx`

### What This Dataset Represents

The largest raw file: 836,700 job execution records over 34 days (Feb 28 18:30 – Apr 1 07:00 UTC) across 3 workspaces. **57,251 more rows than the cleaned CSV** (836,700 vs 779,449) — the delta is dominated by 28,140 null-outcome rows plus deduplication. Predominantly continuous data-copy workflows (`wf_copy_csvfiles_to_csvlake_prod_*`).

### Schema (10 Columns)

|Column|Type|Range / Values|
|---|---|---|
|`result_state`|Categorical|`SUCCEEDED` (800,305), **null (28,140)**, `CANCELLED` (3,921), `ERROR` (3,483), `SKIPPED` (848), `FAILED` (3)|
|`workspace_name`|String|`workspace3` (764,389 = 91.4%), `workspace2` (59,115 = 7.1%), `workspace1` (13,196 = 1.6%)|
|`workspace_id`|Integer|3 distinct IDs|
|`trigger_type`|Categorical|`CONTINUOUS` (674,801 = 80.7%), `CRON` (134,367 = 16.1%), `ONETIME` (23,432), `PERIODIC` (4,100)|
|`run_type`|Categorical|`JOB_RUN` (836,345), `SUBMIT_RUN` (355)|
|`job_name`|String|351 unique; **355 nulls** (all SUBMIT_RUN rows)|
|`job_id`|Float|457 unique; 355 nulls (SUBMIT_RUN)|
|`run_id`|Integer|**800,955 unique across 836,700 rows** — 35,745 duplicated run IDs|
|`period_start_time`|ISO 8601 string|2026-02-28 18:30 → 2026-04-01 06:59|
|`period_end_time`|ISO 8601 string|2026-02-28 18:30 → 2026-04-01 07:00|

Derived (not stored) duration: min **0.001 s**, p50 = 80.5 s, p95 = 599 s, p99 = 3,600 s, max **1,897,200 s (~22 days)**. The 28,140 null-outcome rows have a median derived duration of exactly 3,600 s — their `period_end_time` appears defaulted to start + 1 hour, so durations for unlabeled rows are synthetic, not measured.

### Advantages

**1. Massive Statistical Volume** 836,700 rows — even more than cleaned. Per-job baselines, confidence intervals, and periodicity analysis all statistically robust.

**2. Richer Trigger Taxonomy Than Cleaned** Four trigger types (`CONTINUOUS`/`CRON`/`ONETIME`/`PERIODIC`) with real counts. The cleaned dataset's description flattens this to "CONTINUOUS dominant"; the raw data preserves the 16% CRON scheduled workload as a distinct, analyzable class.

**3. `run_type` Actually Has Variance** 355 `SUBMIT_RUN` rows (one-off API-submitted runs, null `job_name`/`job_id`) exist in the raw data. The cleaned file's constant `JOB_RUN` column is an artifact of cleaning, not of the platform.

**4. In-Flight Runs Visible** 28,140 null `result_state` rows (3.4%) — runs still executing or with unreported outcomes at export time. Their presence lets you measure export-time censoring, which is invisible after cleaning.

**5. High Baseline Reliability** 99.0% of labeled rows SUCCEEDED — deviations are meaningful signal.

**6. Duplicate `run_id`s Enable Repair-Run Detection** 35,745 rows share a `run_id` with another row — consistent with repair/retry re-reporting of the same logical run. The cleaned dataset deduplicated these away; here the retry structure is at least partially recoverable.

### Limitations & Drawbacks

**1. Duplicate run_ids Are Also a Data-Quality Trap** The same 35,745 duplicates cut both ways: naive `groupby(run_id)` double-counts, and nothing distinguishes "repair run" from "export glitch". Every analysis must decide a dedup policy first.

**2. 3.4% Unlabeled Outcomes** 28,140 null `result_state` rows must be excluded or imputed; success-rate arithmetic changes depending on choice (95.65% raw vs 98.98% of labeled).

**3. Timestamps Stored as Text** ISO strings, not native datetimes. Parsing 1.67M timestamp strings is the dominant cost of loading this file.

**4. 55 MB Single-Sheet xlsx** Excel format is the worst possible container at this scale: full parse takes minutes and ~2 GB RAM (vs seconds for the equivalent CSV/parquet). Excel's 1,048,576-row sheet limit is also only ~25% above the current row count — one more month of data would not fit.

**5. Severe Workspace Imbalance** workspace3 = 91.4% of rows. Note the raw split (ws2: 59,115 > ws1: 13,196) differs from the cleaned file's reported split (ws1: 53,502 > ws2: 7,522) — the two versions disagree about which minor workspace dominates, a discrepancy worth auditing before trusting either.

**6. No Task-Level Decomposition, No Error Details, No Resource Metrics** Same blind spots as cleaned: job-level outcomes only, no error text, no DBU/cluster metrics.

**7. No Duration Column, and Synthetic End Times for In-Flight Runs** Duration must be derived from text timestamps for all 836,700 rows — and for the 28,140 unlabeled rows the derived value is meaningless (end time defaulted to start + 1 h). Max derived duration 1,897,200 s (~22 days) vs the cleaned file's tidy 62–3,524 s range: cleaning removed both the synthetic 1-hour rows and the genuine long-tail outliers, making cleaned duration statistics look far better-behaved than the platform actually was.

---

## 4. `databricks_queries_march2026.xlsx`

### What This Dataset Represents

60,000 statement execution records from **one evening**: 2026-03-06, 17:31 – 22:24 UTC (~4.9 hours). Same row count as the cleaned CSV, but with the **raw statement text preserved** and 2 source types. Crucially, all rows are from a **single workspace** (`workspace1`), and much of the "statement text" is actually **Python/PySpark driver code** (`spark.sql(query)`, `dbutils.fs.cp(...)`) rather than SQL — which explains the cleaned dataset's mysterious 72% `OTHER` classification.

### Schema (11 Columns)

|Column|Type|Range / Values|
|---|---|---|
|`workspace_name`|String|`workspace1` only (zero variance)|
|`workspace_id`|Integer|1 value|
|`execution_status`|Categorical|`FINISHED` (59,864), `FAILED` (136)|
|`statement_type`|Categorical|`OTHER` (43,441 = 72.4%), `SELECT` (7,358), `UPDATE` (3,241), `INSERT` (2,795), `SHOW` (1,003), `USE` (627), `REPLACE` (619), `MERGE` (570), `DESCRIBE` (199), `OPTIMIZE` (122), `CREATE` (14), `DELETE` (6), `TRUNCATE` (3), `ALTER` (1), null (1)|
|`source_id`|String|46 unique|
|`source_name`|String|46 unique; job names + `Serverless Starter Warehouse`|
|`source_type`|Categorical|`JOB` (57,040), `WAREHOUSE` (2,960)|
|`statement_text`|String (raw)|9 – 18,329 chars, median 41; 1,700 unique|
|`start_time` / `end_time`|ISO 8601 string|2026-03-06 17:31 → 22:24 UTC|
|`total_duration_ms`|Integer|25 – 522,901 ms; p50 = 246, p95 = 5,715, p99 = 18,623|

### Advantages

**1. Raw Statement Text — Explains the `OTHER` Mystery** Statements up to 18,329 chars are preserved. Inspection shows the dominant "queries" are driver-side code snippets (`spark.sql(update_query)`, `dbutils.fs.cp(csv_file.path, target_path)`) — i.e., the 72.4% `OTHER` classification reflects genuinely non-SQL statements, not a broken classifier. The cleaned CSV (text_length capped at 59 chars) makes this undiagnosable.

**2. WAREHOUSE Queries Present** 2,960 rows from `Serverless Starter Warehouse` (source_type `WAREHOUSE`) — interactive/SQL-warehouse traffic the cleaned file dropped (cleaned is 100% `JOB`). Warehouse queries fail at 0.68% (20/2,960) vs jobs' 0.20% (116/57,040) — a 3.3× difference in failure rate lost in cleaning.

**3. Full Duration Tail Preserved** Max 522,901 ms (~8.7 min) vs cleaned max 22,065 ms — cleaning trimmed or excluded the slowest ~24× tail. For SLA breach and outlier analysis, only the raw file has the actual worst cases.

**4. Millisecond Duration + 60k Rows** Strong within-day statistical power; per-source and per-type duration profiles robust.

**5. Failure Sample Analyzable** 136 failures skew to `OTHER` (95), `INSERT` (15), `DESCRIBE` (15) — cross-referenced with raw text, failure signatures are recoverable.

### Limitations & Drawbacks

**1. Single Evening — Most Severe Temporal Limitation in Collection** ~4.9 hours of one day. No typicality baseline, no weekly patterns, no growth trends. Any model trained here learns March 6th evening, nothing more.

**2. Zero Workspace Variance** `workspace_name` and `workspace_id` are constants. (The cleaned dataset's documentation claims multiple workspaces — the raw data contradicts this.)

**3. Statement Text Is Often Not the Query** The flip side of advantage 1: `spark.sql(query)` tells you a variable was executed, not what SQL it contained. True query-level analysis is impossible for the majority of rows despite text being "present".

**4. One Null `statement_type`** Minor, but nonzero — schema not fully clean.

**5. No Complexity Flags, No Temporal Features** `has_join`, `start_hour` etc. are cleaning-stage additions; here every feature must be derived from raw text/timestamps.

**6. No Table/Schema Metadata, No Concurrency Context** Same as cleaned: hot tables and load-dependent latency unanalyzable.

---

## 5. `Utilization.xlsx`

### What This Dataset Represents

43,296 hourly Azure Monitor metric samples over 32 days (Feb 28 – Mar 31, 2026) for 18 resources in 7 categories, almost all in the `PowerBI-Prod` resource group. Contrary to the cleaned dataset's apparent network-only scope, the raw file carries **23 distinct metrics across 6 units** — including CPU (`cpu_percent`, `Percentage CPU`, `CpuPercentage`), memory (`MemoryPercentage`, `AverageMemoryWorkingSet`), SQL DTU (`dtu_consumption_percent`, `dtu_used`), storage transactions/ingress/egress, disk IOPS, sessions, function executions, and DDoS packets. The single sheet is named `in`, suggesting a raw ETL landing export — and the metric-value columns are stored as **text**, with literal PowerShell object-serialization artifacts (`@{$numberDouble=NaN}`) leaked into the data.

### Schema (24 Columns)

|Column|Type|Range / Values|
|---|---|---|
|`service_account_id`|Hash string|1 value (zero variance)|
|`from_date` / `to_date`|ISO string|Daily boundaries, Feb 28 – Mar 31|
|`updated_at`|ISO string|All Mar 30–31 (export ran once at end of window)|
|`category`|Categorical|`Storage_Accounts` (16,128), `MS-SQL_DB` (7,680), `Web_Apps` (6,432), `Appservice_Plan` (6,144), `Virtual_Machines` (3,072), `Disks` (3,072), `Public_IP_Address` (768)|
|`metric_name`|String|23 unique: `Transactions`, `Egress`, `Ingress`, `BytesReceived`, `FunctionExecutionCount`, `cpu_percent`, `dtu_consumption_percent`, `dtu_used`, `CpuPercentage`, `MemoryPercentage`, `sessions_count`, disk ops/bytes, `Percentage CPU`, `PacketsInDDoS`, `\LogicalDisk(_Total)\% Free Space`, …|
|`metric_display_name` / `metric_group`|String|Mirror `metric_name` (23 values each)|
|`metric_config_type`|String|Always `default_monitoring` (zero variance)|
|`resource_name`|String|18 unique|
|`resource_id` / `resourceid`|String|20 unique full Azure resource IDs (duplicate columns)|
|`service_name`|—|**100% null**|
|`namespace`|String|7 Azure namespaces|
|`unit`|Categorical|`Bytes` (18,528), `Count` (11,712), `Percent` (8,448), `CountPerSecond` (2,304), `BytesPerSecond` (1,536), `Unspecified` (768)|
|`zone`|—|**100% null**|
|`resource_group`|String|`PowerBI-Prod` (40,992), `cloud-shell-storage-centralindia` (2,304)|
|`service_resource_uri`|String|7 category URIs|
|`mv_timeStamp`|ISO string|768 unique hourly stamps = 32 days × 24 h|
|`mv_average`|**Text** (numeric)|0 – 57,012,290; 112 rows contain literal `@{$numberDouble=NaN}`|
|`mv_minimum`|**Text** (numeric)|3,840 null + 198 `NaN`-artifact rows; 58.3% of valid values are 0|
|`mv_maximum`|**Text** (numeric)|0 – 5,221,688,045; same null/artifact pattern|
|`mv_total`|**Text** (numeric)|0 – 271,042,913,569|
|`mv_count`|**Text** (numeric)|0 – 66,010; mode is **0** (12,748 rows), then 360/240/300|

### Advantages

**1. Full Metric Breadth — CPU, Memory, DTU Included** The raw file answers the cleaned dataset's biggest blind spot: compute metrics exist. `cpu_percent` (3,072 rows), `Percentage CPU` (768), `CpuPercentage`/`MemoryPercentage` (1,536 each), DTU metrics (3,072) enable actual compute-bottleneck diagnosis, capacity planning, and SQL DTU headroom analysis.

**2. Best Temporal Coverage in Collection** 32 complete days × 24 hourly samples per resource-metric. Daily and weekly cycles fully observable; strongest dataset for forecasting.

**3. Hourly Statistical Aggregates** avg/min/max/total/count per hour — `mv_maximum` catches bursts that averages smooth over.

**4. Authoritative Azure Identifiers** Full resource IDs, namespaces, and resource groups (`PowerBI-Prod` vs `cloud-shell-storage-centralindia`) allow exact cross-referencing with Azure billing/management APIs.

**5. Honest Minimums and Counts** Unlike the cleaned CSV (where `mv_minimum` is uniformly 0 and `mv_count` pinned at 5,520), the raw data has real variance: 41.7% of minimums are non-zero, and `mv_count` spans 0–66,010. The cleaned versions of these columns appear to be fill-value artifacts; the raw file is the only trustworthy source for them.

**6. Direct SQL-Layer Linkage** The monitored SQL databases (`cspbisqlserverprod/PowerBIprod`, replica, `master`) are the same `PowerBIprod` database whose sessions appear in `DBQuery_Statistics.xlsx` — the only strong cross-dataset join in the collection.

### Limitations & Drawbacks

**1. Metric Values Stored as Text with Serialization Garbage** All five `mv_*` columns are strings. 112–198 rows per column contain the literal string `@{$numberDouble=NaN}` — a PowerShell rendering of a MongoDB extended-JSON NaN that leaked through the export pipeline. Every consumer must run `to_numeric(errors="coerce")` and decide NaN policy before any math.

**2. 3,840 Rows With Null Metric Values** All min/max/total/count nulls concentrate in `dtu_used` (1,536), `dtu_consumption_percent` (1,536), and `PacketsInDDoS` (768) — i.e., the DTU metrics that are this file's headline advantage are also its most incomplete.

**3. Two Dead Columns, Three Constant Columns** `service_name` and `zone` are 100% null; `service_account_id` and `metric_config_type` have zero variance; `resource_id` and `resourceid` are duplicates. Nearly a quarter of the schema carries no information.

**4. `mv_count = 0` on 12,748 Rows** Rows claiming zero samples in the hour yet often carrying an `mv_average` — internally inconsistent aggregation, likely sparse-metric hours backfilled inconsistently.

**5. Metric Name Chaos** CPU appears under three different names (`cpu_percent`, `Percentage CPU`, `CpuPercentage`) depending on resource type; deprecated metrics (`Network In Billable (Deprecated)`) and preview metrics mix freely. Cross-category comparison requires a manual metric-mapping table.

**6. Category Imbalance** Storage Accounts 37% of rows; Public IPs 1.8%. Same as cleaned.

**7. Single Subscription, Single Export** All data from one subscription; `updated_at` clusters on Mar 30–31, so everything is one retrospective export — no incremental lineage.

---

## Original vs Cleaned — What Cleaning Did

|Aspect|Original|Cleaned|Consequence|
|---|---|---|---|
|Pipeline rows|449|316|84 null-outcome + ~113 workspace2/other rows dropped; workspace2's cancel-storm hidden|
|Job rows|836,700|779,449|Null outcomes + duplicate run_ids removed; retry structure lost|
|Job `run_type`|`JOB_RUN` + `SUBMIT_RUN`|constant|Variance removed by cleaning, not absent in platform|
|Job triggers|4 types incl. CRON 16%|"CONTINUOUS dominant"|Scheduled-workload class blurred|
|Pipeline triggers|8 incl. `INFRASTRUCTURE_MAINTENANCE`|7|Trigger class dropped|
|Query text|raw, 9–18,329 chars|length/flags only|`OTHER`=72% becomes explainable (PySpark driver code)|
|Query sources|JOB + WAREHOUSE|JOB only|Interactive warehouse traffic (higher failure rate) dropped|
|Query duration max|522,901 ms|22,065 ms|Slowest tail trimmed ~24×|
|Job duration (derived)|0.001 s – 22 days, incl. synthetic 1 h end-times|62–3,524 s|Both fake and genuine extremes removed|
|SQL statement text (DBQuery)|full SQL + proc names|boolean flags|Table-level root cause lost|
|Utilization `mv_minimum`|58% zero, real variance|always 0|Cleaned column is a fill artifact|
|Utilization `mv_count`|0–66,010, mode 0|constant 5,520|Cleaned column is a fill artifact|
|Utilization metrics|23 incl. CPU/memory/DTU|network-dominant view|Compute metrics exist in raw|
|Timestamps|ISO text strings|parsed + hour/day features|Originals need parsing everywhere|

**Rule of thumb:** use the cleaned CSVs for modeling convenience; return to these originals whenever the question involves outliers, failures, retry structure, raw SQL, warehouse traffic, or any metric the cleaning step normalized.

---

## Cross-Dataset Relationships

### Workspace Linkage (Raw Counts)

|Workspace|pipeline_runs|job_runs|queries|
|---|---|---|---|
|workspace1|302 (67%)|13,196 (1.6%)|60,000 (100%)|
|workspace2|147 (33%)|59,115 (7.1%)|—|
|workspace3|—|764,389 (91.4%)|—|

workspace1 hosts orchestration and all captured SQL activity; workspace3 executes the overwhelming job volume. Note this contradicts the cleaned job_runs distribution (which reports ws1 7% / ws2 1%) — audit before relying on either.

### Inferred Processing Hierarchy

```
pipeline_runs  →  job_runs  →  queries
(orchestration)   (execution)  (statements, incl. PySpark driver code)

        ↕                              ↕
Utilization.xlsx  ←──────────→  DBQuery_Statistics.xlsx
(Azure metrics incl.            (sessions on cspbisqlserverprod/
 cspbisqlserverprod DTU)         PowerBIprod — same server)
```

The strongest raw-data join: `Utilization.xlsx` monitors SQL servers `cspbisqlserverprod`/`cspbisqlserverprodreplica` (database `PowerBIprod`), and `DBQuery_Statistics.xlsx` captures sessions executing `[PowerBIprod].[etl].*` procedures — the same database, from two vantage points (Azure Monitor outside, DMV snapshot inside).

### Temporal Alignment

|Dataset|Coverage|
|---|---|
|`databricks_job_runs`|Feb 28 18:30 – Apr 1 07:00 (34 days)|
|`Utilization`|Feb 28 – Mar 31 (32 days)|
|`databricks_pipeline_runs`|Mar 1 – Apr 1 (31 days)|
|`databricks_queries`|Mar 6, 17:31–22:24 only|
|`DBQuery_Statistics`|Apr 1 ~07:15 snapshot|

The DBQuery snapshot (Apr 1 07:15) falls **inside** the job_runs window (ends Apr 1 07:00, within minutes) — near-simultaneous SQL-session and job-execution capture, likely the same export event.

---

## Collective Limitations

**1. Excel as Interchange Format** All five files are xlsx. Numbers become text (`Utilization`), timestamps become strings (all Databricks files), a 55 MB file takes minutes to parse, and the 1,048,576-row sheet ceiling is one month of growth away for job_runs. First step of any pipeline must be conversion to CSV/parquet.

**2. Null Outcome Labels** 84 pipeline + 28,140 job rows lack `result_state`. Every reliability metric requires an explicit policy for these.

**3. No Ground-Truth Anomaly Labels, No Business Context, No Lineage, No Cost Metrics** Same as the cleaned collection: no `is_anomaly`, no SLA tiers, no source→pipeline→dashboard mapping, no DBU/Azure cost columns.

**4. Fragmented Time Windows** Only Mar 6 evening is covered by 4 datasets simultaneously; queries and the DBQuery snapshot never overlap.

**5. Retry Chains Unlinked** No parent-run/predecessor IDs anywhere; duplicate job `run_id`s hint at repairs but cannot be confirmed.

**6. Vendor-Specific** Azure Monitor + Databricks semantics throughout; not portable to other clouds without remapping.
