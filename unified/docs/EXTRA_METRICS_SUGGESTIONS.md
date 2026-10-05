# Extra metrics for a paper or a project report

These are measurements that [PAPER_RESULTS.md](PAPER_RESULTS.md) does not already
specify. Do not repeat the numbers that file already holds, and do not replace
§B1, §B2, §B5, or §B13.2.

The catalogue already asks for the items a paper should do first. They are not
copied here:

- Time a person on the same two pipelines (§B8.2). Do not publish a guessed minute count.
- Call Groq on the 24 prompts with the repair layer removed, three repeats (§B10, §A4).
- Run each of the six live pipelines a third time so the cost agent is allowed to change a setting, and run the paired copies with the planner's DIU and shuffle left in place (§B2, §B10, §7).
- Run the plans the performance gate aborted, once with the gate not applied (§B5.4, §B10).
- Repeat the local fine-tuned planner three times, which needs Ollama (§B10).

Until those exist, write the work as a systems case study. Twenty live runs and 24
prompts are enough for that. They are too small for a general benchmark.

## For the research paper

### 1. Check that the output values are right, not only the row count

**Review 2026-10-05: the row counts already fail this check.** The benchmark CSVs come
from fixed seeds, so the expected filter output is known exactly: "region = 'EU' and
quantity > 5" keeps **172, 7,623 and 59,817** rows (the 2026-10-03 runs wrote exactly those).
The 2026-10-05 runs wrote **924, 45,172 and 359,921** — about 90% of the input instead of
about 15% — so the row counts do *not* match the request (PAPER_RESULTS §B13.2). Those runs
completed but are not correct. First inspect their saved plans to find which filter ran.
Then, for any run, check both the count and the values:

On the saved outputs, or on one fresh run of each shape, compare a reference query
computed locally from the same CSV: the sum of `quantity` after each filter, and the
five region averages after the aggregation. Report match or mismatch per pipeline.
One mismatch is a result. Do not describe a row-count match as value correctness
until this is done.

### 2. Take per-stage times from Azure

The resource factor is learned from one run total, split across stages in proportion
to the prediction. Copy and notebook errors are therefore not independent, and the
8.3% figure in §B14 is a total, not a per-stage error — and it is in-sample (the factors
were learned from those same runs), so per-stage times should be taken on a new batch.

For one run of each of the three file sizes, record the ADF activity duration and
the Databricks job duration from the run history. Compare each to that stage's
resource estimate. Three runs are enough to say whether the total error is hiding a
copy error and a notebook error of opposite sign.

### 3. Put the formula cost next to the Azure bill

`actual_cost_usd` is the cost agent's formula with the measured duration substituted
in. It is not the invoice. §B14's 14.3% and 83.3% are errors against that formula (and
§B14 is in-sample).

From Cost Management, export the resource-group charges for the day of the 12-run
batch and for one later single run whose start and end you know. Report the invoice
total beside the sum of `actual_cost_usd` for those runs. If the invoice cannot be
separated per run, say so and publish only the day total. Do not relabel the formula
as the bill.

### 4. Separate startup time from data time

Across 1,000 to 400,000 rows the executions stay near two to three minutes. That
flatness is already in §B13.2. What is not measured is how much of it is cluster
and copy startup.

Run one pipeline twice, back to back, on the 24 MB file. The first execution
includes cold start. The second, while the workspace is warm, is the closer figure
for data processing. Report both. A duration model that cannot beat the cold-start
floor should be described that way.

### 5. Rephrase the same request

§B1 and §B13 use one wording per prompt. Repeating that wording measures the
model's sampling noise. It does not measure whether a different sentence for the
same job produces the same pipeline.

Write five rewordings of each of six requests (the two sales shapes plus four
others already in the 24). Score the fraction of rewordings that keep the same
stage types, the same filter columns, and the same aggregation. Do this offline.
No Azure.

### 6. Have a person judge the intent labels

Correctness in §B1 and §B13.1 is a regular expression over the plan. The ten Groq
misses are "compiled but failed the regex", "matched the regex but did not compile",
or both. A reviewer can ask whether the regex rejected a plan a person would accept.

Two people, working separately, label all 24 Groq plans as matching the request or
not. Report agreement between the two people, and agreement between the people and
the regex. Disagreements get listed, not averaged away.

### 7. Run one dataset that is not the sales file

The 12-run batch is one schema at three sizes. The booking run is a single extra
pipeline. A case-study reader will ask whether the planner and the duration numbers
depend on that sales layout.

Pick one public CSV with different columns, already small enough for the student
tier. Plan it through the shipped path, execute it once, and score it with the same
row-count check, the value check from item 1, and the resource estimate. One run.
Report it beside the sales numbers, not in place of them.

### 8. Compare with the product's own wizard

§B8 compares the system with a person clicking through the portal. That is not a
comparison with Azure's Copy Data tool plus a notebook job created from the
workspace UI, which is the tool a reader will name.

On the same two tasks as §B8.2, one person builds the pipeline with Copy Data and
the Databricks UI, and one person uses this system. Record time to first correct
output, failed attempts, and whether the output matches item 1. This can be the
same session as the human study. Keep the two conditions in separate rows.

## For the project report

These belong in a report appendix more than in a paper's results section.

### 9. Trace every required agent to a measured section

Build a table from the responsibility list: agent, what it must do, the section of
PAPER_RESULTS.md that measures it, and the gap if the cell is still open. The open
cells should be the ones named in §B10 and in this file, not a new experiment.

### 10. Time a clean setup

On a machine that does not already have the checkout running, record the time from
a documented install to one successful Run pipeline, and every step that failed on
the way. One person is enough. This is a reproducibility appendix, not a quality metric.

### 11. Show one live rejection

The assurance numbers are 1,400 injected plans offline. A report reader expects one
end-to-end trace: a plan that the gate stops, the decision-log line, and a
confirmation that no Azure job started. Use a fault already in the §B3 set, such as
a filter on a column that is not in the file. Do not weaken the gate to produce the
screenshot.

## Compute from the files you already have

No new cloud runs.

- **Interval on the planner.** From the existing 24 paired outcomes, report a
  confidence interval or a paired test for fine-tuned versus base, and for Groq
  versus the fine-tuned model. The point estimates 96% and 58.3% stay as they are.
- **Interval on the 12 duration errors.** Only after an out-of-sample batch: the 8.3% and
  17.8% in `rescore_after_resource_fix.json` are in-sample fits (§B14), so an interval on
  them would still describe fit, not prediction. Bootstrap the errors of the new batch.
- **Scaling sentence.** From the same 12 executions, state the slope of execution
  time against row count. The measured times are already flat (means 155 s, 135 s,
  and 162 s). The sentence belongs next to §B14 so a reader does not infer that the
  estimator was tested on a workload that grows with the file.

## Leave out of both documents

- The 8.8% in-distribution runtime error, unless a model is retrained without the
  `baseline_s` input. The out-of-sample figure is 51.1%, and that model file is not
  in this checkout. §B14's formula numbers are the current runtime result.
- The anomaly detector's 100% precision and recall. Those signals were planted.
- Worker and DIU exact-match against the synthetic labeler (96% and 98%). That
  measures agreement with the label generator.
- Any estimated human minutes.
- A new Azure batch whose only purpose is to reprint §B14. The DIU and shuffle on
  those plans do not change, so the executions already measured are the right ones.
