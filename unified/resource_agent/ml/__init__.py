"""
ML sub-package for the Resource Agent.

The Resource Agent's job is *resource management*: given a pipeline stage and
its data, recommend the best compute settings (workers, DIU, peak memory,
shuffle partitions, node type) that fit the student-tier hard limits.

This package holds the supervised-regression pieces:
  - feature_spec.py : the single source of truth for the feature/target contract
                      shared by the data generator, the trainer, and the
                      runtime predictor (so they can never drift).
  - calibration.py  : constants derived from the real telemetry in Datasets/
                      (job_runs, pipeline_runs, queries, dbquery_statistics,
                      utilization) that ground the synthetic label generator.

Duration / runtime / SLA prediction deliberately lives in the Performance
Prediction Agent, not here — see RESPONSIBILITIES.md.
"""
