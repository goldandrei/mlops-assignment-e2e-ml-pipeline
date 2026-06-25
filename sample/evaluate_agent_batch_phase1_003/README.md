# evaluate_agent batch sample

This folder contains a sanitized sample from a completed Airflow batch run.

Run ID: batch_phase1_003

This run validates the full Phase 1 workflow:

prepare_run -> run_agent -> run_eval -> summarize_and_log

Key result:
- agent_succeeded: 1
- eval_skipped: 0
- eval_succeeded: 1
- mlflow_logged: 1

The full runtime folder is intentionally not committed because /runs/ is ignored.
This sample keeps only small metadata and evidence files required to understand the run.
