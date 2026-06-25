# MLOps Assignment: Airflow Pipeline for Coding-Agent Evaluation

## Goal

The goal of this assignment was to turn ad-hoc mini-swe-agent and SWE-bench scripts into a configurable, reproducible, and observable Airflow pipeline.

The implemented DAG is:

dags/evaluate_agent.py

The pipeline runs a coding agent on a small SWE-bench subset, evaluates the produced predictions, writes a structured run folder, and logs run metadata to MLflow.

## Pipeline Architecture

The Airflow DAG is named:

evaluate_agent

It contains four main tasks:

prepare_run -> run_agent -> run_eval -> summarize_and_log

### prepare_run

Creates a reproducible run directory:

runs/<run_id>/

and writes:

config.json

The config stores the run ID, project root, timestamp, and Airflow parameters.

### run_agent

Runs mini-swe-agent through uv run.

The DAG supports two modes:

single
batch

For the completed evaluation run, batch mode was used.

The agent writes artifacts under:

runs/<run_id>/run-agent/

Important outputs include:

agent.log
status.json
trajectories/
trajectories/preds.json

The task includes a configurable timeout and Docker cleanup logic to avoid stuck agent containers.

### run_eval

Runs SWE-bench evaluation through the project environment:

uv run python -m swebench.harness.run_evaluation

The evaluation reads:

runs/<run_id>/run-agent/trajectories/preds.json

and writes logs and reports under:

runs/<run_id>/run-eval/

### summarize_and_log

Collects statuses and writes:

metrics.json
manifest.json
mlflow.json

It also logs parameters, metrics, run ID, and artifact path to MLflow.

## Airflow Parameters

The DAG is configurable through Airflow parameters.

Important parameters:

run_id
mode
subset
split
model
instance_id
task_slice
workers
cost_limit
agent_timeout_seconds
mlflow_tracking_uri
mlflow_experiment_name

For the final validation run:

run_id = batch_phase1_003
mode = batch
subset = verified
split = test
task_slice = 0:1
workers = 1
model = nebius/moonshotai/Kimi-K2.6

## Completed Run

The completed validation run was:

batch_phase1_003

This run validated the full Phase 1 workflow:

run_agent -> run_eval

Final metrics:

{
  "agent_succeeded": 1,
  "artifact_count": 12,
  "eval_skipped": 0,
  "eval_succeeded": 1,
  "mlflow_logged": 1
}

SWE-bench evaluation summary:

Instances submitted: 1
Instances completed: 1
Instances resolved: 1
Instances with errors: 0

This confirms that the agent produced a valid preds.json, SWE-bench evaluation ran successfully, and the run was logged to MLflow.

## Artifact Layout

Runtime artifacts are written to:

runs/<run_id>/

Example structure:

runs/batch_phase1_003/
  config.json
  metrics.json
  manifest.json
  mlflow.json
  run-agent/
    agent.log
    status.json
    trajectories/
      preds.json
      minisweagent.log
      <instance trajectory>
  run-eval/
    eval.log
    status.json
    reports/
      <swebench report>.json

The runs/ directory is ignored by Git because it contains runtime artifacts.

A small sanitized evidence sample is committed under:

sample/evaluate_agent_batch_phase1_003/

This sample includes metadata, statuses, predictions, metrics, MLflow status, and a small SWE-bench report.

## MLflow Tracking

MLflow logging is enabled inside the DAG.

The local backend uses SQLite:

sqlite:///mlflow.db

The database is ignored by Git.

For the completed run, MLflow logging succeeded and produced an MLflow run ID recorded in:

runs/batch_phase1_003/mlflow.json

To inspect MLflow locally:

uv tool run --with mlflow mlflow ui --backend-store-uri sqlite:///mlflow.db --host 0.0.0.0 --port 5000

## How to Run

Start Airflow:

source .venv/bin/activate
set -a
source .env
set +a
bash run-airflow-standalone.sh

Then trigger the evaluate_agent DAG from the Airflow UI.

Example params:

{
  "run_id": "batch_phase1_003",
  "mode": "batch",
  "subset": "verified",
  "split": "test",
  "model": "nebius/moonshotai/Kimi-K2.6",
  "task_slice": "0:1",
  "workers": 1,
  "cost_limit": 0,
  "agent_timeout_seconds": 900,
  "mlflow_tracking_uri": "",
  "mlflow_experiment_name": "coding-agent-evaluation"
}

## Notes and Tradeoffs

This implementation follows the standalone Airflow speedrun path.

The DAG uses uv run for agent and evaluation execution so both steps run in the project environment.

DockerOperator was not implemented in this iteration, but the pipeline is structured so the agent and evaluation steps can later be moved into DockerOperator or KubernetesPodOperator.

Remote object storage was not implemented in this iteration. Instead, the DAG writes a clear local runs/<run_id>/ folder and a sanitized sample is committed for reproducibility evidence.
