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

runs/RUN_ID/

and writes:

config.json

The config stores the run ID, project root, timestamp, and Airflow parameters.

### run_agent

Runs mini-swe-agent through uv run.

The evaluated submission uses batch mode.

batch

Batch mode is the supported evaluation path because it produces trajectories/preds.json, which is required by SWE-bench evaluation.

The agent writes artifacts under:

runs/RUN_ID/run-agent/

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

runs/RUN_ID/run-agent/trajectories/preds.json

and writes logs and reports under:

runs/RUN_ID/run-eval/

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

runs/RUN_ID/

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
      instance_trajectory
  run-eval/
    eval.log
    status.json
    reports/
      swebench_report.json

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

Remote object storage was not implemented in this iteration. Instead, the DAG writes a clear local runs/RUN_ID/ folder and a sanitized sample is committed for reproducibility evidence.

## Submission Checklist

This repository includes the minimum working submission requested in the assignment.

- Configurable Airflow DAG: `dags/evaluate_agent.py`
- DAG tasks: `prepare_run`, `run_agent`, `run_eval`, `summarize_and_log`
- Required Airflow params: `split`, `subset`, `workers`
- Additional useful params: `model`, `task_slice`, `run_id`, `cost_limit`
- Agent execution: `uv run mini-extra swebench`
- Evaluation execution: `uv run python -m swebench.harness.run_evaluation`
- Runtime artifact layout: `runs/RUN_ID/`
- Sanitized evidence sample: `sample/evaluate_agent_batch_phase1_003/`
- MLflow logging: params, metrics, run ID, and artifact path
- Completed evaluation evidence: `eval_succeeded = 1`

## Evidence Files Committed

The full `runs/` directory is intentionally ignored because it contains runtime artifacts.

Instead, the repository includes this small sanitized sample:

sample/evaluate_agent_batch_phase1_003/

Important files:

- `config.json`: run configuration and parameters
- `agent_status.json`: agent command, output path, and return code
- `eval_status.json`: SWE-bench evaluation command and return code
- `metrics.json`: final pipeline metrics
- `mlflow.json`: MLflow tracking evidence
- `preds.json`: model patch submitted to SWE-bench
- `swebench_report.json`: SWE-bench evaluation report
- `manifest.json`: important artifact references

## Completed Run Evidence

The completed run was:

batch_phase1_003

Final metrics:

agent_succeeded = 1
eval_skipped = 0
eval_succeeded = 1
mlflow_logged = 1

SWE-bench completed one submitted instance successfully:

Instances submitted: 1
Instances completed: 1
Instances resolved: 1
Instances with errors: 0

## Rerun Instructions

To rerun the same pipeline with a new run ID:

1. Start Airflow:

source .venv/bin/activate
set -a
source .env
set +a
bash run-airflow-standalone.sh

2. Open Airflow on port 8080.

3. Trigger the `evaluate_agent` DAG.

4. Use similar parameters, but change `run_id` to a new value, for example:

batch_rerun_001

5. After completion, inspect:

runs/batch_rerun_001/config.json
runs/batch_rerun_001/run-agent/status.json
runs/batch_rerun_001/run-agent/trajectories/preds.json
runs/batch_rerun_001/run-eval/status.json
runs/batch_rerun_001/metrics.json
runs/batch_rerun_001/manifest.json
runs/batch_rerun_001/mlflow.json

## Production Follow-up

This submission follows the standalone Airflow speedrun path.

The assignment notes that a production-style solution can improve the pipeline with DockerOperator, docker-compose deployment, and remote Object Storage.

Those additions were not implemented in this iteration. The current implementation still keeps the run reproducible by:

- running agent and evaluation through the project `uv` environment
- writing all runtime evidence under `runs/RUN_ID/`
- logging run parameters and metrics to MLflow
- committing a sanitized sample run for review
- ignoring local runtime artifacts and local MLflow databases in Git

The next production step would be to move `run_agent` and `run_eval` from local subprocess calls into DockerOperator tasks using the provided Dockerfile, then upload `runs/RUN_ID/` to S3/Object Storage and log the remote URI to MLflow.

## Docker Compose Deployment

A production-style addition is provided through:

docker-compose.yaml

It starts two services:

- `airflow`: standalone Airflow service on port 8080
- `mlflow`: MLflow tracking server on port 5000

The Airflow service mounts the project directory, uses the project DAGs, loads `.env`, and forwards the Docker socket so SWE-bench evaluation containers can be created by the harness.

To start the compose stack:

docker compose up --build

Then open:

Airflow: http://localhost:8080
MLflow: http://localhost:5000

In compose mode, the DAG can log to MLflow through:

MLFLOW_TRACKING_URI=http://mlflow:5000

The local standalone path is still supported through:

bash run-airflow-standalone.sh


## Evaluation Mode Decision

The graded evaluation path uses batch mode only.

Single-instance ad-hoc agent execution can be useful for local debugging, but the SWE-bench evaluation step expects a predictions file at:

runs/RUN_ID/run-agent/trajectories/preds.json

Batch mode produces this file directly, so the Airflow DAG restricts the submitted evaluation flow to batch mode. This avoids a misleading configuration where the agent runs but evaluation is skipped because preds.json does not exist.

## Airflow Image Improvement

The compose deployment uses a dedicated `Dockerfile.airflow`.

This avoids installing system packages, Docker CLI, curl, and uv on every `docker compose up`.

The Airflow service now builds a reusable image with:

- Python 3.12
- uv
- project dependencies
- Docker CLI for SWE-bench harness container execution

This makes the compose setup faster and more reproducible than installing dependencies at container startup.

## Compose Image Wiring

The Airflow compose service is wired to the reusable Airflow image through:

airflow:
  build:
    context: .
    dockerfile: Dockerfile.airflow

This means the Airflow container no longer installs curl, Docker CLI, uv, and project dependencies on every startup.

The image build happens once through Docker Compose, and startup uses the `CMD` defined in `Dockerfile.airflow`.

The compose file still mounts:

- `./runs:/project/runs` for runtime artifacts
- `/var/run/docker.sock:/var/run/docker.sock` so the SWE-bench harness can create evaluation containers
- `airflow_home:/airflow` for persistent Airflow state

## SWE-bench Metrics Logged to MLflow

The DAG parses the SWE-bench JSON report produced under:

runs/RUN_ID/run-eval/reports/

and extracts numeric evaluation metrics into `metrics.json`.

Examples of metrics extracted when present:

- `swebench_report_found`
- `swebench_total_instances`
- `swebench_submitted_instances`
- `swebench_completed_instances`
- `swebench_resolved_instances`
- `swebench_unresolved_instances`
- `swebench_empty_patch_instances`
- `swebench_error_instances`
- `swebench_resolve_rate`

These metrics are passed through the existing MLflow logging path, so evaluation quality is visible in MLflow instead of only in the raw SWE-bench report.
