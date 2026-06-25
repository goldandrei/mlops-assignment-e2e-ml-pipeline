import json
import os
import signal
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")

def _count_report_value(value):
    """Convert SWE-bench report values into numeric MLflow-friendly metrics."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, (list, tuple, set)):
        return len(value)
    if isinstance(value, dict):
        return len(value)
    return None


def extract_swebench_metrics(run_dir: Path) -> dict:
    """Extract useful numeric metrics from the SWE-bench JSON report.

    The exact report schema may vary across SWE-bench versions, so this parser
    accepts several common key names and converts lists/dicts to counts.
    """
    reports_dir = run_dir / "run-eval" / "reports"
    reports = sorted(reports_dir.glob("*.json"))

    metrics = {
        "swebench_report_found": int(bool(reports)),
    }

    if not reports:
        return metrics

    try:
        report = json.loads(reports[0].read_text(encoding="utf-8"))
    except Exception:
        metrics["swebench_report_parse_error"] = 1
        return metrics

    metrics["swebench_report_parse_error"] = 0

    key_groups = {
        "swebench_total_instances": [
            "total_instances",
            "total",
        ],
        "swebench_submitted_instances": [
            "submitted_instances",
            "instances_submitted",
            "submitted_ids",
        ],
        "swebench_completed_instances": [
            "completed_instances",
            "instances_completed",
            "completed_ids",
        ],
        "swebench_resolved_instances": [
            "resolved_instances",
            "instances_resolved",
            "resolved_ids",
        ],
        "swebench_unresolved_instances": [
            "unresolved_instances",
            "instances_unresolved",
            "unresolved_ids",
        ],
        "swebench_empty_patch_instances": [
            "empty_patch_instances",
            "instances_with_empty_patches",
            "empty_patch_ids",
        ],
        "swebench_error_instances": [
            "error_instances",
            "instances_with_errors",
            "error_ids",
        ],
    }

    for metric_name, possible_keys in key_groups.items():
        for key in possible_keys:
            if key in report:
                value = _count_report_value(report.get(key))
                if value is not None:
                    metrics[metric_name] = value
                    break

    # Some SWE-bench versions store report fields under a nested summary key.
    summary = report.get("summary") if isinstance(report, dict) else None
    if isinstance(summary, dict):
        for metric_name, possible_keys in key_groups.items():
            if metric_name in metrics:
                continue
            for key in possible_keys:
                if key in summary:
                    value = _count_report_value(summary.get(key))
                    if value is not None:
                        metrics[metric_name] = value
                        break

    submitted = metrics.get("swebench_submitted_instances")
    resolved = metrics.get("swebench_resolved_instances")
    if submitted:
        metrics["swebench_resolve_rate"] = float(resolved or 0) / float(submitted)

    return metrics



def run_command(command: list[str], cwd: Path, env: dict, log_path: Path, timeout_seconds: int) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        try:
            return process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            log_file.write(f"\nTIMEOUT: command exceeded {timeout_seconds} seconds\n")
            log_file.flush()
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            return 124

def cleanup_minisweagent_containers() -> list[str]:
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", "name=minisweagent", "-q"],
        text=True,
        capture_output=True,
        check=False,
        timeout=15,
    )

    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    if container_ids:
        subprocess.run(
            ["docker", "rm", "-f", *container_ids],
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

    return container_ids


def log_mlflow_run(config: dict, metrics: dict, run_dir: Path) -> dict:
    params = config["params"]
    tracking_uri = params.get("mlflow_tracking_uri") or os.environ.get("MLFLOW_TRACKING_URI") or f"sqlite:///{PROJECT_ROOT / 'mlflow.db'}"
    experiment_name = params.get("mlflow_experiment_name") or "coding-agent-evaluation"

    try:
        import mlflow

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run(run_name=config["run_id"]) as active_run:
            mlflow.log_param("run_id", config["run_id"])
            mlflow.log_param("artifact_path", str(run_dir))

            for key, value in params.items():
                mlflow.log_param(key, value)

            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    mlflow.log_metric(key, value)

            mlflow.log_artifact(str(run_dir / "config.json"))
            mlflow.log_artifact(str(run_dir / "metrics.json"))

            return {
                "succeeded": True,
                "tracking_uri": tracking_uri,
                "experiment_name": experiment_name,
                "mlflow_run_id": active_run.info.run_id,
                "artifact_path": str(run_dir),
            }

    except Exception as exc:
        return {
            "succeeded": False,
            "tracking_uri": tracking_uri,
            "experiment_name": experiment_name,
            "error": repr(exc),
            "artifact_path": str(run_dir),
        }


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "run_id": Param("", type="string"),
        "mode": Param("batch", enum=["batch"], description="Batch evaluation mode. Produces preds.json and runs SWE-bench evaluation."),
        "subset": Param("verified", type="string"),
        "split": Param("test", type="string"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "instance_id": Param("sympy__sympy-15599", type="string"),
        "task_slice": Param("0:1", type="string"),
        "workers": Param(1, type="integer"),
        "cost_limit": Param(0, type="integer"),
        "agent_timeout_seconds": Param(900, type="integer"),
        "mlflow_tracking_uri": Param("", type="string"),
        "mlflow_experiment_name": Param("coding-agent-evaluation", type="string"),
    },
)
def evaluate_agent_dag():
    @task
    def prepare_run() -> str:
        context = get_current_context()
        params = dict(context["params"])

        run_id = params.get("run_id") or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        run_dir = RUNS_DIR / run_id

        (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
        (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)

        config = {
            "run_id": run_id,
            "created_at_utc": datetime.utcnow().isoformat(),
            "project_root": str(PROJECT_ROOT),
            "params": params,
        }

        write_json(run_dir / "config.json", config)
        return str(run_dir)

    @task
    def run_agent(run_dir_str: str) -> str:
        run_dir = Path(run_dir_str)
        config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
        params = config["params"]

        agent_dir = run_dir / "run-agent"
        log_path = agent_dir / "agent.log"

        env = {
            **os.environ,
            "MSWEA_COST_TRACKING": "ignore_errors",
        }

        if params.get("mode") != "batch":
            raise ValueError(
                "Only batch mode is supported for evaluated runs. "
                "Batch mode produces trajectories/preds.json required by SWE-bench evaluation."
            )

        output_path = agent_dir / "trajectories"
        command = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", params["subset"],
            "--split", params["split"],
            "--model", params["model"],
            "--slice", params["task_slice"],
            "--config", "swebench.yaml",
            "--workers", str(params["workers"]),
            "-o", str(output_path),
        ]

        timeout_seconds = int(params.get("agent_timeout_seconds", 900))
        return_code = run_command(command, PROJECT_ROOT, env, log_path, timeout_seconds)
        cleaned_containers = cleanup_minisweagent_containers() if return_code == 124 else []

        write_json(
            agent_dir / "status.json",
            {
                "step": "run_agent",
                "return_code": return_code,
                "succeeded": return_code == 0,
                "mode": params["mode"],
                "command": command,
                "log_path": str(log_path),
                "output_path": str(output_path),
                "timeout_seconds": timeout_seconds,
                "timed_out": return_code == 124,
                "cleaned_containers": cleaned_containers,
            },
        )

        return str(run_dir)

    @task
    def run_eval(run_dir_str: str) -> str:
        run_dir = Path(run_dir_str)
        config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
        params = config["params"]

        eval_dir = run_dir / "run-eval"
        reports_dir = eval_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        log_path = eval_dir / "eval.log"
        preds_path = run_dir / "run-agent" / "trajectories" / "preds.json"

        if not preds_path.exists():
            write_json(
                eval_dir / "status.json",
                {
                    "step": "run_eval",
                    "succeeded": False,
                    "skipped": True,
                    "reason": "preds.json not found. Evaluation requires batch mode output.",
                    "expected_preds_path": str(preds_path),
                },
            )
            return str(run_dir)

        command = [
            "uv", "run", "python", "-m", "swebench.harness.run_evaluation",
            "--dataset_name", "princeton-nlp/SWE-bench_Verified",
            "--split", params["split"],
            "--predictions_path", str(preds_path),
            "--max_workers", str(params["workers"]),
            "--run_id", config["run_id"],
            "--report_dir", str(reports_dir),
        ]

        return_code = run_command(command, PROJECT_ROOT, os.environ.copy(), log_path, timeout_seconds=1800)

        moved_reports = []
        for report_path in sorted(PROJECT_ROOT.glob(f"*.{config['run_id']}.json")):
            target_path = reports_dir / report_path.name
            report_path.replace(target_path)
            moved_reports.append(str(target_path))

        write_json(
            eval_dir / "status.json",
            {
                "step": "run_eval",
                "return_code": return_code,
                "succeeded": return_code == 0,
                "skipped": False,
                "command": command,
                "log_path": str(log_path),
                "preds_path": str(preds_path),
                "reports": moved_reports,
            },
        )

        return str(run_dir)

    @task
    def summarize_and_log(run_dir_str: str) -> str:
        run_dir = Path(run_dir_str)

        agent_status = json.loads((run_dir / "run-agent" / "status.json").read_text(encoding="utf-8"))
        eval_status = json.loads((run_dir / "run-eval" / "status.json").read_text(encoding="utf-8"))

        artifacts = sorted(
            str(path.relative_to(run_dir))
            for path in run_dir.rglob("*")
            if path.is_file()
        )

        metrics = {
            "agent_succeeded": int(agent_status.get("succeeded", False)),
            "eval_succeeded": int(eval_status.get("succeeded", False)),
            "eval_skipped": int(eval_status.get("skipped", False)),
        }
        metrics.update(extract_swebench_metrics(run_dir))

        write_json(run_dir / "metrics.json", metrics)

        config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
        mlflow_status = log_mlflow_run(config, metrics, run_dir)

        metrics["mlflow_logged"] = int(mlflow_status.get("succeeded", False))
        write_json(run_dir / "mlflow.json", mlflow_status)

        artifacts = sorted(
            str(path.relative_to(run_dir))
            for path in run_dir.rglob("*")
            if path.is_file()
        )

        if "manifest.json" not in artifacts:
            artifacts.append("manifest.json")
            artifacts = sorted(artifacts)

        metrics["artifact_count"] = len(artifacts)
        write_json(run_dir / "metrics.json", metrics)

        manifest = {
            "run_dir": str(run_dir),
            "created_at_utc": datetime.utcnow().isoformat(),
            "artifacts": artifacts,
            "agent_status": agent_status,
            "eval_status": eval_status,
            "mlflow_status": mlflow_status,
        }

        write_json(run_dir / "manifest.json", manifest)

        return str(run_dir)

    run_dir = prepare_run()
    agent_done = run_agent(run_dir)
    eval_done = run_eval(agent_done)
    summarize_and_log(eval_done)


evaluate_agent_dag()
