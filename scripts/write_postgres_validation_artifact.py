import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.artifact_utils import build_manifest_files
from scripts.artifact_utils import write_json_file
from scripts.artifact_utils import write_optional_output


def _latest_soak_history_timestamp(soak_history: Any) -> str:
    if not isinstance(soak_history, list):
        return "n/a"
    for item in soak_history:
        if not isinstance(item, dict):
            continue
        for key in ("recorded_at", "checked_at", "created_at"):
            value = item.get(key)
            if value:
                return str(value)
    return "n/a"


def get_validation_layer(mode: str) -> str:
    if mode == "smoke":
        return "smoke"
    if mode == "compose-runtime":
        return "runtime"
    if mode == "compose-soak-readability":
        return "readability"
    return "unknown"


def get_validation_verdict(mode: str) -> str:
    if mode == "smoke":
        return "quick-check"
    if mode == "compose-runtime":
        return "runtime-check"
    if mode == "compose-soak-readability":
        return "readability-check"
    return "unknown-check"


def build_summary_markdown(result: dict[str, Any]) -> str:
    health = result.get("health", {})
    pipeline = result.get("pipeline", {})
    pipeline_steps = pipeline.get("steps", [])
    last_step = pipeline_steps[-1]["step"] if pipeline_steps else "n/a"
    orders = result.get("orders", [])
    audit_events = result.get("audit_events", [])
    scheduler_logs = result.get("scheduler_logs", [])
    scheduler_tail = scheduler_logs[-1] if scheduler_logs else "n/a"
    lines = [
        "# PostgreSQL Compose Validation",
        "",
        f"- mode: `{result.get('mode', 'n/a')}`",
        f"- validation_layer: `{get_validation_layer(str(result.get('mode', '')))}`",
        f"- verdict: `{get_validation_verdict(str(result.get('mode', '')))}`",
        f"- ok: `{result.get('ok')}`",
        f"- event_name: `{result.get('event_name', 'n/a')}`",
        f"- run_id: `{result.get('run_id', 'n/a')}`",
        f"- generated_at: `{result.get('generated_at', 'n/a')}`",
        f"- base_url: `{result.get('base_url', 'n/a')}`",
        f"- database: `{health.get('database', 'n/a')}`",
        f"- health_status: `{health.get('status', 'n/a')}`",
        f"- pipeline_step_count: `{len(pipeline_steps)}`",
        f"- pipeline_last_step: `{last_step}`",
        f"- order_count: `{len(orders)}`",
        f"- audit_event_count: `{len(audit_events)}`",
        f"- scheduler_last_log: `{scheduler_tail}`",
    ]
    soak = result.get("soak_validation")
    if isinstance(soak, dict):
        lines.append(f"- soak_status: `{soak.get('status', 'n/a')}`")
    soak_history = result.get("soak_history")
    if isinstance(soak_history, list):
        lines.append(f"- soak_history_count: `{len(soak_history)}`")
        lines.append(f"- soak_history_latest_at: `{_latest_soak_history_timestamp(soak_history)}`")
    return "\n".join(lines) + "\n"

def build_artifact_manifest(
    result: dict[str, Any],
    artifact_root: Path,
    file_purposes: dict[str, str],
) -> dict[str, Any]:
    return {
        "mode": result.get("mode", "n/a"),
        "artifact_kind": "postgres-validation-artifact",
        "validation_layer": get_validation_layer(str(result.get("mode", ""))),
        "verdict": get_validation_verdict(str(result.get("mode", ""))),
        "event_name": result.get("event_name", "n/a"),
        "run_id": result.get("run_id", "n/a"),
        "generated_at": result.get("generated_at", "n/a"),
        "files": build_manifest_files(
            artifact_root=artifact_root,
            file_purposes=file_purposes,
        ),
    }


def write_validation_artifacts(
    *,
    result: dict[str, Any],
    json_output: str,
    summary_file: str,
    raw_log_output: str,
    docker_logs_output: str,
    docker_logs_dir: str,
    manifest_output: str,
    write_step_summary: bool,
) -> str:
    result_json = json.dumps(result, indent=2, sort_keys=True)
    raw_log_content = result_json + "\n"
    summary_markdown = build_summary_markdown(result)

    write_optional_output(json_output, result_json + "\n")
    write_optional_output(summary_file, summary_markdown)
    write_optional_output(raw_log_output, raw_log_content)
    write_optional_output(docker_logs_output, str(result.get("docker_logs", "")))

    has_service_logs = False
    if docker_logs_dir and result.get("mode") != "smoke":
        logs_dir = Path(docker_logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)
        for service_key, filename in (
            ("api_logs", "api.log"),
            ("scheduler_logs_full", "scheduler.log"),
            ("postgres_logs", "postgres.log"),
        ):
            content = result.get(service_key)
            if isinstance(content, str):
                (logs_dir / filename).write_text(content, encoding="utf-8")
                has_service_logs = True

    if manifest_output:
        manifest_path = Path(manifest_output)
        artifact_root = manifest_path.parent
        file_purposes = {
            "summary.md": "Human-readable validation summary.",
            "result.json": "Full structured validation result.",
            "raw.log": "Raw stdout payload from the validation script.",
            "runner.log": "Runner-level combined stdout and stderr for the validation command.",
        }
        if docker_logs_output and result.get("mode") != "smoke":
            file_purposes["docker.log"] = "Combined Docker Compose logs for postgres, api, and scheduler."
        if has_service_logs:
            file_purposes["services/api.log"] = "Docker Compose logs for the api service."
            file_purposes["services/scheduler.log"] = "Docker Compose logs for the scheduler service."
            file_purposes["services/postgres.log"] = "Docker Compose logs for the postgres service."
        manifest = build_artifact_manifest(
            result=result,
            artifact_root=artifact_root,
            file_purposes=file_purposes,
        )
        write_json_file(manifest_path, manifest)

    if write_step_summary:
        github_step_summary = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
        write_optional_output(github_step_summary, summary_markdown)

    return result_json
