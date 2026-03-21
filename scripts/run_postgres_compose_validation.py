import argparse
import http.client
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.postgres_smoke import run_postgres_migration_smoke
from app.core.postgres_smoke import run_postgres_smoke
from scripts.write_postgres_validation_artifact import build_artifact_manifest
from scripts.write_postgres_validation_artifact import build_summary_markdown
from scripts.write_postgres_validation_artifact import get_validation_layer
from scripts.write_postgres_validation_artifact import get_validation_verdict
from scripts.write_postgres_validation_artifact import write_optional_output
from scripts.write_postgres_validation_artifact import write_validation_artifacts


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run clean Docker Compose validation for the PostgreSQL runtime."
    )
    parser.add_argument(
        "--mode",
        choices=("smoke", "compose-runtime", "compose-soak-readability"),
        default="compose-runtime",
        help="Validation mode. Default: compose-runtime",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=8012,
        help="Published API port for the isolated validation stack. Default: 8012",
    )
    parser.add_argument(
        "--project-name",
        default="crypto_pg_validation",
        help="Docker Compose project name for the isolated validation stack.",
    )
    parser.add_argument(
        "--database-url",
        default="postgresql://crypto:crypto@postgres:5432/crypto",
        help="PostgreSQL DSN to inject into the Compose runtime.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=90.0,
        help="Seconds to wait for the API to become reachable. Default: 90",
    )
    parser.add_argument(
        "--keep-up",
        action="store_true",
        help="Leave the validation stack running instead of tearing it down.",
    )
    parser.add_argument(
        "--json-output",
        default="",
        help="Optional path to write the full validation JSON payload.",
    )
    parser.add_argument(
        "--summary-file",
        default="",
        help="Optional path to write a markdown summary of the validation result.",
    )
    parser.add_argument(
        "--raw-log-output",
        default="",
        help="Optional path to write the validation script stdout payload.",
    )
    parser.add_argument(
        "--docker-logs-output",
        default="",
        help="Optional path to write collected Docker Compose logs for compose-based modes.",
    )
    parser.add_argument(
        "--docker-logs-dir",
        default="",
        help="Optional directory to write per-service Docker Compose logs for compose-based modes.",
    )
    parser.add_argument(
        "--manifest-output",
        default="",
        help="Optional path to write an artifact manifest describing generated files.",
    )
    return parser.parse_args()


def build_override_compose(api_port: int, work_dir: Path) -> str:
    storage_dir = work_dir / "storage"
    logs_dir = work_dir / "logs"
    runtime_dir = work_dir / "runtime"
    return f"""services:
  postgres:
    ports: []

  api:
    ports:
      - "{api_port}:8000"
    volumes:
      - {storage_dir}:/app/storage
      - {logs_dir}:/app/logs
      - {runtime_dir}:/app/runtime

  scheduler:
    volumes:
      - {storage_dir}:/app/storage
      - {logs_dir}:/app/logs
      - {runtime_dir}:/app/runtime
"""


def run_command(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )


def collect_compose_logs(compose_args: list[str], env: dict[str, str]) -> str:
    return run_command(compose_args + ["logs", "--tail=200", "postgres", "api", "scheduler"], env).stdout


def collect_service_logs(compose_args: list[str], env: dict[str, str], service: str) -> str:
    return run_command(compose_args + ["logs", "--tail=200", service], env).stdout


def request_json(method: str, url: str) -> Any:
    request = urllib.request.Request(url=url, method=method)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def request_json_with_retry(method: str, url: str, attempts: int = 5, delay_seconds: float = 1.0) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return request_json(method, url)
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in (500, 502, 503, 504) or attempt >= attempts:
                raise
        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
            ConnectionResetError,
            ConnectionAbortedError,
            http.client.RemoteDisconnected,
        ) as exc:
            last_error = exc
            if attempt >= attempts:
                raise
        time.sleep(delay_seconds)

    assert last_error is not None
    raise last_error


def assert_pipeline_validation_success(pipeline: Any) -> None:
    if not isinstance(pipeline, dict):
        raise RuntimeError(f"Pipeline validation returned an unexpected payload: {pipeline!r}")

    steps = pipeline.get("steps")
    if not isinstance(steps, list) or not steps:
        nested_result = pipeline.get("result")
        if isinstance(nested_result, dict):
            steps = nested_result.get("steps")
    if not isinstance(steps, list) or not steps:
        raise RuntimeError(f"Pipeline validation did not return executable steps: {pipeline!r}")

    failed_steps = [
        step for step in steps
        if isinstance(step, dict) and step.get("status") in ("failed", "blocked")
    ]
    if failed_steps:
        raise RuntimeError(f"Pipeline validation failed: {json.dumps(failed_steps, sort_keys=True)}")


def wait_for_api(base_url: str, timeout_seconds: float) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            payload = request_json("GET", f"{base_url}/health")
            if isinstance(payload, dict):
                return payload
        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
            ConnectionResetError,
            ConnectionAbortedError,
            http.client.RemoteDisconnected,
        ) as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"API did not become ready within {timeout_seconds} seconds: {last_error}")


def make_env(project_name: str, database_url: str) -> dict[str, str]:
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = project_name
    env["CRYPTO_DB_BACKEND"] = "postgres"
    env["CRYPTO_DATABASE_URL"] = database_url
    env["CRYPTO_USE_FAKE_KLINES"] = "1"
    env.setdefault("CRYPTO_FAKE_KLINE_CLOSES", "10,11,12,13,14")
    env.setdefault("CRYPTO_POSTGRES_CONNECT_RETRIES", "15")
    env.setdefault("CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS", "1")
    return env


def attach_metadata(result: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(result)
    mode = str(result.get("mode", ""))
    enriched["validation_layer"] = get_validation_layer(mode)
    enriched["verdict"] = get_validation_verdict(mode)
    enriched["event_name"] = os.getenv("GITHUB_EVENT_NAME", "local")
    enriched["run_id"] = os.getenv("GITHUB_RUN_ID", "local")
    enriched["generated_at"] = datetime.now(timezone.utc).isoformat()
    return enriched


def validate_compose_runtime(
    api_port: int,
    project_name: str,
    database_url: str,
    startup_timeout: float,
    keep_up: bool,
    include_soak: bool = False,
) -> dict[str, Any]:
    work_dir = Path(tempfile.mkdtemp(prefix="crypto-pg-validate-"))
    for name in ("storage", "logs", "runtime"):
        (work_dir / name).mkdir(parents=True, exist_ok=True)

    override_file = work_dir / "docker-compose.override.yml"
    override_file.write_text(build_override_compose(api_port, work_dir), encoding="utf-8")

    env = make_env(project_name, database_url)
    compose_args = [
        "docker",
        "compose",
        "--project-directory",
        str(PROJECT_ROOT),
        "-f",
        str(BASE_COMPOSE_FILE),
        "-f",
        str(override_file),
        "--profile",
        "postgres",
    ]

    base_url = f"http://127.0.0.1:{api_port}"

    try:
        run_command(compose_args + ["up", "--build", "-d"], env)
        health = wait_for_api(base_url, startup_timeout)
        pipeline = request_json_with_retry("POST", f"{base_url}/pipeline/run")
        assert_pipeline_validation_success(pipeline)
        orders = request_json_with_retry("GET", f"{base_url}/orders?limit=5")
        audit_events = request_json_with_retry("GET", f"{base_url}/audit-events?limit=5")
        scheduler_logs = run_command(compose_args + ["logs", "--tail=80", "scheduler"], env).stdout
        docker_logs = collect_compose_logs(compose_args, env)
        api_logs = collect_service_logs(compose_args, env, "api")
        scheduler_logs_full = collect_service_logs(compose_args, env, "scheduler")
        postgres_logs = collect_service_logs(compose_args, env, "postgres")
        result = {
            "mode": "compose-runtime",
            "ok": True,
            "base_url": base_url,
            "work_dir": str(work_dir),
            "health": health,
            "pipeline": pipeline,
            "orders": orders,
            "audit_events": audit_events,
            "scheduler_logs": scheduler_logs.strip().splitlines(),
            "docker_logs": docker_logs,
            "api_logs": api_logs,
            "scheduler_logs_full": scheduler_logs_full,
            "postgres_logs": postgres_logs,
        }
        if include_soak:
            result["mode"] = "compose-soak-readability"
            result["soak_validation"] = request_json("GET", f"{base_url}/validation/soak")
            result["soak_history"] = request_json("GET", f"{base_url}/validation/soak/history")
        return result
    finally:
        if not keep_up:
            try:
                run_command(compose_args + ["down", "-v"], env)
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)


def validate_compose_soak_readability(
    api_port: int,
    project_name: str,
    database_url: str,
    startup_timeout: float,
    keep_up: bool,
) -> dict[str, Any]:
    return validate_compose_runtime(
        api_port=api_port,
        project_name=project_name,
        database_url=database_url,
        startup_timeout=startup_timeout,
        keep_up=keep_up,
        include_soak=True,
    )


def validate_smoke(database_url: str) -> dict[str, Any]:
    return {
        "mode": "smoke",
        "ok": True,
        "connection_smoke": run_postgres_smoke(database_url),
        "migration_smoke": run_postgres_migration_smoke(database_url),
    }


def run_validation_mode(args: argparse.Namespace) -> dict[str, Any]:
    if args.mode == "smoke":
        return validate_smoke(args.database_url)
    if args.mode == "compose-soak-readability":
        return validate_compose_soak_readability(
            api_port=args.api_port,
            project_name=args.project_name,
            database_url=args.database_url,
            startup_timeout=args.startup_timeout,
            keep_up=args.keep_up,
        )
    return validate_compose_runtime(
        api_port=args.api_port,
        project_name=args.project_name,
        database_url=args.database_url,
        startup_timeout=args.startup_timeout,
        keep_up=args.keep_up,
    )


def main() -> None:
    args = parse_args()
    result = attach_metadata(run_validation_mode(args))
    result_json = write_validation_artifacts(
        result=result,
        json_output=args.json_output,
        summary_file=args.summary_file,
        raw_log_output=args.raw_log_output,
        docker_logs_output=args.docker_logs_output,
        docker_logs_dir=args.docker_logs_dir,
        manifest_output=args.manifest_output,
        write_step_summary=True,
    )
    print(result_json)


if __name__ == "__main__":
    main()
