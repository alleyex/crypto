import json
from typing import Any

from app.core.db import DBConnection, utc_now_iso
from app.training.job_service import update_job as update_training_job


def initialize_job_progress(
    connection: DBConnection,
    job_id: int,
    *,
    total: int,
) -> None:
    connection.execute(
        "UPDATE training_jobs SET progress_json=? WHERE id=?;",
        (json.dumps({"pct": 0, "step": 0, "total": int(total)}), job_id),
    )
    connection.commit()


def mark_job_running(
    connection: DBConnection,
    job_id: int,
    *,
    started_at: str | None = None,
    total_steps: int | None = None,
) -> str:
    resolved_started_at = started_at or utc_now_iso()
    update_training_job(connection, job_id, status="running", started_at=resolved_started_at)
    if total_steps is not None:
        initialize_job_progress(connection, job_id, total=total_steps)
    return resolved_started_at


def update_job_progress(
    connection: DBConnection,
    job_id: int,
    *,
    step: int,
    total: int,
    extra: dict[str, Any] | None = None,
) -> None:
    pct = round(step / total * 100, 1) if total > 0 else 0
    progress = {"pct": pct, "step": int(step), "total": int(total)}
    if extra:
        progress.update(extra)
    connection.execute(
        "UPDATE training_jobs SET progress_json=? WHERE id=?;",
        (json.dumps(progress, sort_keys=True), job_id),
    )
    connection.commit()


def mark_job_done(
    connection: DBConnection,
    job_id: int,
    *,
    dataset: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    model: dict[str, Any] | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    total_steps: int | None = None,
) -> str:
    resolved_finished_at = finished_at or utc_now_iso()
    update_training_job(
        connection,
        job_id,
        status="done",
        dataset=dataset,
        metrics=metrics,
        model=model,
        started_at=started_at,
        finished_at=resolved_finished_at,
    )
    if total_steps is not None:
        update_job_progress(connection, job_id, step=total_steps, total=total_steps)
    return resolved_finished_at


def mark_job_failed(
    connection: DBConnection,
    job_id: int,
    *,
    error: str,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> str:
    resolved_finished_at = finished_at or utc_now_iso()
    update_training_job(
        connection,
        job_id,
        status="failed",
        error=error,
        started_at=started_at,
        finished_at=resolved_finished_at,
    )
    return resolved_finished_at


def mark_job_cancelled(
    connection: DBConnection,
    job_id: int,
    *,
    error: str = "Cancelled by user.",
    started_at: str | None = None,
    finished_at: str | None = None,
) -> str:
    resolved_finished_at = finished_at or utc_now_iso()
    update_training_job(
        connection,
        job_id,
        status="cancelled",
        error=error,
        started_at=started_at,
        finished_at=resolved_finished_at,
    )
    return resolved_finished_at
