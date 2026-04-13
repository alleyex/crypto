import json
from pathlib import Path as _Path
from typing import Any

from fastapi import APIRouter, Depends, Query
from app.api.errors import http_not_found, http_unprocessable
from pydantic import BaseModel, Field

from app.core.db import DBConnection, utc_now_iso

from app.features.compute import FEATURE_SET_VERSION
from app.features.store import get_features as get_feature_vectors
from app.training.dataset import (
    FEATURE_NAMES,
    build_dataset,
    dataset_summary,
    train_test_split as training_split,
)
from app.training.job_service import (
    create_job as create_training_job,
    get_job as get_training_job,
    list_jobs as list_training_jobs,
    update_job as update_training_job,
)
from app.training.trainer import (
    evaluate as evaluate_model,
    model_to_dict,
    predict,
    train as train_model,
)
from app.api.deps import get_db

router = APIRouter()

class TrainingJobRequest(BaseModel):
    symbol: str
    timeframe: str = "1m"
    feature_set: str = FEATURE_SET_VERSION
    test_ratio: float = Field(default=0.2, ge=0.05, le=0.5)
    n_epochs: int = Field(default=100, ge=1, le=2000)
    learning_rate: float = Field(default=0.01, gt=0.0)
    batch_size: int = Field(default=32, ge=1)
    l2_lambda: float = Field(default=1e-4, ge=0.0)
    seed: int = Field(default=42)

@router.post("/training/jobs")
def run_training_job(
    body: TrainingJobRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Train a logistic regression model on stored feature vectors.

    Loads all materialised feature vectors for symbol/timeframe/feature_set,
    builds a supervised dataset, splits it chronologically, trains, evaluates,
    and persists the result as a training_jobs row.

    Returns the complete training job record.
    """

    hyperparams: dict[str, Any] = {
        "test_ratio": body.test_ratio,
        "n_epochs": body.n_epochs,
        "learning_rate": body.learning_rate,
        "batch_size": body.batch_size,
        "l2_lambda": body.l2_lambda,
        "seed": body.seed,
    }
    job_id = create_training_job(
        connection,
        symbol=body.symbol,
        timeframe=body.timeframe,
        feature_set=body.feature_set,
        params=hyperparams,
    )

    started_at = utc_now_iso()
    update_training_job(connection, job_id, status="running", started_at=started_at)

    try:
        fv_result = get_feature_vectors(
            connection,
            symbol=body.symbol,
            timeframe=body.timeframe,
            feature_set=body.feature_set,
            limit=100_000,
        )
        vectors = fv_result.get("vectors", [])
        if len(vectors) < 10:
            raise ValueError(
                f"Insufficient feature vectors ({len(vectors)}) for training. "
                "Run POST /features/materialize first."
            )

        X_all, y_all, times_all = build_dataset(vectors)
        if len(X_all) < 10:
            raise ValueError(
                f"Dataset has only {len(X_all)} labelled rows after building. "
                "Need at least 10."
            )

        X_train, y_train, _, X_test, y_test, _ = training_split(
            X_all, y_all, times_all, test_ratio=body.test_ratio
        )

        dataset_stats: dict[str, Any] = {
            "n_total": len(X_all),
            "n_train": len(X_train),
            "n_test": len(X_test),
            "feature_names": FEATURE_NAMES,
            "train_balance": dataset_summary(y_train),
            "test_balance": dataset_summary(y_test),
        }

        train_result = train_model(
            X_train, y_train,
            n_features=len(FEATURE_NAMES),
            learning_rate=body.learning_rate,
            n_epochs=body.n_epochs,
            batch_size=body.batch_size,
            l2_lambda=body.l2_lambda,
            seed=body.seed,
        )

        train_preds = predict(train_result["weights"], train_result["bias"], X_train)
        test_preds = predict(train_result["weights"], train_result["bias"], X_test)

        metrics: dict[str, Any] = {
            "train": evaluate_model(y_train, train_preds),
            "test": evaluate_model(y_test, test_preds),
            "final_train_loss": train_result["final_train_loss"],
        }

        model_dict = model_to_dict(
            train_result,
            feature_names=FEATURE_NAMES,
            symbol=body.symbol,
            timeframe=body.timeframe,
            feature_set=body.feature_set,
        )

        finished_at = utc_now_iso()
        update_training_job(
            connection,
            job_id,
            status="done",
            dataset=dataset_stats,
            metrics=metrics,
            model=model_dict,
            started_at=started_at,
            finished_at=finished_at,
        )

    except Exception as exc:
        finished_at = utc_now_iso()
        update_training_job(
            connection,
            job_id,
            status="failed",
            error=str(exc),
            started_at=started_at,
            finished_at=finished_at,
        )

    job = get_training_job(connection, job_id)
    return job or {}

@router.get("/training/jobs")
def list_training_jobs_endpoint(
    symbol: str | None = None,
    status: str | None = None,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return paginated training jobs, newest first."""
    return list_training_jobs(connection, symbol=symbol, status=status, limit=limit, offset=offset)

@router.get("/training/jobs/{job_id}")
def get_training_job_endpoint(
    job_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return a single training job by id."""
    job = get_training_job(connection, job_id)
    if job is None:
        http_not_found(f"Training job {job_id} not found.")
    return job

@router.post("/training/jobs/{job_id}/cancel")
def cancel_training_job(
    job_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    job = get_training_job(connection, job_id)
    if job is None:
        http_not_found(f"Training job {job_id} not found.")

    status = str(job.get("status") or "").lower()
    if status not in {"pending", "running"}:
        http_unprocessable(f"Job {job_id} status={job.get('status')!r}. Only pending/running jobs can be cancelled.")

    now = utc_now_iso()
    progress = dict(job.get("progress_json") or {})
    progress["cancel_requested"] = True
    progress.setdefault("pct", 0)
    progress.setdefault("step", 0)
    progress.setdefault("total", int((job.get("params") or {}).get("total_steps") or 0))

    if status == "pending":
        connection.execute(
            """
            UPDATE training_jobs
            SET status = 'cancelled',
                error = ?,
                finished_at = ?,
                progress_json = ?
            WHERE id = ?;
            """,
            ("Cancelled by user.", now, json.dumps(progress, sort_keys=True), job_id),
        )
        connection.execute(
            """
            UPDATE job_queue
            SET status = 'completed',
                result_json = ?,
                error_message = NULL,
                completed_at = ?
            WHERE job_type = 'training_ppo'
              AND status = 'queued'
              AND payload_json LIKE ?;
            """,
            (
                json.dumps({"status": "cancelled", "training_job_id": job_id}, sort_keys=True),
                now,
                f'%"training_job_id": {job_id}%',
            ),
        )
    else:
        connection.execute(
            """
            UPDATE training_jobs
            SET progress_json = ?,
                error = ?
            WHERE id = ?;
            """,
            (json.dumps(progress, sort_keys=True), "Cancellation requested by user.", job_id),
        )

    connection.commit()
    updated_job = get_training_job(connection, job_id)

    return {
        "cancelled": True,
        "job_id": job_id,
        "job": updated_job,
    }

@router.delete("/training/jobs/{job_id}")
def delete_training_job(
    job_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Delete a training job record and its TensorBoard log directory."""
    import shutil

    job = get_training_job(connection, job_id)
    if job is None:
        http_not_found(f"Training job {job_id} not found.")

    symbol    = job.get("symbol", "")
    timeframe = job.get("timeframe", "")
    queue_delete_cursor = connection.execute(
        """
        DELETE FROM job_queue
        WHERE job_type = 'training_ppo'
          AND payload_json LIKE ?;
        """,
        (f'%"training_job_id": {job_id}%',),
    )
    deleted_queue_rows = max(int(getattr(queue_delete_cursor, "rowcount", 0) or 0), 0)

    connection.execute("DELETE FROM training_jobs WHERE id = ?;", (job_id,))
    connection.commit()

    tb_logs_dir = _Path(__file__).resolve().parent.parent.parent.parent / "runtime" / "tb_logs"
    deleted_dirs: list[str] = []
    if symbol and timeframe and tb_logs_dir.is_dir():
        prefix = f"ppo_{symbol}_{timeframe}_job{job_id}_"
        for entry in tb_logs_dir.iterdir():
            if entry.is_dir() and entry.name.startswith(prefix):
                shutil.rmtree(entry, ignore_errors=True)
                deleted_dirs.append(entry.name)

    return {
        "deleted": True,
        "job_id": job_id,
        "queue_rows_removed": deleted_queue_rows,
        "tb_dirs_removed": deleted_dirs,
    }
