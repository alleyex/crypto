import json
from typing import Any

from app.core.db import get_connection
from app.core.migrations import run_migrations
from app.training.job_service import get_job as get_training_job
from app.training.lifecycle import (
    mark_job_cancelled,
    mark_job_done,
    mark_job_failed,
    mark_job_running,
    update_job_progress,
)
from app.training.ppo_trainer import run_ppo_training

class CancelledTrainingJob(Exception):
    def __init__(self, job_id: int):
        super().__init__(f"PPO training job {job_id} was cancelled.")
        self.job_id = job_id

    def to_payload(self) -> dict[str, Any]:
        return {"training_job_id": self.job_id, "status": "cancelled"}

class MissingTrainingJob(Exception):
    def __init__(self, job_id: int):
        super().__init__(f"PPO training job {job_id} no longer exists.")
        self.job_id = job_id

    def to_payload(self) -> dict[str, Any]:
        return {"training_job_id": self.job_id, "status": "deleted"}

def _cancel_requested(connection, job_id: int) -> bool:
    job = get_training_job(connection, job_id)
    if job is None:
        raise MissingTrainingJob(job_id)
    if str(job.get("status") or "").lower() == "cancelled":
        return True
    progress = job.get("progress_json") or {}
    return bool(progress.get("cancel_requested"))

def run_ppo_training_job(job_id: int, *, queue_job_id: int | None = None) -> dict[str, Any]:
    started_at: str | None = None
    connection = get_connection()
    try:
        run_migrations(connection)
        job = get_training_job(connection, job_id)
        if job is None:
            raise MissingTrainingJob(job_id)
        if _cancel_requested(connection, job_id):
            raise CancelledTrainingJob(job_id)
        params = dict(job.get("params") or {})
        if params.get("job_type") != "ppo":
            raise ValueError(f"Training job {job_id} is not a PPO job.")
        started_at = mark_job_running(
            connection,
            job_id,
            total_steps=int(params.get("total_steps") or 0),
        )
        if queue_job_id is not None:
            from app.core.job_queue import touch_job_lease

            touch_job_lease(connection, queue_job_id)
    finally:
        connection.close()

    progress_state = {
        "step": 0,
        "total": int(params.get("total_steps") or 0),
        "stage": "initializing",
    }

    def _sync_progress(*, step: int, total: int, stage: str | None = None) -> None:
        conn = get_connection()
        try:
            if _cancel_requested(conn, job_id):
                raise CancelledTrainingJob(job_id)
            extra = {"stage": stage} if stage else None
            update_job_progress(conn, job_id, step=step, total=total, extra=extra)
            if queue_job_id is not None:
                from app.core.job_queue import touch_job_lease

                touch_job_lease(conn, queue_job_id)
        finally:
            conn.close()

    def _on_status(stage: str) -> None:
        progress_state["stage"] = stage
        _sync_progress(
            step=int(progress_state["step"]),
            total=int(progress_state["total"]),
            stage=stage,
        )

    def _on_progress(current: int, total: int) -> None:
        progress_state["step"] = int(current)
        progress_state["total"] = int(total)
        _sync_progress(step=int(current), total=int(total), stage=str(progress_state["stage"]))

    _on_status("initializing")

    try:
        result = run_ppo_training(
            symbol=str(job["symbol"]),
            timeframe=str(job["timeframe"]),
            total_steps=int(params.get("total_steps") or 1_000_000),
            eval_windows=int(params.get("eval_windows") or 8),
            fee_rate=float(params.get("fee_rate") or 0.001),
            learning_rate=float(params.get("learning_rate") or 3e-4),
            n_steps=int(params.get("n_steps") or 2048),
            batch_size=int(params.get("batch_size") or 256),
            n_epochs=int(params.get("n_epochs") or 10),
            gamma=float(params.get("gamma") or 0.99),
            gae_lambda=float(params.get("gae_lambda") or 0.95),
            clip_range=float(params.get("clip_range") or 0.2),
            ent_coef=float(params.get("ent_coef") or 0.01),
            seed=int(params.get("seed") or 42),
            frame_stack=int(params.get("frame_stack") or 1),
            holding_bonus=float(params.get("holding_bonus") or 0.0),
            decision_interval=int(params.get("decision_interval") or params.get("action_interval") or 1),
            reward_horizon=int(params.get("reward_horizon") or params.get("action_interval") or 1),
            train_frac=float(params.get("train_frac") or 0.70),
            job_id=job_id,
            on_progress=_on_progress,
            on_status=_on_status,
        )
        metrics = {
            "verdict": result["verdict"],
            "win_rate": result["win_rate"],
            "avg_ppo_pct": result["avg_ppo_pct"],
            "avg_bnh_pct": result["avg_bnh_pct"],
            "avg_edge": result["avg_edge"],
            "walk_forward": result["walk_forward"],
        }
        dataset = {
            "n_total": result["n_total"],
            "n_train": result["n_train"],
            "fee_rate": result["fee_rate"],
        }
        conn = get_connection()
        try:
            if _cancel_requested(conn, job_id):
                mark_job_cancelled(conn, job_id, started_at=started_at)
                raise CancelledTrainingJob(job_id)
            mark_job_done(
                conn,
                job_id,
                dataset=dataset,
                metrics=metrics,
                model={"model_path": result["model_path"], "job_type": "ppo"},
                started_at=started_at,
                total_steps=int(params.get("total_steps") or 0),
            )
        finally:
            conn.close()
        return {
            "status": "ok",
            "training_job_id": job_id,
            "model_path": result["model_path"],
            "steps": [
                {
                    "step": "training_ppo",
                    "status": "done",
                    "training_job_id": job_id,
                    "symbol": job["symbol"],
                    "timeframe": job["timeframe"],
                    "verdict": result["verdict"],
                }
            ],
        }
    except CancelledTrainingJob:
        conn = get_connection()
        try:
            mark_job_cancelled(conn, job_id, started_at=started_at)
        finally:
            conn.close()
        raise
    except MissingTrainingJob:
        raise
    except Exception as exc:
        conn = get_connection()
        try:
            mark_job_failed(conn, job_id, error=str(exc), started_at=started_at)
        finally:
            conn.close()
        raise
