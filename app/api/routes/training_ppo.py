"""PPO training API routes (async queue-based)."""

import os
from pathlib import Path as _Path
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from app.api.errors import http_not_found, http_unprocessable
from app.core.db import DBConnection
from app.core.job_queue import enqueue_job

from app.training.job_service import (
    create_job as create_training_job,
    get_job as get_training_job,
)
from app.api.deps import get_db

router = APIRouter()

class PPOJobRequest(BaseModel):
    symbol: str = "BTCUSDT"
    timeframe: str = "1m"
    total_steps: int = Field(default=1_000_000, ge=10_000, le=10_000_000)
    eval_windows: int = Field(default=8, ge=1, le=20)
    fee_rate: float = Field(default=0.001, ge=0.0, le=0.05)
    learning_rate: float = Field(default=3e-4, gt=0.0)
    n_steps: int = Field(default=2048, ge=64)
    batch_size: int = Field(default=256, ge=16)
    n_epochs: int = Field(default=10, ge=1, le=50)
    gamma: float = Field(default=0.99, ge=0.0, le=1.0)
    gae_lambda: float = Field(default=0.95, ge=0.0, le=1.0)
    clip_range: float = Field(default=0.2, ge=0.0, le=1.0)
    ent_coef: float = Field(default=0.01, ge=0.0, le=1.0)
    seed: int = Field(default=42)
    frame_stack: int = Field(default=1, ge=1, le=20)
    holding_bonus: float = Field(default=0.0, ge=0.0, le=0.01)
    decision_interval: int | None = Field(default=None, ge=1, le=60)
    reward_horizon: int | None = Field(default=None, ge=1, le=60)
    action_interval: int | None = Field(default=None, ge=1, le=60)
    train_frac: float = Field(default=0.70, ge=0.50, le=0.99)

@router.post("/training/ppo-jobs")
def start_ppo_job(
    body: PPOJobRequest,
    request: Request,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Queue a PPO training job for the dedicated training worker."""
    import json as _json

    from app.training.ppo_trainer import resolve_episode_lengths

    train_ep_len, eval_ep_len = resolve_episode_lengths(body.timeframe)
    decision_interval = int(body.action_interval or body.decision_interval or 1)
    reward_horizon = int(body.reward_horizon or body.action_interval or 1)

    params = {
        "job_type":          "ppo",
        "total_steps":       body.total_steps,
        "eval_windows":      body.eval_windows,
        "fee_rate":          body.fee_rate,
        "learning_rate":     body.learning_rate,
        "n_steps":           body.n_steps,
        "batch_size":        body.batch_size,
        "n_epochs":          body.n_epochs,
        "gamma":             body.gamma,
        "gae_lambda":        body.gae_lambda,
        "clip_range":        body.clip_range,
        "ent_coef":          body.ent_coef,
        "train_ep_len":      train_ep_len,
        "eval_ep_len":       eval_ep_len,
        "seed":              body.seed,
        "frame_stack":       body.frame_stack,
        "holding_bonus":     body.holding_bonus,
        "decision_interval": decision_interval,
        "reward_horizon":    reward_horizon,
        "action_interval":   decision_interval,
        "train_frac":        body.train_frac,
    }
    job_id = create_training_job(
        connection,
        symbol=body.symbol,
        timeframe=body.timeframe,
        feature_set="ppo",
        params=params,
    )

    connection.execute(
        "UPDATE training_jobs SET progress_json=? WHERE id=?;",
        (_json.dumps({"pct": 0, "step": 0, "total": body.total_steps}), job_id),
    )
    enqueue_job(
        connection,
        "training_ppo",
        payload={"training_job_id": job_id},
    )
    connection.commit()
    job = get_training_job(connection, job_id)

    result = job or {"id": job_id, "status": "pending"}
    tb_host = request.headers.get("host", "localhost").split(":")[0]
    tb_port = int(os.environ.get("CRYPTO_TENSORBOARD_PORT", "6006"))
    result["tensorboard_url"] = f"http://{tb_host}:{tb_port}"
    return result

@router.post("/training/ppo-jobs/{job_id}/deploy")
def deploy_ppo_job(
    job_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Deploy a completed PPO candidate model as the active model.

    Copies runtime/models/ppo_{symbol}_{tf}_candidate_{job_id}.zip
    to runtime/models/ppo_{symbol}_{tf}.zip and clears the in-memory cache.
    """
    from app.training.ppo_trainer import deploy_candidate_model

    job = get_training_job(connection, job_id)
    if job is None:
        http_not_found(f"PPO job {job_id} not found.")
    if job.get("status") != "done":
        http_unprocessable(f"Job {job_id} status={job.get('status')!r}. Only 'done' jobs can be deployed.")

    symbol    = job["symbol"]
    timeframe = job["timeframe"]
    try:
        active_path = deploy_candidate_model(symbol, timeframe, job_id)
    except FileNotFoundError as exc:
        http_not_found(str(exc))

    return {
        "job_id":      job_id,
        "symbol":      symbol,
        "timeframe":   timeframe,
        "active_path": active_path,
        "deployed":    True,
    }
