"""RL (REINFORCE) training API routes."""

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.api.deps import get_db
from app.core.db import DBConnection, utc_now_iso
from app.features.compute import FEATURE_SET_VERSION
from app.features.store import get_features as get_feature_vectors
from app.registry.registry_service import (
    get_champion,
    promote_model,
    register_model,
)
from app.rl.experiment import run_rl_experiment
from app.training.job_service import (
    create_job as create_training_job,
    get_job as get_training_job,
    update_job as update_training_job,
)

router = APIRouter()


class RLJobRequest(BaseModel):
    symbol: str
    timeframe: str = "1m"
    feature_set: str = FEATURE_SET_VERSION
    n_episodes: int = Field(default=200, ge=1, le=5000)
    learning_rate: float = Field(default=1e-3, gt=0.0)
    gamma: float = Field(default=1.0, ge=0.0, le=1.0)
    fee_rate: float = Field(default=0.001, ge=0.0, le=0.05)
    test_ratio: float = Field(default=0.2, ge=0.05, le=0.5)
    seed: int = Field(default=42)
    use_champion: bool = True
    auto_promote: bool = False


@router.post("/training/rl-jobs")
def run_rl_job(
    body: RLJobRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Train a REINFORCE agent and benchmark it against buy-and-hold
    (and optionally the supervised champion model).

    Requires materialised feature vectors (POST /features/materialize first).
    Persists results as a training_jobs row with job_type='rl'.
    Returns the complete job record including comparison verdict.
    """

    hyperparams: dict[str, Any] = {
        "job_type": "rl",
        "n_episodes": body.n_episodes,
        "learning_rate": body.learning_rate,
        "gamma": body.gamma,
        "fee_rate": body.fee_rate,
        "test_ratio": body.test_ratio,
        "seed": body.seed,
        "use_champion": body.use_champion,
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

    registry_model_id = None
    registry_status = None
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
                f"Insufficient feature vectors ({len(vectors)}). "
                "Run POST /features/materialize first."
            )

        sup_weights: list | None = None
        sup_bias: float | None = None
        if body.use_champion:
            champion = get_champion(
                connection,
                symbol=body.symbol,
                timeframe=body.timeframe,
                feature_set=body.feature_set,
            )
            if champion and champion.get("model"):
                sup_weights = champion["model"].get("weights")
                sup_bias = champion["model"].get("bias")

        result = run_rl_experiment(
            vectors=vectors,
            n_episodes=body.n_episodes,
            learning_rate=body.learning_rate,
            gamma=body.gamma,
            fee_rate=body.fee_rate,
            test_ratio=body.test_ratio,
            seed=body.seed,
            supervised_weights=sup_weights,
            supervised_bias=sup_bias,
        )

        dataset_stats = result["dataset"]
        metrics_dict: dict[str, Any] = {
            "test_rl": result["test_rl"],
            "test_bnh": result["test_bnh"],
            "test_supervised": result["test_supervised"],
            "verdict": result["verdict"],
            "final_train_loss": result["train"]["final_loss"],
        }
        model_dict: dict[str, Any] = {
            **result["agent"],
            "symbol": body.symbol,
            "timeframe": body.timeframe,
            "feature_set": body.feature_set,
            "n_episodes": body.n_episodes,
            "fee_rate": body.fee_rate,
            "train_loss_history": result["train"]["loss_history"],
        }

        finished_at = utc_now_iso()
        update_training_job(
            connection,
            job_id,
            status="done",
            dataset=dataset_stats,
            metrics=metrics_dict,
            model=model_dict,
            started_at=started_at,
            finished_at=finished_at,
        )

        registry_model_id = register_model(
            connection,
            symbol=body.symbol,
            timeframe=body.timeframe,
            feature_set=body.feature_set,
            model=model_dict,
            training_job_id=job_id,
            metrics=metrics_dict,
            notes=f"REINFORCE agent, verdict={result['verdict']}, episodes={body.n_episodes}",
        )
        registry_status = "candidate"
        if body.auto_promote:
            promote_model(connection, registry_model_id)
            registry_status = "champion"

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

    job = get_training_job(connection, job_id) or {}
    job["registry_model_id"] = registry_model_id
    job["registry_status"] = registry_status
    return job
