from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.core.db import DBConnection

from app.features.compute import FEATURE_SET_VERSION
from app.registry.registry_service import (
    archive_model,
    get_champion,
    get_model as get_registry_model,
    list_models as list_registry_models,
    list_versions as list_registry_versions,
    promote_model,
    register_model,
    update_notes as update_registry_notes,
)
from app.training.job_service import get_job as get_training_job
from app.api.errors import http_not_found, http_unprocessable
from app.api.deps import get_db

router = APIRouter()

class RegisterModelRequest(BaseModel):
    symbol: str
    timeframe: str = "1m"
    feature_set: str = FEATURE_SET_VERSION
    training_job_id: int | None = None
    notes: str | None = None

class UpdateModelNotesRequest(BaseModel):
    notes: str

@router.post("/registry/models")
def register_model_endpoint(
    body: RegisterModelRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Register a model version from a completed training job.

    Copies the model weights and metrics from the training job into the
    registry as a new ``candidate``.  The ``training_job_id`` must point to
    a job with ``status = 'done'``.
    """
    if body.training_job_id is None:
        http_unprocessable("training_job_id is required.")
    job = get_training_job(connection, body.training_job_id)
    if job is None:
        http_not_found(f"Training job {body.training_job_id} not found.")
    if job["status"] != "done":
        http_unprocessable(
            f"Training job {body.training_job_id} has status={job['status']!r}. "
            "Only 'done' jobs can be registered."
        )
    model_dict = job.get("model")
    if not model_dict:
        http_unprocessable(f"Training job {body.training_job_id} has no model artifact.")
    model_id = register_model(
        connection,
        symbol=body.symbol,
        timeframe=body.timeframe,
        feature_set=body.feature_set,
        model=model_dict,
        training_job_id=body.training_job_id,
        metrics=job.get("metrics"),
        notes=body.notes,
    )
    return get_registry_model(connection, model_id) or {}

@router.get("/registry/models")
def list_registry_models_endpoint(
    symbol: str | None = None,
    timeframe: str | None = None,
    feature_set: str | None = None,
    status: str | None = None,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return paginated model registry entries, newest first."""
    return list_registry_models(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        status=status,
        limit=limit,
        offset=offset,
    )

@router.get("/registry/models/{model_id}")
def get_registry_model_endpoint(
    model_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return a single model registry entry by id."""
    model = get_registry_model(connection, model_id)
    if model is None:
        http_not_found(f"Model {model_id} not found.")
    return model

@router.get("/registry/versions/{symbol}")
def list_registry_versions_endpoint(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    connection: DBConnection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return all versions for a symbol/timeframe/feature_set, newest first."""
    return list_registry_versions(connection, symbol=symbol, timeframe=timeframe, feature_set=feature_set)

@router.post("/registry/models/{model_id}/promote")
def promote_model_endpoint(
    model_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Promote a model to champion.

    Archives the current champion for the same (symbol, timeframe, feature_set).
    Can be used for rollback by promoting any previous candidate or archived model.
    """
    model = get_registry_model(connection, model_id)
    if model is None:
        http_not_found(f"Model {model_id} not found.")
    result = promote_model(connection, model_id)
    return result or {}

@router.post("/registry/models/{model_id}/archive")
def archive_model_endpoint(
    model_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Archive a model, removing it from serving."""
    model = get_registry_model(connection, model_id)
    if model is None:
        http_not_found(f"Model {model_id} not found.")
    result = archive_model(connection, model_id)
    return result or {}

@router.patch("/registry/models/{model_id}")
def update_registry_model_notes(
    model_id: int,
    body: UpdateModelNotesRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Update the notes field on a registry entry."""
    model = get_registry_model(connection, model_id)
    if model is None:
        http_not_found(f"Model {model_id} not found.")
    result = update_registry_notes(connection, model_id, body.notes)
    return result or {}

@router.get("/registry/champion/{symbol}")
def get_champion_model_endpoint(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return the current champion model for a symbol/timeframe/feature_set.

    Returns 404 if no champion has been promoted yet.
    """
    model = get_champion(connection, symbol=symbol, timeframe=timeframe, feature_set=feature_set)
    if model is None:
        http_not_found(f"No champion model for symbol={symbol!r} timeframe={timeframe!r}.")
    return model
