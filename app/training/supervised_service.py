from typing import Any

from app.core.db import DBConnection
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
)
from app.training.lifecycle import mark_job_done, mark_job_failed, mark_job_running
from app.training.trainer import (
    evaluate as evaluate_model,
    model_to_dict,
    predict,
    train as train_model,
)


def run_supervised_training_job(
    connection: DBConnection,
    *,
    symbol: str,
    timeframe: str,
    feature_set: str,
    test_ratio: float,
    n_epochs: int,
    learning_rate: float,
    batch_size: int,
    l2_lambda: float,
    seed: int,
) -> dict[str, Any]:
    hyperparams: dict[str, Any] = {
        "test_ratio": test_ratio,
        "n_epochs": n_epochs,
        "learning_rate": learning_rate,
        "batch_size": batch_size,
        "l2_lambda": l2_lambda,
        "seed": seed,
    }
    job_id = create_training_job(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        params=hyperparams,
    )

    started_at = mark_job_running(connection, job_id)

    try:
        fv_result = get_feature_vectors(
            connection,
            symbol=symbol,
            timeframe=timeframe,
            feature_set=feature_set,
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
            X_all, y_all, times_all, test_ratio=test_ratio
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
            X_train,
            y_train,
            n_features=len(FEATURE_NAMES),
            learning_rate=learning_rate,
            n_epochs=n_epochs,
            batch_size=batch_size,
            l2_lambda=l2_lambda,
            seed=seed,
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
            symbol=symbol,
            timeframe=timeframe,
            feature_set=feature_set,
        )

        mark_job_done(
            connection,
            job_id,
            dataset=dataset_stats,
            metrics=metrics,
            model=model_dict,
            started_at=started_at,
        )
    except Exception as exc:
        mark_job_failed(connection, job_id, error=str(exc), started_at=started_at)

    return get_training_job(connection, job_id) or {}
