from typing import Any, Union

import requests
from fastapi import APIRouter
from pydantic import BaseModel

from app.alerting.telegram import send_telegram_message, telegram_configured
from app.execution.adapter import get_execution_backend_status
from app.execution.runtime import get_execution_backend_runtime_status, set_execution_backend

router = APIRouter()

class ExecutionBackendRequest(BaseModel):
    backend: str
    audit_action: str | None = None
    audit_message: str | None = None

class AlertTestRequest(BaseModel):
    message: str = "Crypto alert test message."

@router.get("/alerts/status")
def alerts_status() -> dict[str, bool]:
    return {"telegram_configured": telegram_configured()}

@router.post("/alerts/test")
def alerts_test(payload: AlertTestRequest) -> dict[str, Any]:
    return send_telegram_message(payload.message)

@router.get("/execution/backend")
def execution_backend() -> dict[str, Union[bool, str, list[str]]]:
    return {
        **get_execution_backend_status(),
        **get_execution_backend_runtime_status(),
    }

@router.post("/execution/backend")
def execution_backend_update(payload: ExecutionBackendRequest) -> dict[str, Union[bool, str, list[str]]]:
    set_execution_backend(
        payload.backend,
        audit_action=payload.audit_action or f"set_execution_backend:{payload.backend}",
        audit_message=payload.audit_message or f"Execution backend set to {payload.backend}.",
    )
    return {
        **get_execution_backend_status(),
        **get_execution_backend_runtime_status(),
    }

@router.get("/execution/backend/check")
def execution_backend_check() -> dict[str, Union[bool, str, int]]:
    backend_status = get_execution_backend_status()
    backend = str(backend_status["backend"])
    if backend != "binance":
        return {
            "status": "skipped",
            "backend": backend,
            "reason": "Remote connectivity checks are only implemented for the binance backend.",
        }

    try:
        from app.execution.binance_broker import BinanceBrokerClient

        client = BinanceBrokerClient()
        return client.check_account_connectivity()
    except ValueError as exc:
        return {
            "status": "error",
            "backend": backend,
            "reason": str(exc),
        }
    except requests.RequestException as exc:
        return {
            "status": "error",
            "backend": backend,
            "reason": str(exc),
        }
