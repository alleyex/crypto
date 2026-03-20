from pathlib import Path
from typing import Dict, List, Union

from app.audit.service import log_event
from app.core.settings import EXECUTION_BACKEND


RUNTIME_DIR = Path("runtime")
EXECUTION_BACKEND_FILE = RUNTIME_DIR / "execution.backend"
SUPPORTED_EXECUTION_BACKENDS = ("paper", "noop", "simulated_live", "binance")


def list_supported_execution_backends() -> List[str]:
    return list(SUPPORTED_EXECUTION_BACKENDS)


def read_configured_execution_backend() -> str:
    if not EXECUTION_BACKEND_FILE.exists():
        return EXECUTION_BACKEND
    configured_name = EXECUTION_BACKEND_FILE.read_text(encoding="utf-8").strip().lower()
    if configured_name in SUPPORTED_EXECUTION_BACKENDS:
        return configured_name
    return EXECUTION_BACKEND


def set_execution_backend(
    backend_name: str,
    *,
    audit_action: str = "set_execution_backend",
    audit_message: str = "Execution backend updated.",
) -> Dict[str, str]:
    normalized_name = str(backend_name).strip().lower()
    if normalized_name not in SUPPORTED_EXECUTION_BACKENDS:
        raise ValueError(f"Unsupported execution backend: {normalized_name}")
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    EXECUTION_BACKEND_FILE.write_text(f"{normalized_name}\n", encoding="utf-8")
    log_event(
        event_type="execution_control",
        status="updated",
        source="execution_control",
        message=audit_message,
        payload={
            "action": audit_action,
            "backend": normalized_name,
            "execution_backend_file": str(EXECUTION_BACKEND_FILE),
        },
    )
    return {
        "backend": normalized_name,
        "execution_backend_file": str(EXECUTION_BACKEND_FILE),
    }


def get_execution_backend_runtime_status() -> Dict[str, Union[str, List[str]]]:
    return {
        "backend": read_configured_execution_backend(),
        "default_backend": EXECUTION_BACKEND,
        "available_backends": list_supported_execution_backends(),
        "execution_backend_file": str(EXECUTION_BACKEND_FILE),
    }
