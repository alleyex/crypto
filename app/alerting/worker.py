from pathlib import Path
from typing import Any

from app.alerting.base import run_alert
from app.alerting.state import build_fingerprint

RUNTIME_DIR = Path("runtime")
WORKER_ALERT_STATE_FILE = RUNTIME_DIR / "worker_alert_state.json"


def _stale_workers(report: dict[str, Any]) -> list[dict[str, Any]]:
    heartbeats = report.get("checks", {}).get("heartbeats", {})
    components = heartbeats.get("components", []) if isinstance(heartbeats, dict) else []
    return [
        item
        for item in components
        if isinstance(item, dict) and item.get("stale") and str(item.get("component", "")).endswith("_worker")
    ]


def _build_fingerprint(report: dict[str, Any]) -> str:
    return build_fingerprint([
        {"component": item.get("component"), "status": item.get("status")}
        for item in _stale_workers(report)
    ])


def _build_message(report: dict[str, Any]) -> str:
    worker_bits = [
        f"{item.get('component')} age={item.get('age_seconds', 'unknown')}s"
        for item in _stale_workers(report)
    ]
    return "Crypto alert: stale worker heartbeats detected. Workers: {workers}".format(
        workers=", ".join(worker_bits),
    )


def maybe_send_worker_alert(report: dict[str, Any]) -> dict[str, Any]:
    return run_alert(
        WORKER_ALERT_STATE_FILE,
        report,
        is_ok=lambda r: not _stale_workers(r),
        ok_reason="No stale worker heartbeats.",
        duplicate_reason="Worker alert already sent for current stale state.",
        fingerprint_fn=_build_fingerprint,
        message_fn=_build_message,
        state_fn=lambda fp, r: {"fingerprint": fp, "worker_count": len(_stale_workers(r))},
    )
