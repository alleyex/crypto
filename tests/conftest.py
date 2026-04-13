"""
Global test fixtures.

The kill switch reads a runtime file (runtime/kill.switch) that may be present
on the developer's machine.  Without a fixture, any risk evaluation in tests
would be blocked by the live kill switch state.  This autouse fixture isolates
all tests from the host machine's kill switch file by default.

Tests that specifically exercise kill-switch-blocked behaviour should override
this via their own monkeypatch.setattr call.

The execution backend reads a runtime file (runtime/execution.backend) that
may contain "binance" on the developer's machine.  All tests default to the
"paper" backend to avoid unintended live network calls.
"""
import sqlite3
import os
from typing import Any

# Force PyTorch to use CPU in tests.  On Apple Silicon the MPS backend
# accumulates state across multiple neural-network initialisations within
# the same process and eventually triggers a SIGSEGV in torch.nn.init.orthogonal_().
# Disabling MPS before torch is first imported keeps every PPO model on the CPU
# path and avoids the crash when running the full test suite together.
try:
    import torch as _torch
    if hasattr(_torch.backends, "mps"):
        _torch.backends.mps.is_available = lambda: False  # type: ignore[method-assign]
        _torch.backends.mps.is_built = lambda: False  # type: ignore[method-assign]
    del _torch
except ImportError:
    pass

import pytest

from app.core.migrations import run_migrations

# Capture the value of CRYPTO_DATABASE_URL at conftest import time — before any
# test modules (including scripts) are collected and imported.  Some scripts call
# load_dotenv_file() at module level, which can set CRYPTO_DATABASE_URL from the
# local .env file and pollute os.environ for all tests that run afterwards.
# By snapshotting the value here we can distinguish "real postgres was explicitly
# configured by the caller" (should not patch) from "load_dotenv_file set it
# incidentally during collection" (should patch with SQLite).
_DATABASE_URL_AT_STARTUP: str | None = os.environ.get("CRYPTO_DATABASE_URL")


def make_connection() -> sqlite3.Connection:
    """Return a bare in-memory SQLite connection (no migrations, no row_factory)."""
    return sqlite3.connect(":memory:")


def make_conn() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with row_factory and migrations applied."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def make_kline(open_time: int, close: float) -> list:
    """Return a minimal Binance-style kline list for testing."""
    return [
        open_time,
        str(close - 1),
        str(close + 1),
        str(close - 2),
        str(close),
        "100",
        open_time + 59_999,
        "1000",
        10,
        "50",
        "500",
    ]


@pytest.fixture(autouse=True)
def _disable_kill_switch_for_tests(monkeypatch):
    monkeypatch.setattr("app.risk.risk_service.kill_switch_enabled", lambda: False)


@pytest.fixture(autouse=True)
def _reset_execution_backend_for_tests(monkeypatch, tmp_path):
    # Point EXECUTION_BACKEND_FILE to a non-existent temp path so that
    # read_configured_execution_backend() falls back to the env-var default
    # ("paper") rather than reading the developer's live runtime/execution.backend.
    # Tests that need a specific backend still work by setting EXECUTION_BACKEND
    # or creating their own backend file via a separate monkeypatch.
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND_FILE", tmp_path / "execution.backend")
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND", "paper")


@pytest.fixture(autouse=True)
def _patch_get_connection_for_tests(monkeypatch, tmp_path):
    """When CRYPTO_DATABASE_URL is not set, patch all module-level get_connection
    references to return a per-test file-backed SQLite connection.

    Tests that explicitly call monkeypatch.setattr("app.some.module.get_connection", ...)
    will override this patch for those specific paths.
    """
    if _DATABASE_URL_AT_STARTUP:
        return  # Real postgres was configured at startup — do not interfere.

    db_path = tmp_path / "test_shared.db"

    def make_conn():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        run_migrations(conn)
        return conn

    # Patch every module that imports get_connection at the module level.
    for module_path in (
        "app.api.main.get_connection",
        "app.health.checks.get_connection",
        "app.api.deps.get_connection",
        "app.api.routes.exchange.get_connection",
        "app.audit.service.get_connection",
        "app.system.heartbeat.get_connection",
        "app.scheduler.runner.get_connection",
        "app.pipeline.run_pipeline.get_connection",
        "app.validation.soak_report.get_connection",
    ):
        monkeypatch.setattr(module_path, make_conn)


class _PersistentConn:
    """Wraps a sqlite3 connection, ignoring close() so it can be reused across requests."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, params: Any = ()) -> Any:
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Any) -> Any:
        return self._conn.executemany(sql, seq_of_params)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        pass  # intentional no-op

    def really_close(self) -> None:
        self._conn.close()


def _make_api_conn() -> _PersistentConn:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return _PersistentConn(conn)
