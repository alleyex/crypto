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
import pytest


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
