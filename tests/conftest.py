"""
Global test fixtures.

The kill switch reads a runtime file (runtime/kill.switch) that may be present
on the developer's machine.  Without a fixture, any risk evaluation in tests
would be blocked by the live kill switch state.  This autouse fixture isolates
all tests from the host machine's kill switch file by default.

Tests that specifically exercise kill-switch-blocked behaviour should override
this via their own monkeypatch.setattr call.
"""
import pytest


@pytest.fixture(autouse=True)
def _disable_kill_switch_for_tests(monkeypatch):
    monkeypatch.setattr("app.risk.risk_service.kill_switch_enabled", lambda: False)
