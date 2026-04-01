from scripts import run_futures_orderbook_collector as worker


def test_should_restart_collector_when_ws_unavailable() -> None:
    result = {
        "source_counts": {"rest": 6, "ws": 0},
        "collector": {
            "ws_available": False,
            "symbol_runtime": {},
        },
    }
    assert worker._should_restart_collector(result, ["BTCUSDT", "ETHUSDT"]) is True


def test_should_not_restart_collector_when_ws_runtime_present() -> None:
    result = {
        "source_counts": {"rest": 5, "ws": 1},
        "collector": {
            "ws_available": True,
            "symbol_runtime": {
                "BTCUSDT": {"sample_count": 1},
            },
        },
    }
    assert worker._should_restart_collector(result, ["BTCUSDT", "ETHUSDT"]) is False


def test_notify_watchdog_restart_logs_and_alerts(monkeypatch) -> None:
    captured = {}

    monkeypatch.setattr(
        worker,
        "log_event",
        lambda event_type, status, source, message, payload=None: captured.update(
            {
                "event_type": event_type,
                "status": status,
                "source": source,
                "message": message,
                "payload": payload,
            }
        ),
    )
    monkeypatch.setattr(worker, "send_telegram_message", lambda text: {"sent": False, "text": text})

    result = {
        "source_counts": {"rest": 6, "ws": 0},
        "collector": {"ws_available": False, "symbol_runtime": {}},
    }
    worker._notify_watchdog_restart(3, ["BTCUSDT"], result)

    assert captured["event_type"] == "futures_orderbook_watchdog_restart"
    assert captured["status"] == "warning"
    assert captured["source"] == "futures_orderbook_collector"
    assert captured["payload"]["run_count"] == 3
    assert captured["payload"]["symbols"] == ["BTCUSDT"]
