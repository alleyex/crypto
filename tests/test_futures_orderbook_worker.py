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
