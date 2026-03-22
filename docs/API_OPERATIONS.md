# API Operations Guide

This guide focuses on the manual operations you are most likely to run against the local API.

Base URL:

- `http://127.0.0.1:8000`

Start the API first:

```bash
/Users/alleyex/Projects/crypto/scripts/run_api.py
```

## 1. Check System Health

Use this before and after manual operations.

```bash
curl -s http://127.0.0.1:8000/health | python -m json.tool
```

What to check:

- top-level `status`
- `checks.scheduler.status`
- `checks.kill_switch.status`
- `checks.broker_protection.status`
- `checks.candles.status`
- `config` values such as risk limits

Broker/order protection can now degrade on:

- execution backend capability mismatches
- stale non-terminal orders
- repeated risk rejections

When it degrades, inspect:

- `checks.broker_protection.reason_code`
- `checks.broker_protection.severity`
- `checks.broker_protection.recommended_action`

## 2. Run One Pipeline Cycle

This runs:

`candles -> signals -> risk_events -> orders -> fills -> positions -> pnl`

```bash
curl -s -X POST http://127.0.0.1:8000/pipeline/run | python -m json.tool
```

Queue-native variants:

```bash
curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"],"orchestration":"queue_batch"}' \
  | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"],"orchestration":"queue_dispatch"}' \
  | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"],"orchestration":"queue_drain"}' \
  | python -m json.tool
```

Typical uses:

- verify end-to-end flow after a code change
- generate fresh paper-trading state
- check whether kill switch blocks execution
- test queue-native pipeline orchestration without switching to scheduler mode

Notes:

- `orchestration=queue_batch` is now the default runtime orchestration
- `orchestration=direct` remains available as a fallback path
- `orchestration=queue_batch` enqueues and drains a full pipeline batch in one request
- `orchestration=queue_dispatch` enqueues a full pipeline batch
- `orchestration=queue_drain` drains the next full queued pipeline batch

## 3. Insert a Manual Test Signal

Useful for force-testing the risk and execution path.

```bash
curl -s -X POST http://127.0.0.1:8000/signals/test \
  -H "Content-Type: application/json" \
  -d '{"signal_type":"BUY"}' | python -m json.tool
```

Other valid values:

- `SELL`
- `HOLD`

Recommended follow-up:

```bash
curl -s http://127.0.0.1:8000/signals | python -m json.tool
curl -s http://127.0.0.1:8000/risk-events | python -m json.tool
```

## 4. Read Current Trading State

Positions:

```bash
curl -s http://127.0.0.1:8000/positions | python -m json.tool
```

Orders:

```bash
curl -s http://127.0.0.1:8000/orders | python -m json.tool
```

Fills:

```bash
curl -s http://127.0.0.1:8000/fills | python -m json.tool
```

PnL snapshots:

```bash
curl -s http://127.0.0.1:8000/pnl | python -m json.tool
```

## 5. Rebuild Positions and PnL

If you changed data manually or want to re-derive state from fills:

```bash
curl -s -X POST http://127.0.0.1:8000/positions/rebuild | python -m json.tool
curl -s -X POST http://127.0.0.1:8000/pnl/update | python -m json.tool
```

## 6. Control the Scheduler

Check status:

```bash
curl -s http://127.0.0.1:8000/scheduler/status | python -m json.tool
```

Stop:

```bash
curl -s -X POST http://127.0.0.1:8000/scheduler/stop | python -m json.tool
```

Stop with explicit audit metadata:

```bash
curl -s -X POST http://127.0.0.1:8000/scheduler/stop \
  -H "Content-Type: application/json" \
  -d '{"audit_action":"broker_protection:pause_scheduler","audit_message":"Scheduler paused from broker protection recommendation."}' \
  | python -m json.tool
```

Start again:

```bash
curl -s -X POST http://127.0.0.1:8000/scheduler/start | python -m json.tool
```

Read recent scheduler logs:

```bash
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20" | python -m json.tool
```

## 7. Control the Kill Switch

Check status:

```bash
curl -s http://127.0.0.1:8000/kill-switch/status | python -m json.tool
```

Enable:

```bash
curl -s -X POST http://127.0.0.1:8000/kill-switch/enable | python -m json.tool
```

Enable with broker-protection metadata:

```bash
curl -s -X POST http://127.0.0.1:8000/kill-switch/enable \
  -H "Content-Type: application/json" \
  -d '{"reason":"Daily loss limit protection triggered.","source":"broker_protection","notify_message":"Crypto alert: broker protection enabled the kill switch."}' \
  | python -m json.tool
```

Disable:

```bash
curl -s -X POST http://127.0.0.1:8000/kill-switch/disable | python -m json.tool
```

Behavior:

- when enabled, `POST /pipeline/run` with `orchestration=direct` returns a blocked result
- `/health` will show the kill switch as degraded
- scheduler may still run, but trading execution is blocked at the pipeline entry

## 8. Record a Soak Validation Snapshot

Use this during multi-day paper-trading validation:

```bash
python scripts/read_soak_validation.py
```

Recommended cadence:

- once after startup
- once after enabling scheduler
- once per day during soak validation
- once before stopping the run

Suggested acceptance criteria for Stage 1 soak validation:

1. Let the system run for 3 continuous days before calling the soak check complete.
2. Record at least 1 snapshot per day with `python scripts/read_soak_validation.py --record` or `POST /validation/soak/record`.
3. Confirm recent scheduler logs continue to show fresh `run=` lines instead of only stop-flag lines.
4. Confirm `/validation/soak` is never `error`, and investigate any `degraded` result the same day.
5. Confirm no unexpected kill switch activations occur during the run.
6. Confirm `orders`, `fills`, `positions`, and `pnl` remain logically consistent.
7. Confirm Telegram alert delivery does not show sustained failures in `alert_delivery` audit events.

## 9. Market Data

Check candle freshness and gap status per symbol:

```bash
curl -s http://127.0.0.1:8000/candles/status | python -m json.tool
```

Fields per `(symbol, timeframe)`:

- `count` — total candles stored
- `earliest` / `latest` — UTC ISO timestamps
- `stale_seconds` — seconds since the most recent candle
- `has_gaps` / `gap_count_estimate` — gap detection

Fetch new candles without running the full pipeline:

```bash
curl -s -X POST http://127.0.0.1:8000/market-data/fetch \
  -H "Content-Type: application/json" \
  -d '{"symbols":["BTCUSDT","SOLUSDT"],"limit":100}' \
  | python -m json.tool
```

Fetch with historical start date (paginates Binance if > 1000 candles):

```bash
curl -s -X POST http://127.0.0.1:8000/market-data/fetch \
  -H "Content-Type: application/json" \
  -d '{"symbols":["BTCUSDT"],"start_date":"2026-03-01"}' \
  | python -m json.tool
```

Read recent candles (optional symbol filter):

```bash
curl -s "http://127.0.0.1:8000/candles?symbol=BTCUSDT&limit=10" | python -m json.tool
```

## 10. Data Retention

Purge old audit events and completed job queue rows:

```bash
curl -s -X POST http://127.0.0.1:8000/maintenance/retention \
  -H "Content-Type: application/json" \
  -d '{"audit_days":90,"job_queue_days":30}' \
  | python -m json.tool
```

Returns `deleted_audit_events` and `deleted_job_queue_rows` counts.

## 12. Reconcile Orders and Portfolio State

Use this when `broker_protection.recommended_action=inspect_and_reconcile_orders` or after manual order cleanup.

```bash
curl -s -X POST http://127.0.0.1:8000/orders/reconcile | python -m json.tool
```

With explicit audit metadata:

```bash
curl -s -X POST http://127.0.0.1:8000/orders/reconcile \
  -H "Content-Type: application/json" \
  -d '{"audit_action":"broker_protection:reconcile_orders","audit_message":"Order reconciliation triggered from broker protection recommendation."}' \
  | python -m json.tool
```

What it does:

- refreshes positions
- refreshes pnl snapshots
- returns latest orders for quick inspection
- writes an `execution_control` audit event

## 13. Switch Execution Backend

Check current backend:

```bash
curl -s http://127.0.0.1:8000/execution/backend | python -m json.tool
```

Switch to `paper`:

```bash
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"paper"}' | python -m json.tool
```

With broker-protection audit metadata:

```bash
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"paper","audit_action":"broker_protection:switch_to_paper_backend","audit_message":"Execution backend switched to paper from broker protection recommendation."}' \
  | python -m json.tool
```

Switch to Binance Spot backend:

```bash
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"binance"}' | python -m json.tool
```

Required environment variables before using Binance:

- `CRYPTO_BINANCE_API_KEY`
- `CRYPTO_BINANCE_API_SECRET`
- `CRYPTO_BINANCE_TESTNET=true`

Validate Binance account connectivity:

```bash
curl -s http://127.0.0.1:8000/execution/backend/check | python -m json.tool
python scripts/check_binance_backend.py
python scripts/check_binance_order.py --symbol BTCUSDT --side BUY --qty 0.001
```

Behavior:

- returns `status=skipped` when the active backend is not `binance`
- returns `status=ok` with account capability fields when signed Binance auth succeeds
- returns `status=error` when credentials are missing or the remote request fails
- `scripts/check_binance_order.py` uses Binance `POST /api/v3/order/test`, so it validates signing and order parameters without creating a real testnet order

## 14. Broker Protection Workflow

Suggested response flow when `/health` reports `checks.broker_protection.status=degraded`:

1. Read `/health` and capture `reason_code`, `severity`, and `recommended_action`.
2. Inspect `/orders`, `/risk-events`, and `/execution/backend`.
3. If action is `switch_to_paper_backend`, switch backend first.
4. If action is `pause_scheduler`, stop scheduler before further investigation.
5. If action is `enable_kill_switch`, enable the kill switch immediately.
6. If action is `inspect_and_reconcile_orders`, run `/orders/reconcile`.
7. Re-check `/health` and confirm broker protection returns to `ok`.

## Suggested Manual Workflow

1. Check `/health`
2. Confirm kill switch is disabled
3. Run one pipeline cycle
4. Inspect `signals`, `risk-events`, `orders`, `positions`, and `pnl`
5. Enable scheduler
6. Record soak validation snapshots over time

## Docker Compose Validation

Stage 1 Docker Compose runtime validation is complete when:

1. `docker compose up --build` starts both `api` and `scheduler`.
2. `docker compose ps` shows both services as `Up`.
3. `docker compose logs --tail=50` shows scheduler `run=` lines without container crashes.
4. `curl http://127.0.0.1:8000/health` returns a valid JSON payload with top-level `status`.

The project has now passed this local validation on macOS Apple Silicon with Docker Desktop.

## Common Recovery Actions

Scheduler stopped unexpectedly:

```bash
curl -s http://127.0.0.1:8000/scheduler/status | python -m json.tool
curl -s -X POST http://127.0.0.1:8000/scheduler/start | python -m json.tool
```

Kill switch left enabled:

```bash
curl -s http://127.0.0.1:8000/kill-switch/status | python -m json.tool
curl -s -X POST http://127.0.0.1:8000/kill-switch/disable | python -m json.tool
```

Need a clean operational snapshot:

```bash
curl -s http://127.0.0.1:8000/health | python -m json.tool
python scripts/read_soak_validation.py
```

Broker protection degraded:

```bash
curl -s http://127.0.0.1:8000/health | python -m json.tool
curl -s http://127.0.0.1:8000/orders | python -m json.tool
curl -s http://127.0.0.1:8000/risk-events | python -m json.tool
curl -s http://127.0.0.1:8000/execution/backend | python -m json.tool
```
