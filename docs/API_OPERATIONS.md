# API Operations Guide

This guide focuses on the manual operations you are most likely to run against the local API.

Base URL:

- `http://127.0.0.1:8000`

Start the API first:

```bash
python scripts/run_api.py
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
- `checks.candles.status`
- `config` values such as risk limits

## 2. Run One Pipeline Cycle

This runs:

`candles -> signals -> risk_events -> orders -> fills -> positions -> pnl`

```bash
curl -s -X POST http://127.0.0.1:8000/pipeline/run | python -m json.tool
```

Typical uses:

- verify end-to-end flow after a code change
- generate fresh paper-trading state
- check whether kill switch blocks execution

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

Disable:

```bash
curl -s -X POST http://127.0.0.1:8000/kill-switch/disable | python -m json.tool
```

Behavior:

- when enabled, `POST /pipeline/run` returns a blocked result
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

## Suggested Manual Workflow

1. Check `/health`
2. Confirm kill switch is disabled
3. Run one pipeline cycle
4. Inspect `signals`, `risk-events`, `orders`, `positions`, and `pnl`
5. Enable scheduler
6. Record soak validation snapshots over time

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
