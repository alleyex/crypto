# Crypto Trading MVP

Minimal crypto trading MVP for `BTCUSDT` on Binance spot market.

Current stack:

- Python
- SQLite
- FastAPI
- launchd scheduler on macOS

Project path:

- `/Users/alleyex/Projects/crypto`

## What It Does

The current MVP supports:

- Fetching Binance `BTCUSDT` `1m` klines
- Saving candle data into SQLite
- Generating a simple moving-average signal
- Evaluating basic risk rules
- Executing paper trades
- Rebuilding positions
- Updating PnL snapshots
- Running on a fixed scheduler interval
- Querying and controlling the system via API

Main flow:

`candles -> signals -> risk_events -> orders -> fills -> positions -> pnl`

Current default risk rules:

- Reject `HOLD`
- Reject duplicate signal types
- Reject `BUY` when an existing long position is already open
- Reject trades during cooldown after the latest fill
- Reject `BUY` if the resulting position would exceed the configured max position

## Setup

Create and use the virtual environment:

```bash
cd /Users/alleyex/Projects/crypto
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional runtime configuration:

```bash
export CRYPTO_ORDER_QTY=0.001
export CRYPTO_MAX_POSITION_QTY=0.002
export CRYPTO_COOLDOWN_SECONDS=300
export CRYPTO_CANDLE_STALENESS_SECONDS=600
```

## Main Commands

Run one pipeline cycle:

```bash
python scripts/run_pipeline.py
```

Run the scheduler once for testing:

```bash
python scripts/run_scheduler.py --interval 1 --iterations 1
```

Run the scheduler continuously:

```bash
python scripts/run_scheduler.py
```

Run the API locally:

```bash
python scripts/run_api.py
```

## CLI Utilities

Insert a manual test signal:

```bash
python scripts/insert_test_signal_sqlite.py BUY
python scripts/insert_test_signal_sqlite.py SELL
python scripts/insert_test_signal_sqlite.py HOLD
```

Read current position:

```bash
python scripts/read_positions_sqlite.py
```

Read scheduler log:

```bash
python scripts/read_scheduler_log.py
```

Stop the scheduler:

```bash
python scripts/set_stop_flag.py
```

Clear the stop flag:

```bash
python scripts/clear_stop_flag.py
```

Check stop flag status:

```bash
python scripts/read_stop_flag.py
```

## API Endpoints

Read endpoints:

- `GET /health`
- `GET /candles`
- `GET /signals`
- `GET /risk-events`
- `GET /orders`
- `GET /fills`
- `GET /positions`
- `GET /pnl`

Control endpoints:

- `POST /pipeline/run`
- `POST /signals/test`
- `POST /positions/rebuild`
- `POST /pnl/update`
- `GET /scheduler/status`
- `POST /scheduler/stop`
- `POST /scheduler/start`
- `GET /scheduler/logs?lines=20`

Example API usage:

```bash
curl -s http://127.0.0.1:8000/health
curl -s -X POST http://127.0.0.1:8000/pipeline/run
curl -s -X POST http://127.0.0.1:8000/signals/test \
  -H "Content-Type: application/json" \
  -d '{"signal_type":"SELL"}'
curl -s http://127.0.0.1:8000/scheduler/status
curl -s -X POST http://127.0.0.1:8000/scheduler/stop
curl -s -X POST http://127.0.0.1:8000/scheduler/start
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20"
```

`GET /health` returns:

- database table status
- latest candle freshness
- latest pipeline activity
- scheduler stop flag and latest log line
- active runtime config values

## launchd

Install the LaunchAgent:

```bash
python scripts/install_launch_agent.py
```

Check LaunchAgent status:

```bash
python scripts/read_launch_agent_status.py
```

Remove the LaunchAgent:

```bash
python scripts/uninstall_launch_agent.py
```

The scheduler LaunchAgent label is:

- `com.alleyex.crypto.scheduler`

Logs:

- `logs/scheduler.log`
- `logs/launchd.stdout.log`
- `logs/launchd.stderr.log`

## Important Paths

- Database: `storage/market_data.db`
- Scheduler stop flag: `runtime/scheduler.stop`
- Scheduler log: `logs/scheduler.log`

## Current Limitations

- SQLite only
- Single market: `BTCUSDT`
- Single timeframe: `1m`
- Basic MA cross strategy only
- Paper trading only

## Next Recommended Work

- Add PostgreSQL migration path
- Add API docs for manual operations
- Add dashboard or admin UI
- Add daily loss limit and alerting
- Add CI status badge and branch protection
