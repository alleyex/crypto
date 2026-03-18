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
- Reject new trades after the realized loss limit has been breached

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
export CRYPTO_MAX_DAILY_LOSS=50
export CRYPTO_DB_BACKEND=sqlite
export CRYPTO_SQLITE_PATH=storage/market_data.db
export CRYPTO_DATABASE_URL=
export TELEGRAM_BOT_TOKEN=
export TELEGRAM_CHAT_ID=8703043602
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

Open the admin UI:

```bash
http://127.0.0.1:8000/admin
```

The root path `/` now redirects to `/admin`.

Run with Docker Compose:

```bash
docker compose up --build
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

Read soak validation summary:

```bash
python scripts/read_soak_validation.py
```

Record and append a soak validation snapshot:

```bash
python scripts/read_soak_validation.py --record
```

Recommended soak validation acceptance target:

1. Keep the scheduler running continuously for 3 days.
2. Record at least 1 soak snapshot per day.
3. Confirm `scheduler/logs` keeps advancing with new `run=` lines.
4. Confirm `/validation/soak` stays out of `error`.
5. Confirm there are no unexpected `kill switch enabled` events.
6. Confirm `orders`, `fills`, `positions`, and `pnl` remain internally consistent.
7. Confirm Telegram alert delivery does not show repeated failures.

The scheduler now records a soak validation snapshot automatically after each scheduled run, so history will accumulate even without manual recording.
Set `CRYPTO_SOAK_ACTIVITY_STALENESS_SECONDS` if you want soak validation to mark old pipeline activity as degraded sooner or later.
Health and soak validation now also use runtime heartbeats from scheduler, pipeline, market data, and alerting components.

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

Manual operations guide:

- `docs/API_OPERATIONS.md`

Read endpoints:

- `GET /admin`
- `GET /alerts/status`
- `GET /audit-events`
- `GET /validation/soak`
- `GET /validation/soak/history`
- `GET /health`
- `GET /candles`
- `GET /signals`
- `GET /risk-events`
- `GET /orders`
- `GET /fills`
- `GET /positions`
- `GET /pnl`

Control endpoints:

- `POST /alerts/test`
- `POST /validation/soak/record`
- `POST /pipeline/run`
- `POST /signals/test`
- `POST /positions/rebuild`
- `POST /pnl/update`
- `GET /scheduler/status`
- `POST /scheduler/stop`
- `POST /scheduler/start`
- `GET /scheduler/logs?lines=20`
- `GET /kill-switch/status`
- `POST /kill-switch/enable`
- `POST /kill-switch/disable`

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
curl -s http://127.0.0.1:8000/kill-switch/status
curl -s -X POST http://127.0.0.1:8000/kill-switch/enable
curl -s -X POST http://127.0.0.1:8000/kill-switch/disable
curl -s http://127.0.0.1:8000/alerts/status
curl -s -X POST http://127.0.0.1:8000/alerts/test \
  -H "Content-Type: application/json" \
  -d '{"message":"Crypto alert test"}'
```

`GET /health` returns:

- database table status
- latest candle freshness
- latest pipeline activity
- scheduler stop flag and latest log line
- active runtime config values
- kill switch status
- active database backend info

`GET /admin` provides:

- health overview
- positions, orders, pnl, and scheduler log panels
- buttons for pipeline run, scheduler control, and kill switch control

`GET /audit-events` provides:

- recent structured audit records for pipeline, risk, scheduler, and kill switch actions

`GET /alerts/status` provides:

- whether Telegram alerting is configured

Current note:

- `CRYPTO_MAX_DAILY_LOSS` is enforced against the current UTC day realized PnL ledger rebuilt from `fills`
- previous-day realized losses do not trip today's daily loss limit

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
- Kill switch flag: `runtime/kill.switch`
- Scheduler log: `logs/scheduler.log`

## Current Limitations

- SQLite only
- Single market: `BTCUSDT`
- Single timeframe: `1m`
- Basic MA cross strategy only
- Paper trading only

## Next Recommended Work

- Add dashboard or admin UI
- Add alerting
- Add CI status badge and branch protection

## PostgreSQL Migration Path

The current runtime still uses SQLite, but the database configuration is now prepared for a future backend switch.

Details:

- `docs/POSTGRES_MIGRATION.md`

Important:

- `CRYPTO_DB_BACKEND=postgres` is not production-ready yet
- the current code will fail fast if PostgreSQL is selected before SQL compatibility work is completed

## Audit Log

Structured audit events are stored in the `audit_events` table.

Current event sources:

- pipeline runs
- risk evaluations
- scheduler start / stop control
- kill switch enable / disable control

## Telegram Alerts

Current Telegram alert triggers:

- scheduler stop flag set
- kill switch enabled
- health becomes degraded or error

Manual test endpoint:

- `POST /alerts/test`

Deduplication:

- health alerts are sent once per unique degraded/error state
- when health returns to `ok`, the alert state is cleared

Required environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Soak Validation

Use this to record a runtime summary during multi-day paper trading checks:

```bash
python scripts/read_soak_validation.py
```

The report includes:

- scheduler log summary
- key table row counts
- latest signal / order / pnl activity
- open position count and realized PnL summary
- a simple `ok` / `degraded` status with issues

## Docker Compose

The repository includes:

- `Dockerfile`
- `docker-compose.yml`

Services:

- `api`: FastAPI server on `http://127.0.0.1:8000`
- `scheduler`: runs `scripts/run_scheduler.py`

Shared bind mounts:

- `./storage:/app/storage`
- `./logs:/app/logs`
- `./runtime:/app/runtime`

Useful commands:

```bash
docker compose up --build
docker compose up -d
docker compose logs -f api
docker compose logs -f scheduler
docker compose down
```
- a simple `ok` / `degraded` status with issues
