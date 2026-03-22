# Crypto Trading System

An automated paper-trading system for Binance spot markets. Runs a full signal-to-execution pipeline on a scheduled loop, with a web-based admin console and Telegram alerting.

## Architecture

```
Binance API → candles → signals → risk_events → orders → fills → positions → pnl
```

**Core modules:**

| Module | Purpose |
|---|---|
| `app/data` | Binance kline fetch, candle storage |
| `app/strategy` | Signal generation (`ma_cross`, `momentum_3bar`) |
| `app/risk` | Risk rule evaluation |
| `app/execution` | Order execution (`paper`, `noop`, `simulated_live`, `binance`) |
| `app/pipeline` | End-to-end pipeline orchestration |
| `app/scheduler` | Scheduled pipeline loop |
| `app/api` | FastAPI REST API + admin UI |
| `app/alerting` | Telegram alert delivery |
| `app/ml` / `app/training` / `app/inference` | ML model training and inference |

## Setup

**Requirements:** Python 3.12, OpenSSL-backed build

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start

Start the API:

```bash
python scripts/run_api.py
```

Open the admin console:

```
http://127.0.0.1:8000/admin
```

Run one pipeline cycle:

```bash
curl -s -X POST http://127.0.0.1:8000/pipeline/run | python -m json.tool
```

Check system health:

```bash
curl -s http://127.0.0.1:8000/health | python -m json.tool
```

## Configuration

```bash
# Risk
export CRYPTO_ORDER_QTY=0.001
export CRYPTO_MAX_POSITION_QTY=0.002
export CRYPTO_COOLDOWN_SECONDS=300
export CRYPTO_MAX_DAILY_LOSS=50
export CRYPTO_ORDER_STALENESS_SECONDS=300
export CRYPTO_RISK_REJECTION_STREAK_THRESHOLD=3
export CRYPTO_CANDLE_STALENESS_SECONDS=600

# Database (defaults to SQLite)
export CRYPTO_DB_BACKEND=sqlite
export CRYPTO_SQLITE_PATH=storage/market_data.db
export CRYPTO_DATABASE_URL=          # PostgreSQL DSN when using postgres backend

# Execution backend
export CRYPTO_EXECUTION_BACKEND=paper   # paper | noop | simulated_live | binance

# Binance live/testnet credentials
export CRYPTO_BINANCE_API_KEY=
export CRYPTO_BINANCE_API_SECRET=
export CRYPTO_BINANCE_TESTNET=true

# Telegram alerting
export TELEGRAM_BOT_TOKEN=
export TELEGRAM_CHAT_ID=

# Development / CI
export CRYPTO_USE_FAKE_KLINES=        # set to 1 to bypass live Binance fetches
```

## Scheduler

**launchd (macOS, recommended for production):**

```bash
python scripts/install_launch_agent.py     # install and start
python scripts/read_launch_agent_status.py # check status
python scripts/uninstall_launch_agent.py   # remove
```

LaunchAgent label: `com.alleyex.crypto.scheduler`

**Direct run:**

```bash
python scripts/run_scheduler.py                              # continuous loop
python scripts/run_scheduler.py --interval 60 --iterations 1 # one cycle
python scripts/run_scheduler.py --mode market-data-only
python scripts/run_scheduler.py --mode strategy-only --strategy momentum_3bar
```

**Stop / clear:**

```bash
python scripts/set_stop_flag.py
python scripts/clear_stop_flag.py
```

## Docker Compose

```bash
docker compose up --build                             # api + scheduler (SQLite)
docker compose --profile split-workers up --build     # split into 4 worker services
docker compose --profile postgres up --build          # with PostgreSQL backend
```

Split worker environment variables:

```bash
export CRYPTO_DATA_INTERVAL=60
export CRYPTO_STRATEGY_INTERVAL=60
export CRYPTO_EXECUTION_INTERVAL=60
export CRYPTO_STRATEGY_NAME=momentum_3bar
```

## API Reference

Full operations guide: [`docs/API_OPERATIONS.md`](docs/API_OPERATIONS.md)

**Read:**

| Endpoint | Description |
|---|---|
| `GET /health` | System health (scheduler, broker protection, kill switch, candles) |
| `GET /admin` | Admin console UI |
| `GET /candles` | Recent candles (`?symbol=BTCUSDT&limit=10`) |
| `GET /candles/status` | Candle freshness and gap estimate per symbol |
| `GET /signals` | Recent signals |
| `GET /risk-events` | Recent risk evaluations |
| `GET /orders` | Recent orders |
| `GET /fills` | Recent fills |
| `GET /positions` | Current positions |
| `GET /pnl` | PnL snapshots |
| `GET /scheduler/status` | Scheduler state |
| `GET /scheduler/strategy` | Active strategy set |
| `GET /scheduler/logs` | Scheduler log tail (`?lines=20&mode=all`) |
| `GET /queue/summary` | Job queue summary |
| `GET /execution/backend` | Active execution backend |
| `GET /kill-switch/status` | Kill switch state |
| `GET /alerts/status` | Telegram alert config |
| `GET /audit-events` | Structured audit log |
| `GET /validation/soak` | Soak validation status |
| `GET /validation/soak/history` | Soak snapshot history |

**Control:**

| Endpoint | Description |
|---|---|
| `POST /pipeline/run` | Run one pipeline cycle |
| `POST /market-data/fetch` | Fetch candles without full pipeline |
| `POST /signals/test` | Insert a manual test signal |
| `POST /positions/rebuild` | Rebuild positions from fills |
| `POST /pnl/update` | Recalculate PnL snapshots |
| `POST /orders/reconcile` | Reconcile orders and portfolio state |
| `POST /scheduler/start` | Start the scheduler |
| `POST /scheduler/stop` | Stop the scheduler |
| `POST /scheduler/strategy` | Set active strategy set |
| `POST /scheduler/strategy/preset` | Apply a strategy preset |
| `POST /execution/backend` | Switch execution backend |
| `POST /kill-switch/enable` | Enable kill switch |
| `POST /kill-switch/disable` | Disable kill switch |
| `POST /alerts/test` | Send a test Telegram alert |
| `POST /validation/soak/record` | Record a soak snapshot |
| `POST /maintenance/retention` | Purge old audit events and job queue rows |

## Execution Backends

| Backend | Behavior |
|---|---|
| `paper` | Default. Writes orders and fills. No real exchange. |
| `noop` | Dry run. Never writes orders or fills. |
| `simulated_live` | Uses broker abstraction without a real exchange. |
| `binance` | Live Binance Spot API. Requires `CRYPTO_BINANCE_API_KEY` and secret. |

Validate Binance connectivity:

```bash
python scripts/check_binance_backend.py
python scripts/check_binance_order.py --symbol BTCUSDT --side BUY --qty 0.001
```

## Risk Rules

- Reject `HOLD` signals
- Reject duplicate signal direction
- Reject `BUY` when a long position is already open
- Reject trades during cooldown after latest fill
- Reject `BUY` if resulting position exceeds max position limit
- Reject new trades after daily loss limit is breached

## Alerting

Telegram alerts fire on:

- Scheduler stop flag set
- Kill switch enabled
- Health status becomes `degraded` or `error`
- Queue has failed jobs
- Broker protection degrades
- Split worker heartbeats become stale

Alerts are deduplicated — each unique state triggers one alert. Recovery clears the state.

## Soak Validation

```bash
python scripts/read_soak_validation.py           # print current status
python scripts/read_soak_validation.py --record  # record and append a snapshot
```

Acceptance criteria for a clean soak run:

1. Scheduler runs continuously for 3 days with new `run=` lines in logs
2. At least 1 snapshot recorded per day
3. `/validation/soak` never returns `error`
4. No unexpected kill switch activations
5. `orders`, `fills`, `positions`, and `pnl` remain internally consistent
6. Telegram alert delivery has no repeated failures

## Important Paths

| Path | Purpose |
|---|---|
| `storage/market_data.db` | SQLite database |
| `runtime/scheduler.stop` | Scheduler stop flag |
| `runtime/kill.switch` | Kill switch flag |
| `logs/scheduler.log` | Scheduler log |
| `logs/launchd.stdout.log` | launchd stdout |
| `logs/launchd.stderr.log` | launchd stderr |

## Tests

```bash
python -m pytest tests/test_trading_flow.py -q
```

## Documentation

- [`docs/API_OPERATIONS.md`](docs/API_OPERATIONS.md) — manual operations and recovery workflows
- [`docs/POSTGRES.md`](docs/POSTGRES.md) — PostgreSQL setup and validation record
