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

Recommended local Python baseline:

- Python `3.12`
- OpenSSL-backed build (not LibreSSL)

Create and use the virtual environment:

```bash
cd /Users/alleyex/Projects/crypto
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To verify the interpreter is using OpenSSL:

```bash
python - <<'PY'
import ssl
print(ssl.OPENSSL_VERSION)
PY
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
export CRYPTO_POSTGRES_CONNECT_RETRIES=15
export CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS=1
export CRYPTO_USE_FAKE_KLINES=
export CRYPTO_FAKE_KLINE_CLOSES=
export CRYPTO_EXECUTION_BACKEND=paper
export CRYPTO_ORDER_STALENESS_SECONDS=300
export CRYPTO_RISK_REJECTION_STREAK_THRESHOLD=3
export TELEGRAM_BOT_TOKEN=
export TELEGRAM_CHAT_ID=8703043602
```

PostgreSQL smoke-test example:

```bash
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@127.0.0.1:5432/crypto
```

## Main Commands

Run one pipeline cycle:

```bash
python scripts/run_pipeline.py
```

Run the scheduler once for testing:

```bash
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --interval 1 --iterations 1
```

Run the scheduler continuously:

```bash
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py
```

Run the scheduler in a specific job mode:

```bash
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode pipeline
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode market-data-only
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode strategy-only
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode execution-only
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode strategy-only --queue-dispatch
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode strategy-only --queue-drain
```

Run a specific strategy job:

```bash
python scripts/run_strategy_job.py --strategy ma_cross
python scripts/run_strategy_job.py --strategy momentum_3bar
/Users/alleyex/Projects/crypto/scripts/run_scheduler.py --mode strategy-only --strategy momentum_3bar
```

Execution backend modes:

- `paper`
  - default paper-trading broker
  - writes orders and fills
- `noop`
  - dry-run execution backend
  - never writes orders or fills
- `simulated_live`
  - live-style backend backed by a simulated broker client
  - uses the broker abstraction without talking to a real exchange
- `binance`
  - live execution backend using Binance Spot API
  - supports Spot testnet credentials and account connectivity checks

Read or update the active execution backend used by runtime jobs:

```bash
curl -s http://127.0.0.1:8000/execution/backend
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"noop"}'
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"simulated_live"}'
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"binance"}'
curl -s http://127.0.0.1:8000/execution/backend/check
python scripts/check_binance_backend.py
python scripts/check_binance_order.py --symbol BTCUSDT --side BUY --qty 0.001
```

Binance backend configuration:

```bash
export CRYPTO_BINANCE_API_KEY=your_testnet_api_key
export CRYPTO_BINANCE_API_SECRET=your_testnet_api_secret
export CRYPTO_BINANCE_TESTNET=true
```

`GET /execution/backend/check` and `python scripts/check_binance_backend.py` perform a signed Binance account call when testnet credentials are configured.
`python scripts/check_binance_order.py` performs a signed Binance `order/test` validation so you can verify symbol, side, quantity, and signature without placing a real testnet order.

Set the active runtime strategy set used by pipeline and strategy-only scheduler loops:

```bash
curl -s http://127.0.0.1:8000/scheduler/strategy
curl -s -X POST http://127.0.0.1:8000/scheduler/strategy \
  -H "Content-Type: application/json" \
  -d '{"strategy_names":["ma_cross","momentum_3bar"]}'
curl -s -X POST http://127.0.0.1:8000/scheduler/strategy/preset \
  -H "Content-Type: application/json" \
  -d '{"preset":"active_first"}'
curl -s -X POST http://127.0.0.1:8000/scheduler/strategy/limit-preset \
  -H "Content-Type: application/json" \
  -d '{"preset":"top_2"}'
```

Run the API locally:

```bash
/Users/alleyex/Projects/crypto/scripts/run_api.py
```

The API launcher will automatically re-exec into the project virtualenv Python at `/Users/alleyex/Projects/crypto/.venv/bin/python` when needed, so it does not stay on an older system interpreter by accident.

The scheduler launcher applies the same guard and will automatically re-exec into `/Users/alleyex/Projects/crypto/.venv/bin/python` when needed.

Open the admin UI:

```bash
http://127.0.0.1:8000/admin
```

The root path `/` now redirects to `/admin`.

Run with Docker Compose:

```bash
docker compose up --build
```

Run split worker services with Docker Compose:

```bash
docker compose --profile split-workers up --build
```

Optional split worker intervals:

```bash
export CRYPTO_DATA_INTERVAL=60
export CRYPTO_STRATEGY_INTERVAL=60
export CRYPTO_EXECUTION_INTERVAL=60
```

Run PostgreSQL smoke test with Docker Compose:

```bash
docker compose --profile postgres up -d postgres
python scripts/run_postgres_smoke.py
```

Run clean PostgreSQL Compose validation end-to-end:

```bash
python scripts/run_postgres_compose_validation.py --mode compose-runtime
```

The PostgreSQL validation script now injects fake kline input automatically so CI validates runtime/database behavior without depending on live Binance availability.

Available validation modes:

- `smoke`
- `compose-runtime`
- `compose-soak-readability`

Run API and scheduler on PostgreSQL with Docker Compose:

```bash
export CRYPTO_DB_BACKEND=postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto
docker compose --profile postgres up --build
```

Docker Compose runtime validation status:

- verified on macOS Apple Silicon with Docker Desktop
- `api` and `scheduler` containers both start successfully
- split worker services now also exist behind the `split-workers` Compose profile: `data-worker`, `strategy-worker`, `risk-worker`, and `execution-worker`
- split worker services can now run at different intervals via `CRYPTO_DATA_INTERVAL`, `CRYPTO_STRATEGY_INTERVAL`, `CRYPTO_RISK_INTERVAL`, and `CRYPTO_EXECUTION_INTERVAL`
- pipeline and strategy workers can now select a registered strategy via `CRYPTO_STRATEGY_NAME` (currently `ma_cross` or `momentum_3bar`)
- split worker scheduler modes can also run in queue-dispatch or queue-drain mode via `python scripts/run_scheduler.py --queue-dispatch|--queue-drain`
- admin now exposes queue controls for enqueueing strategy jobs and draining strategy/risk/execution jobs
- admin queue controls also support retrying the latest failed strategy, risk, or execution job
- admin queue summary now exposes per-job-type counts, failure/attempt metrics, retry counts, failure streaks, per-job-type latest failed/retried markers, recent terminal status trends/trend strings, overall latest failed/retried job details, and filterable recent jobs
- health/admin now also expose `broker_protection`, which can degrade on execution-backend capability mismatches, stale non-terminal orders, and repeated risk rejections
- broker protection now emits `reason_code`, `severity`, and `recommended_action` so alerts and admin can distinguish backend, stale-order, and reject-streak cases
- admin issue chips now expose one-click broker-protection actions for switching back to `paper`, pausing the scheduler, enabling the kill switch, and reconciling orders
- `curl http://127.0.0.1:8000/health` returns `status: ok` under Compose
- PostgreSQL Compose startup now tolerates database boot lag via application-level connection retry
- GitHub Actions now uses two workflows: `CI` for the core test suite and `Postgres Validation` for PostgreSQL smoke/runtime/readability checks
- both workflows now support manual `workflow_dispatch` runs from the Actions UI; `CI` exposes a `python_version` override and `Postgres Validation` exposes scope plus runtime overrides
- `CI` now uploads a test-results artifact containing `summary.md`, `junit.xml`, and a `manifest.json`; both the summary and manifest include `outcome`, the manifest file list now covers `summary.md`, `junit.xml`, and `manifest.json`, and the artifact metadata is generated by `scripts/write_test_artifact.py`
- `Postgres Validation` also supports manual `workflow_dispatch` runs from the Actions UI, with `all`, `smoke`, `runtime`, or `readability` scope selection plus optional `database_url`, `api_port`, `artifact_retention_days`, and `python_version` overrides
- the GitHub Actions UI labels these jobs as `Test Suite (PR/Push)`, `Postgres Quick Check (PR/Push)`, `Postgres Runtime Check`, and nightly `Postgres Readability Check`
- pull requests run `test` and `postgres-smoke`; full `postgres-compose-validation` runs on `main` pushes and nightly schedule
- the nightly PostgreSQL validation uses `compose-soak-readability` so `/validation/soak` and history are exercised under PostgreSQL
- nightly CI summary now includes `validation_layer`, human-readable `verdict`, `soak_status`, soak history count, and the latest recorded snapshot timestamp
- GitHub Actions step names now also surface the verdict directly, for example `Validate PostgreSQL (quick-check)` and `Validate PostgreSQL (readability-check)`
- GitHub Actions artifact names now also encode event source, for example `postgres-smoke-validation-pull_request` and `postgres-compose-validation-push`
- all PostgreSQL validation outputs now include consistent metadata fields like `mode`, `validation_layer`, `verdict`, `event_name`, `run_id`, and `generated_at`
- every PostgreSQL validation artifact now also includes `manifest.json` with `artifact_kind`, `validation_layer`, `verdict`, and per-file `size_bytes` / `sha256`; smoke artifacts use `summary.md`, `result.json`, and `raw.log`, while compose-based artifacts also include `docker.log` plus `services/api.log`, `services/scheduler.log`, and `services/postgres.log`; this artifact metadata is now written by `scripts/write_postgres_validation_artifact.py`
- both artifact writers now share common checksum/file-entry helpers in `scripts/artifact_utils.py`, so CI and PostgreSQL validation manifests stay structurally aligned
- both workflows now also share a local composite action at `.github/actions/setup-python-project` for Python setup and dependency installation, so workflow YAML only keeps job-specific logic
- artifact uploads are now also routed through a shared local composite action at `.github/actions/upload-artifact`, so naming, path wiring, and retention handling are centralized at the workflow boundary
- PostgreSQL smoke/runtime/readability jobs now also share `.github/actions/run-postgres-validation`, so the workflow only passes `mode`, `database_url`, `api_port`, and artifact directory while the command wiring stays centralized
- the `CI` workflow now also uses `.github/actions/run-test-suite`, so test execution and test artifact generation follow the same local-action pattern as PostgreSQL validation
- the top-level `Postgres Validation` workflow now calls a reusable workflow at `.github/workflows/postgres-validation-job.yml`, so smoke and runtime/readability share the same job skeleton instead of duplicating checkout/setup/run/upload/cleanup
- the top-level `CI` workflow now also calls a reusable workflow at `.github/workflows/ci-job.yml`, so both top-level workflows mainly keep triggers and event routing while job execution lives in reusable workflow files
- both top-level workflows now declare minimal `contents: read` permissions and workflow-level `concurrency`; branch/PR reruns cancel older in-flight runs, while `schedule` runs are left uninterrupted
- reusable workflow files now expose basic outputs like `artifact_name`, `artifact_dir`, and PostgreSQL `mode`, so future notification or aggregate-summary steps can consume stable metadata without recomputing it in the caller
- top-level workflows now also consume those reusable-workflow outputs in small summary jobs, so artifact metadata is surfaced in the Actions summary without recalculating names or mode in a second place
- those summary jobs now also share a local composite action at `.github/actions/write-workflow-summary`, so summary markdown formatting is centralized instead of repeated inline in workflow YAML
- top-level workflows now also call a reusable workflow at `.github/workflows/workflow-summary-job.yml`, so summary jobs themselves no longer duplicate the runner/step wrapper around summary rendering
- reusable workflow files now also carry minimal `contents: read` permissions, and the summary reusable workflow has its own short timeout, so execution policy is aligned inside the reusable layer instead of only at the top level
- the PostgreSQL CI job publishes a markdown step summary and JSON artifact for the validation result
- PostgreSQL validation now treats `POST /pipeline/run` as successful only when the returned step payload does not contain `failed` or `blocked` states
- PostgreSQL validation no longer depends on live Binance availability during CI; it runs with `CRYPTO_USE_FAKE_KLINES=1`
- PostgreSQL migration bootstrap is now serialized with a PostgreSQL advisory lock so API, scheduler, and pipeline startup do not race each other on `schema_migrations`
- GitHub Actions workflow dependencies are now pinned to Node 24-compatible versions: `actions/checkout@v5`, `actions/setup-python@v6`, and `actions/upload-artifact@v6`
- Stage 1 local container startup verification is complete

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

Read scheduler logs by mode through the API:

```bash
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20&mode=all"
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20&mode=pipeline"
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20&mode=strategy-only"
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

Stage 1 completion note:

- Docker Compose local runtime verification is complete.
- The main remaining Stage 1 item is collecting a real 3-day soak validation record.

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
- `docs/POSTGRES_RUNTIME_VALIDATION.md`

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
- `POST /orders/reconcile`
- `GET /scheduler/status`
- `GET /scheduler/strategy`
- `POST /scheduler/stop`
- `POST /scheduler/start`
- `POST /scheduler/strategy`
- `POST /scheduler/strategy/preset`
- `POST /scheduler/strategy/limit-preset`
- `GET /queue/jobs`
- `GET /queue/summary`
- `GET /execution/backend`
- `POST /queue/jobs`
- `POST /queue/jobs/enqueue-pipeline`
- `POST /queue/jobs/run-next`
- `POST /queue/jobs/run-next-pipeline`
- `POST /queue/jobs/{job_id}/retry`
- `POST /execution/backend`
- `GET /scheduler/logs?lines=20`
- `GET /scheduler/logs?lines=20&mode=all|pipeline|market-data-only|strategy-only|risk-only|execution-only`
- `GET /kill-switch/status`
- `POST /kill-switch/enable`
- `POST /kill-switch/disable`

Example API usage:

```bash
curl -s http://127.0.0.1:8000/health
curl -s -X POST http://127.0.0.1:8000/pipeline/run
curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"]}'
curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"],"orchestration":"queue_batch"}'
curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"],"orchestration":"queue_dispatch"}'
curl -s -X POST http://127.0.0.1:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"strategy_name":"momentum_3bar","symbol_names":["BTCUSDT","ETHUSDT"],"orchestration":"queue_drain"}'
curl -s -X POST http://127.0.0.1:8000/signals/test \
  -H "Content-Type: application/json" \
  -d '{"signal_type":"SELL"}'
curl -s http://127.0.0.1:8000/scheduler/status
curl -s http://127.0.0.1:8000/scheduler/strategy
curl -s -X POST http://127.0.0.1:8000/scheduler/stop
curl -s -X POST http://127.0.0.1:8000/scheduler/start
curl -s -X POST http://127.0.0.1:8000/scheduler/strategy \
  -H "Content-Type: application/json" \
  -d '{"strategy_names":["ma_cross","momentum_3bar"]}'
curl -s http://127.0.0.1:8000/scheduler/symbols
curl -s -X POST http://127.0.0.1:8000/scheduler/symbols \
  -H "Content-Type: application/json" \
  -d '{"symbol_names":["BTCUSDT","ETHUSDT","SOLUSDT"]}'
curl -s -X POST http://127.0.0.1:8000/scheduler/strategy/preset \
  -H "Content-Type: application/json" \
  -d '{"preset":"reverse"}'
curl -s -X POST http://127.0.0.1:8000/scheduler/strategy/limit-preset \
  -H "Content-Type: application/json" \
  -d '{"preset":"all_enabled"}'
curl -s http://127.0.0.1:8000/queue/jobs
curl -s http://127.0.0.1:8000/queue/summary
curl -s http://127.0.0.1:8000/execution/backend
curl -s -X POST http://127.0.0.1:8000/execution/backend \
  -H "Content-Type: application/json" \
  -d '{"backend":"simulated_live"}'
curl -s -X POST http://127.0.0.1:8000/orders/reconcile
curl -s -X POST http://127.0.0.1:8000/queue/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_type":"strategy","strategy_names":["ma_cross","momentum_3bar"],"symbol_names":["BTCUSDT","ETHUSDT"]}'
curl -s -X POST http://127.0.0.1:8000/queue/jobs/enqueue-pipeline \
  -H "Content-Type: application/json" \
  -d '{"strategy_names":["ma_cross","momentum_3bar"],"symbol_names":["BTCUSDT","ETHUSDT"]}'
curl -s -X POST http://127.0.0.1:8000/queue/jobs/run-next \
  -H "Content-Type: application/json" \
  -d '{"job_type":"strategy"}'
curl -s -X POST http://127.0.0.1:8000/queue/jobs/run-next-pipeline
curl -s -X POST http://127.0.0.1:8000/queue/jobs/42/retry
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20"
curl -s "http://127.0.0.1:8000/scheduler/logs?lines=20&mode=execution-only"
curl -s http://127.0.0.1:8000/kill-switch/status
curl -s -X POST http://127.0.0.1:8000/kill-switch/enable
curl -s -X POST http://127.0.0.1:8000/kill-switch/disable
curl -s http://127.0.0.1:8000/alerts/status
curl -s -X POST http://127.0.0.1:8000/alerts/test \
  -H "Content-Type: application/json" \
  -d '{"message":"Crypto alert test"}'
```

Queued pipeline batches now carry a shared `batch_id` and execute in dependency order `market_data -> strategy -> risk -> execution`. `/queue/summary` / admin queue debug show recent batch status snapshots plus the latest incomplete/completed batch, `POST /queue/jobs/run-next-pipeline` still drains the next queued step from the oldest pending batch, and `POST /pipeline/run` now supports `orchestration=queue_batch|queue_dispatch|queue_drain`, with `queue_batch` acting as the default queue-native main path.

`GET /health` returns:

- database table status
- latest candle freshness
- latest pipeline activity, including multi-symbol pipeline summary counts when available
- broker/order protection status, including `reason_code`, `severity`, and `recommended_action` when protection is triggered
- scheduler stop flag and latest log line
- active runtime config values
- kill switch status
- active database backend info

`GET /admin` provides:

- health overview
- positions, orders, pnl, and scheduler log panels
- buttons for targeted pipeline runs, scheduler control, and kill switch control
- runtime strategy and symbol controls for split workers
- runtime execution backend control for `paper`, `noop`, `simulated_live`, and `binance`
- broker protection issue chips with direct actions for `switch_to_paper_backend`, `pause_scheduler`, `enable_kill_switch`, and `inspect_and_reconcile_orders`

`GET /audit-events` provides:

- recent structured audit records for pipeline, risk, scheduler, and kill switch actions
- broker-protection-triggered control actions are now distinguishable in audit payloads, for example `broker_protection:switch_to_paper_backend`, `broker_protection:pause_scheduler`, and `broker_protection:reconcile_orders`

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
- Split worker logs:
  - `logs/data-worker.log`
  - `logs/strategy-worker.log`
  - `logs/risk-worker.log`
  - `logs/execution-worker.log`

## Current Limitations

- SQLite only
- Multi-symbol runtime currently validated for `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`
- Single timeframe: `1m`
- Multi-strategy runtime currently validated for `ma_cross` and `momentum_3bar`
- Paper trading only
- Execution backend runtime control now supports `paper`, `noop`, `simulated_live`, and `binance`; `binance` can perform signed Spot account connectivity checks and live order routing when testnet credentials are configured
- A persistent `job_queue` abstraction now exists, and split worker scheduler modes can now dispatch to or drain from it through the full `market_data -> strategy -> risk -> execution` path

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
- execution backend changes and order reconciliation

## Telegram Alerts

Current Telegram alert triggers:

- scheduler stop flag set
- kill switch enabled
- health becomes degraded or error
- queue contains failed jobs
- broker protection becomes degraded
- split worker heartbeats become stale
- execution queue jobs fail

Manual test endpoint:

- `POST /alerts/test`

Deduplication:

- health alerts are sent once per unique degraded/error state
- queue failed-job alerts are sent once per unique failed queue state
- stale worker alerts are sent once per unique stale worker state
- execution failure alerts are sent once per unique failed execution job
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
