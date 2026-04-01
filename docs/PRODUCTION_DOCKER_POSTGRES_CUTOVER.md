# Production Docker + PostgreSQL Cutover

This runbook is for the **remote production host** currently running:

- `systemd + SQLite` for the live stack
- `Docker + PostgreSQL + paper` for staging

It assumes staging has already been validated on the same host.

## Current Intent

Move production from:

- `systemd` services
- SQLite database at `storage/market_data.db`

To:

- Docker Compose services
- PostgreSQL container `postgres`

This document is intentionally operational. Run it on the remote host unless
explicitly stated otherwise.

## Preconditions

Before touching production, confirm all of these are true:

1. Staging API is healthy enough and uses PostgreSQL:

```bash
curl -sS http://127.0.0.1:18000/health | jq '.database_info, .execution_backend, .checks.pipeline.status'
```

Expected:

- `backend = "postgres"`
- `execution_backend.backend = "paper"`
- `checks.pipeline.status = "ok"` or another known-good staging state

2. PostgreSQL container is healthy:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep crypto-postgres-1
docker exec crypto-postgres-1 pg_isready -U crypto -d crypto
```

3. The repo on the host is at the intended commit:

```bash
cd /root/crypto
git rev-parse --short HEAD
git status --short --branch
```

4. You have decided the production execution backend explicitly:

- `paper` for a safe dry cutover
- `binance` only if API keys and live execution are intentionally enabled

## Production Services To Stop

These are the existing SQLite-writing `systemd` services:

- `crypto-api.service`
- `crypto-scheduler.service`
- `crypto-futures-candles.service`
- `crypto-futures-orderbook.service`
- `crypto-futures-aggtrade.service`
- `crypto-futures-premium.service`
- `crypto-futures-open-interest.service`
- `crypto-futures-liquidation.service`

## Safety Backups

Create both backups before cutover:

```bash
mkdir -p /root/crypto-backups
cp /root/crypto/.env /root/crypto-backups/.env.pre-docker-cutover.$(date +%Y%m%d-%H%M%S)
cp /root/crypto/storage/market_data.db /root/crypto-backups/market_data.pre-docker-cutover.$(date +%Y%m%d-%H%M%S).db
```

## Recommended First Production Cut

For the first cut, keep production execution on `paper` unless there is a
specific reason to enable live orders immediately.

Use the checked-in production override:

- [/Users/alleyex/Projects/crypto/docker-compose.production.yml](/Users/alleyex/Projects/crypto/docker-compose.production.yml)

It pins production Docker services to:

- `CRYPTO_DB_BACKEND=postgres`
- `CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto`
- `CRYPTO_EXECUTION_BACKEND=paper`

## Cutover Sequence

### 1. Stop the existing `systemd` production services

```bash
systemctl stop crypto-api.service
systemctl stop crypto-scheduler.service
systemctl stop crypto-futures-candles.service
systemctl stop crypto-futures-orderbook.service
systemctl stop crypto-futures-aggtrade.service
systemctl stop crypto-futures-premium.service
systemctl stop crypto-futures-open-interest.service
systemctl stop crypto-futures-liquidation.service
```

Verify they are stopped:

```bash
systemctl is-active crypto-api.service crypto-scheduler.service \
  crypto-futures-candles.service crypto-futures-orderbook.service \
  crypto-futures-aggtrade.service crypto-futures-premium.service \
  crypto-futures-open-interest.service crypto-futures-liquidation.service
```

### 2. Run a final frozen SQLite -> PostgreSQL migration

Use the existing automation script:

```bash
cd /root/crypto
python3 scripts/freeze_sqlite_and_migrate_to_postgres.py \
  --sqlite-path /root/crypto/storage/market_data.db \
  --database-url postgresql://crypto:crypto@127.0.0.1:5432/crypto \
  --truncate
```

Do not continue if the final count verification reports mismatches.

### 3. Prepare production Docker runtime environment

No shell export is required for the normal `paper` cutover if you use the
production override file.

If you intentionally want live trading on the first cut, update
`docker-compose.production.yml` or pass explicit overrides for:

- `CRYPTO_EXECUTION_BACKEND=binance`
- `CRYPTO_BINANCE_API_KEY`
- `CRYPTO_BINANCE_API_SECRET`

### 4. Bring up production Docker services

For the first production cut, start the same collector set that staging uses:

```bash
cd /root/crypto
docker compose \
  -f docker-compose.yml \
  -f docker-compose.production.yml \
  --profile postgres \
  --profile futures-collectors \
  up -d postgres api futures-candles futures-orderbook futures-aggtrade futures-premium futures-open-interest futures-liquidation
```

If you also want the Docker scheduler active immediately:

```bash
cd /root/crypto
docker compose \
  -f docker-compose.yml \
  -f docker-compose.production.yml \
  --profile postgres \
  --profile futures-collectors \
  up -d scheduler
```

### 5. Verify production Docker health

```bash
docker compose ps
curl -sS http://127.0.0.1:8000/health | jq '.database_info, .execution_backend, .checks.candles, .checks.pipeline, .checks.queue'
```

Minimum expected result:

- `database_info.backend = "postgres"`
- `execution_backend.backend` matches your intended cutover mode
- API responds successfully
- required collectors show fresh heartbeats

### 6. Verify collector freshness

```bash
curl -sS http://127.0.0.1:8000/orderbook/futures/status | jq '.symbols[:2]'
curl -sS http://127.0.0.1:8000/aggtrades/futures/status | jq '.symbols[:2]'
curl -sS http://127.0.0.1:8000/premium/futures/status | jq '.symbols[:2]'
curl -sS http://127.0.0.1:8000/open-interest/futures/status | jq '.symbols[:2]'
curl -sS http://127.0.0.1:8000/liquidations/futures/status | jq '.'
curl -sS http://127.0.0.1:8000/candles/futures/collector/status | jq '.'
```

## Rollback

If production Docker fails verification:

1. Stop Docker production services

```bash
cd /root/crypto
docker compose -f docker-compose.yml -f docker-compose.production.yml down
```

2. Restore the old runtime by starting the `systemd` services again

```bash
systemctl start crypto-futures-liquidation.service
systemctl start crypto-futures-open-interest.service
systemctl start crypto-futures-premium.service
systemctl start crypto-futures-aggtrade.service
systemctl start crypto-futures-orderbook.service
systemctl start crypto-futures-candles.service
systemctl start crypto-scheduler.service
systemctl start crypto-api.service
```

3. Verify the old API:

```bash
curl -sS http://127.0.0.1:8000/health | jq '.database_info, .execution_backend'
```

## Notes

- Staging and production should not share the same runtime bind mounts.
- The first production cut should prioritize backend and collector stability over
  immediate live trading.
- If the host remains on a 2 GB droplet, avoid rebuilding images on the box
  during the actual cutover window.
- Use `docker compose -f docker-compose.yml -f docker-compose.production.yml ...`
  for future production rebuilds so services do not drift back to `sqlite` or
  `binance` due to host `.env` defaults.
