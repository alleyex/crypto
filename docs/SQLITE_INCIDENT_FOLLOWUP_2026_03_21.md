# SQLite Incident Follow-Up

Date: March 21, 2026

## Incident Summary

During soak restart on March 21, 2026, the runtime hit SQLite corruption in `storage/market_data.db`.

Observed symptoms:

- queue-native pipeline failed at the `strategy` step with `database disk image is malformed`
- `PRAGMA integrity_check` failed
- corruption evidence included:
  - duplicate page references
  - broken `idx_job_queue_status_created_at`
  - `NULL value in job_queue.created_at`

Recovery completed by:

1. backing up the original DB
2. exporting recovery SQL via SQLite `.recover`
3. rebuilding a recovered DB
4. replacing `storage/market_data.db`
5. clearing corrupted `job_queue` rows

## Current Status

- recovered DB passes `PRAGMA integrity_check`
- core trading tables remain usable
- soak was restarted on `paper` backend
- queue-native four-step pipeline is running again

## Follow-Up Goals

1. reduce the chance of another SQLite corruption event
2. detect corruption earlier
3. make recovery operationally simpler
4. decide whether this incident changes PostgreSQL migration priority

## Priority Checklist

### P0: Detection And Recovery

- [ ] Add a reusable DB integrity check command
  Acceptance: one command prints `PRAGMA integrity_check` result and exits non-zero on failure.

- [ ] Add a documented SQLite recovery runbook
  Acceptance: exact backup, `.recover`, rebuild, verify, and swap steps are written as an operator procedure.

- [ ] Add a lightweight daily DB backup step
  Acceptance: the active SQLite DB is copied to a timestamped backup path without stopping the app.

### P1: Runtime Guardrails

- [ ] Add startup or periodic integrity verification
  Acceptance: scheduler or a dedicated check can detect corruption early and emit a degraded/failed alert before multiple queue batches pile up.

- [ ] Add explicit queue cleanup tooling for recovery scenarios
  Acceptance: there is a safe way to clear or rebuild `job_queue` without touching trading ledger tables.

- [x] Review SQLite durability settings — **done (2026-03-22)**
  `PRAGMA foreign_keys = ON` enabled in `app/core/db.py`. WAL mode and busy timeout remain as follow-up items (flagged in DB backlog).

### P2: Root Cause Investigation

- [x] Determine whether corruption was limited to `job_queue` or broader file damage — **done**
  Recovery confirmed corruption was isolated to `job_queue` index; core trading tables (orders, fills, positions, pnl) were intact.

- [ ] Audit recent process lifecycle around the incident
  Acceptance: reconstruct whether abrupt stops, multiple schedulers, or file replacement timing could have contributed.

- [x] Check whether any direct SQLite file operations bypass normal connection handling — **done**
  No unsafe file-level manipulation found in runtime paths.

### P3: Architecture Decision

- [x] Re-evaluate PostgreSQL migration priority after this incident — **done (2026-03-22)**
  SQLite remains the default runtime. PostgreSQL is validated and available as opt-in. WAL mode and busy timeout will be added to SQLite before further soak runs. See `docs/POSTGRES.md`.

- [ ] Decide whether queue state should remain in the main DB
  Acceptance: explicitly choose between keeping `job_queue` in the same SQLite file, isolating it, or moving it to another backend.

### Remaining DB Backlog (from 2026-03-22 review)

| Priority | Item |
|---|---|
| High | SQLite WAL mode (`PRAGMA journal_mode = WAL`) |
| High | SQLite busy timeout (`PRAGMA busy_timeout = 5000`) |
| Medium | CHECK constraints (qty/price must be positive) |
| Medium | Partial index on open orders |
| Medium | `GET /db/stats` endpoint (row counts + DB size) |
| Low | Auto VACUUM after retention runs |

## Recommended First Pass

These are the next three concrete items worth doing first:

1. Add `scripts/check_sqlite_integrity.py`
2. Add a documented recovery runbook under `docs/`
3. Add a simple timestamped backup script for `storage/market_data.db`

## Open Questions

- Was the corruption triggered by an unclean stop, concurrent access pattern, or pre-existing DB damage?
- Should `job_queue` be treated as disposable operational state and managed separately from trading history?
- Is this enough evidence to accelerate PostgreSQL for runtime, not just validation?
