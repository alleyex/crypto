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

- [ ] Review SQLite durability settings
  Acceptance: document current `journal_mode`, `synchronous`, and any WAL usage; decide whether settings should be changed.

### P2: Root Cause Investigation

- [ ] Determine whether corruption was limited to `job_queue` or broader file damage
  Acceptance: compare recovered counts, indexes, and recent writes across trading tables and queue tables.

- [ ] Audit recent process lifecycle around the incident
  Acceptance: reconstruct whether abrupt stops, multiple schedulers, or file replacement timing could have contributed.

- [ ] Check whether any direct SQLite file operations bypass normal connection handling
  Acceptance: search code/scripts for unsafe file-level manipulation of the active DB file and confirm none are used in runtime paths.

### P3: Architecture Decision

- [ ] Re-evaluate PostgreSQL migration priority after this incident
  Acceptance: record whether SQLite remains acceptable for soak/runtime or whether PostgreSQL should move up in priority.

- [ ] Decide whether queue state should remain in the main DB
  Acceptance: explicitly choose between keeping `job_queue` in the same SQLite file, isolating it, or moving it to another backend.

## Recommended First Pass

These are the next three concrete items worth doing first:

1. Add `scripts/check_sqlite_integrity.py`
2. Add a documented recovery runbook under `docs/`
3. Add a simple timestamped backup script for `storage/market_data.db`

## Open Questions

- Was the corruption triggered by an unclean stop, concurrent access pattern, or pre-existing DB damage?
- Should `job_queue` be treated as disposable operational state and managed separately from trading history?
- Is this enough evidence to accelerate PostgreSQL for runtime, not just validation?
