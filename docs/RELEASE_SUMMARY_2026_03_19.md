# Release Summary

Date: March 19, 2026

This update delivers the initial PostgreSQL runtime path, centralizes schema bootstrapping, fixes noisy health alert deduplication, and restructures GitHub Actions into reusable workflows with standardized artifacts.

## Highlights

- Added explicit application migrations and shared migration entrypoints for API, scheduler, and pipeline startup
- Added PostgreSQL smoke validation, Compose validation, and runtime validation documentation
- Introduced backend-aware DB helpers for schema inspection, inserts, and timestamp parsing
- Enabled PostgreSQL-backed pipeline, API, and scheduler runtime validation
- Fixed repeated Telegram health alerts caused by volatile heartbeat fields affecting alert deduplication
- Standardized CI and PostgreSQL validation artifacts with summaries, manifests, checksums, and reusable artifact tooling
- Refactored GitHub Actions into local composite actions and reusable workflows for setup, validation, upload, and summary rendering

## Main Technical Changes

### Database and Runtime

- Added `app/core/migrations.py` to centralize schema creation and migration execution
- Added `app/core/postgres_smoke.py` and standalone PostgreSQL smoke scripts
- Extended `app/core/db.py` with backend-aware introspection, PostgreSQL connection handling, placeholder rewriting, and `RETURNING id` support
- Updated trading and query paths to reduce SQLite-only assumptions
- Added PostgreSQL support to Docker Compose and documented runtime validation

### Reliability Fixes

- Updated health alert fingerprinting so volatile heartbeat timestamps do not trigger repeated Telegram alerts
- Added PostgreSQL startup retry configuration to tolerate database boot lag during API and scheduler startup
- Fixed date-sensitive tests so daily-loss checks do not break across calendar rollovers

### CI and Workflow Structure

- Split general CI and PostgreSQL validation into separate top-level workflows
- Added workflow dispatch options for manual PostgreSQL validation scope, Python version, DSN, API port, and artifact retention
- Added reusable workflows for CI jobs, PostgreSQL validation jobs, and summary jobs
- Added shared local actions for Python setup, artifact upload, PostgreSQL validation runs, test runs, and workflow summary rendering
- Standardized artifact metadata with manifest files, checksums, validation layer labels, and human-readable summaries

## Validation

- Local test suite: `73 passed, 1 warning`
- PostgreSQL runtime validated across smoke, single-process runtime, API endpoints, scheduler execution, and Docker Compose runtime

## Suggested Follow-Up

1. Decide whether PostgreSQL should remain optional or become the default runtime.
2. Continue removing residual SQLite-specific typing and assumptions from service code.
3. Keep the reusable workflow structure stable unless future automation needs justify more abstraction.
