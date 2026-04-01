#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.settings import DATABASE_URL
from app.core.settings import SQLITE_PATH


DEFAULT_SERVICES = [
    "crypto-api.service",
    "crypto-scheduler.service",
    "crypto-futures-orderbook.service",
    "crypto-futures-aggtrade.service",
    "crypto-futures-premium.service",
    "crypto-futures-open-interest.service",
    "crypto-futures-liquidation.service",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stop SQLite-writing services, run freeze-and-migrate into PostgreSQL, and optionally restart."
    )
    parser.add_argument("--sqlite-path", default=str(SQLITE_PATH), help="Path to source SQLite database.")
    parser.add_argument(
        "--database-url",
        default=DATABASE_URL,
        help="Target PostgreSQL DSN. Defaults to CRYPTO_DATABASE_URL.",
    )
    parser.add_argument(
        "--services",
        default=",".join(DEFAULT_SERVICES),
        help="Comma-separated systemd services to stop before cutover.",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per batch insert. Default: 1000")
    parser.add_argument(
        "--skip-stop",
        action="store_true",
        help="Do not stop services before migration.",
    )
    parser.add_argument(
        "--restart-services",
        action="store_true",
        help="Restart the listed services after a successful migration.",
    )
    parser.add_argument(
        "--set-postgres-env",
        action="store_true",
        help="Update the .env file to use PostgreSQL before restarting services.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the environment file to update when --set-postgres-env is used.",
    )
    parser.add_argument(
        "--keep-snapshot",
        action="store_true",
        help="Keep the generated SQLite snapshot file.",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Optional comma-separated subset of tables to migrate and verify.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned actions without executing them.",
    )
    return parser.parse_args()


def _run(command: list[str], *, dry_run: bool = False) -> None:
    print("$ " + " ".join(command))
    if dry_run:
        return
    subprocess.run(command, check=True)


def _update_env_file(env_file: Path, database_url: str, *, dry_run: bool = False) -> None:
    if not env_file.exists():
        if dry_run:
            print(f"update_env_file: {env_file} (missing in dry-run preview)")
            return
        raise RuntimeError(f"Environment file not found: {env_file}")

    lines = env_file.read_text(encoding="utf-8").splitlines()
    updated: list[str] = []
    seen_backend = False
    seen_url = False

    for line in lines:
        if line.startswith("CRYPTO_DB_BACKEND="):
            updated.append("CRYPTO_DB_BACKEND=postgres")
            seen_backend = True
        elif line.startswith("CRYPTO_DATABASE_URL="):
            updated.append(f"CRYPTO_DATABASE_URL={database_url}")
            seen_url = True
        else:
            updated.append(line)

    if not seen_backend:
        updated.append("CRYPTO_DB_BACKEND=postgres")
    if not seen_url:
        updated.append(f"CRYPTO_DATABASE_URL={database_url}")

    print(f"update_env_file: {env_file}")
    if not dry_run:
        env_file.write_text("\n".join(updated) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if not args.database_url.strip():
        raise RuntimeError("Provide --database-url or set CRYPTO_DATABASE_URL.")

    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        raise RuntimeError(f"SQLite database not found: {sqlite_path}")

    services = [item.strip() for item in args.services.split(",") if item.strip()]
    env_file = Path(args.env_file).expanduser().resolve()
    freeze_script = PROJECT_ROOT / "scripts" / "freeze_sqlite_and_migrate_to_postgres.py"
    if not freeze_script.exists():
        raise RuntimeError(f"Freeze script not found: {freeze_script}")

    python_bin = shutil.which("python3") or sys.executable
    freeze_command = [
        python_bin,
        str(freeze_script),
        "--sqlite-path",
        str(sqlite_path),
        "--database-url",
        args.database_url,
        "--batch-size",
        str(args.batch_size),
        "--truncate",
    ]
    if args.tables.strip():
        freeze_command.extend(["--tables", args.tables])
    if args.keep_snapshot:
        freeze_command.append("--keep-snapshot")

    print("cutover_plan:")
    print(f"  sqlite_path: {sqlite_path}")
    print(f"  database_url: {args.database_url}")
    print(f"  services: {', '.join(services) if services else '(none)'}")
    print(f"  restart_services: {args.restart_services}")
    print(f"  set_postgres_env: {args.set_postgres_env}")
    print(f"  dry_run: {args.dry_run}")

    if not args.skip_stop:
        for service in services:
            _run(["systemctl", "stop", service], dry_run=args.dry_run)

    try:
        _run(freeze_command, dry_run=args.dry_run)
        if args.set_postgres_env:
            _update_env_file(env_file, args.database_url, dry_run=args.dry_run)
        if args.restart_services:
            for service in reversed(services):
                _run(["systemctl", "start", service], dry_run=args.dry_run)
    except Exception:
        if not args.skip_stop and args.restart_services:
            print("cutover_failed: attempting to restart stopped services")
            for service in reversed(services):
                try:
                    _run(["systemctl", "start", service], dry_run=args.dry_run)
                except Exception as restart_error:
                    print(f"restart_failed: {service}: {restart_error}")
        raise


if __name__ == "__main__":
    main()
