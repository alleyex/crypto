#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
PRODUCTION_COMPOSE_FILE = PROJECT_ROOT / "docker-compose.production.yml"

DEFAULT_SERVICES = [
    "postgres",
    "api",
    "scheduler",
    "futures-candles",
    "futures-orderbook",
    "futures-aggtrade",
    "futures-premium",
    "futures-open-interest",
    "futures-liquidation",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start or restart the production Docker Compose runtime with the checked-in PostgreSQL + paper override."
    )
    parser.add_argument(
        "--services",
        default=",".join(DEFAULT_SERVICES),
        help="Comma-separated service subset to bring up. Defaults to the full production stack.",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Rebuild images before starting services.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the docker compose command without executing it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    services = [item.strip() for item in args.services.split(",") if item.strip()]
    if not services:
        raise SystemExit("At least one service must be provided.")

    command = [
        "docker",
        "compose",
        "-f",
        str(BASE_COMPOSE_FILE),
        "-f",
        str(PRODUCTION_COMPOSE_FILE),
        "--profile",
        "postgres",
        "--profile",
        "futures-collectors",
        "up",
        "-d",
    ]
    if args.build:
        command.append("--build")
    command.extend(services)

    print("$ " + " ".join(command))
    if args.dry_run:
        return

    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


if __name__ == "__main__":
    main()
