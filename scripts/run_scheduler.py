import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.scheduler.runner import run_scheduler
from app.scheduler.runner import SCHEDULER_MODES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the trading pipeline on a fixed interval.")
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Interval in seconds between pipeline runs. Default: 60",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Optional number of runs before exit. Default: run forever",
    )
    parser.add_argument(
        "--mode",
        choices=SCHEDULER_MODES,
        default="pipeline",
        help="Scheduled job mode. Default: pipeline",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_scheduler(interval_seconds=args.interval, iterations=args.iterations, mode=args.mode)


if __name__ == "__main__":
    main()
