import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.settings import DEFAULT_STRATEGY_NAME
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
    parser.add_argument(
        "--strategy",
        default=DEFAULT_STRATEGY_NAME,
        help=f"Strategy name for pipeline/strategy-only runs. Default: {DEFAULT_STRATEGY_NAME}",
    )
    parser.add_argument(
        "--queue-dispatch",
        action="store_true",
        help="Enqueue split worker jobs instead of executing them directly.",
    )
    parser.add_argument(
        "--queue-drain",
        action="store_true",
        help="Drain queued split worker jobs instead of executing direct scheduler jobs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_scheduler(
        interval_seconds=args.interval,
        iterations=args.iterations,
        mode=args.mode,
        strategy_name=args.strategy,
        queue_dispatch=args.queue_dispatch,
        queue_drain=args.queue_drain,
    )


if __name__ == "__main__":
    main()
