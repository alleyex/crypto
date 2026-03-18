from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.scheduler.control import read_scheduler_log


def main() -> None:
    lines = read_scheduler_log(lines=200)
    if not lines:
        print("Log file not found or empty: logs/scheduler.log")
        return

    print("\n".join(lines))


if __name__ == "__main__":
    main()
