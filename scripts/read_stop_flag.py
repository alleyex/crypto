from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.scheduler.control import get_stop_status


def main() -> None:
    status = get_stop_status()
    if status["stopped"]:
        print(f"STOPPED: {status['stop_file']}")
        return

    print("RUNNING: no stop flag present")


if __name__ == "__main__":
    main()
