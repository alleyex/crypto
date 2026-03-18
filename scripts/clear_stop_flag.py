from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.scheduler.control import clear_stop_flag


def main() -> None:
    removed, stop_file = clear_stop_flag()
    if removed:
        print(f"Stop flag removed: {stop_file}")
        return

    print(f"Stop flag not found: {stop_file}")


if __name__ == "__main__":
    main()
