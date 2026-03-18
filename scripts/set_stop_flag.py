from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.scheduler.control import set_stop_flag


def main() -> None:
    stop_file = set_stop_flag()
    print(f"Stop flag created: {stop_file}")


if __name__ == "__main__":
    main()
