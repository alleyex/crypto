from pathlib import Path


STOP_FILE = Path("runtime") / "scheduler.stop"


def main() -> None:
    if STOP_FILE.exists():
        print(f"STOPPED: {STOP_FILE}")
        return

    print("RUNNING: no stop flag present")


if __name__ == "__main__":
    main()
