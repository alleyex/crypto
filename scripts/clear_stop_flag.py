from pathlib import Path


STOP_FILE = Path("runtime") / "scheduler.stop"


def main() -> None:
    if STOP_FILE.exists():
        STOP_FILE.unlink()
        print(f"Stop flag removed: {STOP_FILE}")
        return

    print(f"Stop flag not found: {STOP_FILE}")


if __name__ == "__main__":
    main()
