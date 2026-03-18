from pathlib import Path


LOG_FILE = Path("logs") / "scheduler.log"


def main() -> None:
    if not LOG_FILE.exists():
        print(f"Log file not found: {LOG_FILE}")
        return

    print(LOG_FILE.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
