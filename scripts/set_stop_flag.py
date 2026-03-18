from pathlib import Path


STOP_FILE = Path("runtime") / "scheduler.stop"


def main() -> None:
    STOP_FILE.parent.mkdir(parents=True, exist_ok=True)
    STOP_FILE.write_text("stop\n", encoding="utf-8")
    print(f"Stop flag created: {STOP_FILE}")


if __name__ == "__main__":
    main()
