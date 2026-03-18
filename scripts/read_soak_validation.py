import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.validation.soak_report import build_soak_validation_report


def main() -> None:
    print(json.dumps(build_soak_validation_report(), indent=2))


if __name__ == "__main__":
    main()
