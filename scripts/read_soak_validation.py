import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.validation.soak_history import build_soak_history_summary
from app.validation.soak_history import record_soak_validation_snapshot
from app.validation.soak_report import build_soak_validation_report


def main() -> None:
    should_record = "--record" in sys.argv[1:]
    summary_only = "--summary" in sys.argv[1:]
    report = (
        build_soak_history_summary()
        if summary_only
        else record_soak_validation_snapshot() if should_record else build_soak_validation_report()
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
