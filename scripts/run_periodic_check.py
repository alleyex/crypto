import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "bin" / "python"


CHECKS = [
    ("soak_validation", [str(PYTHON), "scripts/read_soak_validation.py"]),
    ("soak_summary", [str(PYTHON), "scripts/read_soak_validation.py", "--summary"]),
    ("broker_protection", [str(PYTHON), "scripts/analyze_broker_protection.py"]),
    ("scheduler_tail", ["tail", "-n", "20", "logs/scheduler.log"]),
]


def main() -> None:
    failed = False
    for name, command in CHECKS:
        print(f"=== {name} ===")
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.stdout:
            print(completed.stdout.rstrip())
        if completed.stderr:
            print(completed.stderr.rstrip(), file=sys.stderr)
        if completed.returncode != 0:
            failed = True
            print(f"[exit_code={completed.returncode}] {name} failed", file=sys.stderr)
        print("")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
