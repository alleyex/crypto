import subprocess
import sys
from pathlib import Path


SCRIPTS = [
    "save_klines_sqlite.py",
    "generate_signal_sqlite.py",
    "evaluate_risk_sqlite.py",
    "paper_execute_sqlite.py",
    "update_positions_sqlite.py",
    "update_pnl_sqlite.py",
]


def main() -> None:
    scripts_dir = Path(__file__).resolve().parent

    for script_name in SCRIPTS:
        script_path = scripts_dir / script_name
        print(f"\n=== Running {script_name} ===")
        subprocess.run([sys.executable, str(script_path)], check=True)

    print("\nPipeline completed.")


if __name__ == "__main__":
    main()
