#!/usr/bin/env python3

import shutil
import subprocess
from pathlib import Path


SYSTEMD_DIR = Path("/etc/systemd/system")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICE_DIR = PROJECT_ROOT / "deploy" / "systemd"
SERVICE_NAMES = [
    "crypto-api.service",
    "crypto-scheduler.service",
    "crypto-futures-orderbook.service",
    "crypto-futures-aggtrade.service",
    "crypto-futures-premium.service",
    "crypto-futures-open-interest.service",
    "crypto-futures-liquidation.service",
]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def main() -> None:
    if not SERVICE_DIR.exists():
        raise SystemExit(f"Missing service directory: {SERVICE_DIR}")

    for name in SERVICE_NAMES:
        src = SERVICE_DIR / name
        dst = SYSTEMD_DIR / name
        if not src.exists():
            raise SystemExit(f"Missing service template: {src}")
        shutil.copy2(src, dst)
        print(f"Installed {dst}")

    run(["systemctl", "daemon-reload"])
    run(["systemctl", "enable", *SERVICE_NAMES])
    run(["systemctl", "restart", *SERVICE_NAMES])
    run(["systemctl", "--no-pager", "--full", "status", *SERVICE_NAMES])


if __name__ == "__main__":
    main()
