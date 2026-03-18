import os
import subprocess
from pathlib import Path


LABEL = "com.alleyex.crypto.scheduler"


def main() -> None:
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"
    uid = str(os.getuid())

    subprocess.run(
        ["launchctl", "bootout", f"gui/{uid}", str(plist_path)],
        check=False,
    )

    if plist_path.exists():
        plist_path.unlink()
        print(f"Removed launch agent: {plist_path}")
    else:
        print(f"Launch agent not found: {plist_path}")


if __name__ == "__main__":
    main()
