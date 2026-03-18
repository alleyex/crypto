import os
import plistlib
import subprocess
from pathlib import Path


LABEL = "com.alleyex.crypto.scheduler"
PROTECTED_FOLDERS = {"Desktop", "Documents", "Downloads"}


def run_command(command: list[str]) -> None:
    subprocess.run(command, check=False)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    python_bin = project_root / ".venv" / "bin" / "python"
    scheduler_script = project_root / "scripts" / "run_scheduler.py"
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"
    plist_path = launch_agents_dir / f"{LABEL}.plist"
    logs_dir = project_root / "logs"

    launch_agents_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    protected_match = next(
        (part for part in project_root.parts if part in PROTECTED_FOLDERS),
        None,
    )
    if protected_match is not None:
        raise SystemExit(
            "launchd install blocked: project is under a macOS protected folder "
            f"({protected_match}). Move the repo to a non-protected path such as "
            "~/Projects or ~/code before installing the LaunchAgent."
        )

    plist_data = {
        "Label": LABEL,
        "ProgramArguments": [
            str(python_bin),
            str(scheduler_script),
            "--interval",
            "60",
        ],
        "WorkingDirectory": str(project_root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(logs_dir / "launchd.stdout.log"),
        "StandardErrorPath": str(logs_dir / "launchd.stderr.log"),
    }

    with plist_path.open("wb") as file:
        plistlib.dump(plist_data, file)

    uid = str(os.getuid())
    run_command(["launchctl", "bootout", f"gui/{uid}", str(plist_path)])
    run_command(["launchctl", "bootstrap", f"gui/{uid}", str(plist_path)])
    run_command(["launchctl", "enable", f"gui/{uid}/{LABEL}"])

    print(f"Installed launch agent: {plist_path}")
    print(f"Label: {LABEL}")


if __name__ == "__main__":
    main()
