import os
import subprocess


LABEL = "com.alleyex.crypto.scheduler"


def main() -> None:
    uid = str(os.getuid())
    result = subprocess.run(
        ["launchctl", "print", f"gui/{uid}/{LABEL}"],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Launch agent not loaded: {LABEL}")
        if result.stderr.strip():
            print(result.stderr.strip())
        return

    print(result.stdout)


if __name__ == "__main__":
    main()
