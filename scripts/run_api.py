#!/usr/bin/env python3

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"


def _ensure_project_venv_python() -> None:
    if not EXPECTED_PYTHON.exists():
        return

    current_python = Path(sys.executable).resolve()
    expected_python = EXPECTED_PYTHON.resolve()
    if current_python == expected_python:
        return

    os.execv(str(expected_python), [str(expected_python), __file__])


_ensure_project_venv_python()

sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn


if __name__ == "__main__":
    uvicorn.run("app.api.main:app", host="127.0.0.1", port=8000, reload=False)
