import hashlib
import json
from pathlib import Path
from typing import Any


def write_optional_output(path_text: str, content: str) -> None:
    if not path_text:
        return
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_file_entry(path: Path, artifact_root: Path, purpose: str) -> dict[str, str]:
    content = path.read_bytes()
    return {
        "path": str(path.relative_to(artifact_root)),
        "purpose": purpose,
        "size_bytes": str(len(content)),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def build_manifest_files(
    *,
    artifact_root: Path,
    file_purposes: dict[str, str],
) -> list[dict[str, str]]:
    files: list[dict[str, str]] = []
    for relative_path, purpose in file_purposes.items():
        path = artifact_root / relative_path
        if not path.exists():
            continue
        files.append(build_file_entry(path, artifact_root, purpose))
    return files


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
