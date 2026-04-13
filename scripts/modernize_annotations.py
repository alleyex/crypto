"""Modernize typing annotations: Dict→dict, List→list, Optional[X]→X|None.

Usage:
    python scripts/modernize_annotations.py [--dry-run]
"""
import re
import sys
from pathlib import Path


def replace_optional(text: str) -> str:
    """Replace Optional[X] with X | None, handling nested brackets."""
    result = []
    i = 0
    while i < len(text):
        if text[i:i+9] == "Optional[":
            # Find matching close bracket
            depth = 0
            start = i + 9  # position after "Optional["
            j = start
            while j < len(text):
                if text[j] == "[":
                    depth += 1
                elif text[j] == "]":
                    if depth == 0:
                        break
                    depth -= 1
                j += 1
            inner = text[start:j]
            result.append(f"{inner} | None")
            i = j + 1  # skip past the closing ]
        else:
            result.append(text[i])
            i += 1
    return "".join(result)


def clean_typing_import(line: str) -> str | None:
    """Remove Dict, List, Optional, Tuple from a typing import line.
    Returns None if nothing from typing is imported anymore (remove the line).
    """
    # Match: from typing import X, Y, Z (possibly with parens / multi-line not handled)
    match = re.match(r"^(\s*from typing import\s+)(.+)$", line)
    if not match:
        return line

    prefix = match.group(1)
    imports_str = match.group(2)

    # Remove trailing comment if any
    comment = ""
    if "#" in imports_str:
        idx = imports_str.index("#")
        comment = "  " + imports_str[idx:]
        imports_str = imports_str[:idx].rstrip()

    # Remove surrounding parens if present
    imports_str = imports_str.strip().rstrip("\\").strip()
    if imports_str.startswith("(") and imports_str.endswith(")"):
        imports_str = imports_str[1:-1]

    names = [n.strip() for n in imports_str.split(",")]
    obsolete = {"Dict", "List", "Optional", "Tuple"}
    kept = [n for n in names if n and n not in obsolete]

    if not kept:
        return None  # remove entire line
    return prefix + ", ".join(kept) + comment


def process_file(path: Path, dry_run: bool = False) -> bool:
    original = path.read_text(encoding="utf-8")
    text = original

    # Step 1: replace Dict[, List[, Tuple[ (simple, safe)
    text = text.replace("Dict[", "dict[")
    text = text.replace("List[", "list[")
    text = text.replace("Tuple[", "tuple[")

    # Step 2: replace Optional[X] with X | None
    text = replace_optional(text)

    # Step 3: clean typing imports line by line
    lines = text.splitlines(keepends=True)
    new_lines = []
    for line in lines:
        if re.match(r"^\s*from typing import\s+", line):
            cleaned = clean_typing_import(line.rstrip("\n"))
            if cleaned is None:
                continue  # drop the line
            new_lines.append(cleaned + "\n")
        else:
            new_lines.append(line)
    text = "".join(new_lines)

    # Remove blank lines left by removed imports (at most one consecutive blank)
    text = re.sub(r"\n{3,}", "\n\n", text)

    if text == original:
        return False

    if not dry_run:
        path.write_text(text, encoding="utf-8")
    return True


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    root = Path(__file__).parent.parent / "app"
    changed = []
    for py_file in sorted(root.rglob("*.py")):
        if process_file(py_file, dry_run=dry_run):
            changed.append(py_file.relative_to(root.parent))

    action = "Would change" if dry_run else "Changed"
    print(f"{action} {len(changed)} file(s):")
    for f in changed:
        print(f"  {f}")


if __name__ == "__main__":
    main()
