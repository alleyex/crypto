import argparse
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.artifact_utils import build_file_entry
from scripts.artifact_utils import write_json_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate summary.md and manifest.json for CI test artifacts."
    )
    parser.add_argument(
        "--junit-xml",
        required=True,
        help="Path to the pytest JUnit XML report.",
    )
    parser.add_argument(
        "--artifact-dir",
        required=True,
        help="Directory where summary.md and manifest.json should be written.",
    )
    parser.add_argument(
        "--event-name",
        default=os.getenv("GITHUB_EVENT_NAME", "local"),
        help="Source event name. Defaults to GITHUB_EVENT_NAME or local.",
    )
    parser.add_argument(
        "--run-id",
        default=os.getenv("GITHUB_RUN_ID", "local"),
        help="Workflow run id. Defaults to GITHUB_RUN_ID or local.",
    )
    parser.add_argument(
        "--write-step-summary",
        action="store_true",
        help="Also write the generated summary to GITHUB_STEP_SUMMARY when available.",
    )
    return parser.parse_args()


def read_junit_counts(junit_xml_path: Path) -> dict[str, int]:
    if not junit_xml_path.exists():
        return {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    root = ET.parse(junit_xml_path).getroot()
    return {
        "tests": int(root.attrib.get("tests", 0)),
        "failures": int(root.attrib.get("failures", 0)),
        "errors": int(root.attrib.get("errors", 0)),
        "skipped": int(root.attrib.get("skipped", 0)),
    }


def get_outcome(counts: dict[str, int]) -> str:
    return "passed" if counts["failures"] == 0 and counts["errors"] == 0 else "failed"


def build_test_summary(
    counts: dict[str, int],
    *,
    event_name: str,
    run_id: str,
    generated_at: str,
) -> str:
    outcome = get_outcome(counts)
    lines = [
        "# Test Results",
        "",
        f"- outcome: `{outcome}`",
        f"- event_name: `{event_name}`",
        f"- run_id: `{run_id}`",
        f"- generated_at: `{generated_at}`",
        "- validation_layer: `test`",
        "- verdict: `test-check`",
        f"- tests: `{counts['tests']}`",
        f"- failures: `{counts['failures']}`",
        f"- errors: `{counts['errors']}`",
        f"- skipped: `{counts['skipped']}`",
        "- files: `summary.md`, `junit.xml`, `manifest.json`",
        "",
    ]
    return "\n".join(lines)

def build_test_artifact_manifest(
    *,
    artifact_dir: Path,
    junit_xml_path: Path,
    summary_path: Path,
    event_name: str,
    run_id: str,
    generated_at: str,
) -> dict[str, object]:
    counts = read_junit_counts(junit_xml_path)
    return {
        "artifact_kind": "test-results-artifact",
        "validation_layer": "test",
        "verdict": "test-check",
        "outcome": get_outcome(counts),
        "event_name": event_name,
        "run_id": run_id,
        "generated_at": generated_at,
        "files": [
            build_file_entry(summary_path, artifact_dir, "Human-readable test summary."),
            build_file_entry(junit_xml_path, artifact_dir, "Pytest JUnit XML report."),
        ],
    }


def write_test_artifact(
    *,
    artifact_dir: Path,
    junit_xml_path: Path,
    event_name: str,
    run_id: str,
    write_step_summary: bool,
) -> tuple[Path, Path]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    counts = read_junit_counts(junit_xml_path)
    summary_text = build_test_summary(
        counts,
        event_name=event_name,
        run_id=run_id,
        generated_at=generated_at,
    )
    summary_path = artifact_dir / "summary.md"
    summary_path.write_text(summary_text, encoding="utf-8")

    if write_step_summary:
        step_summary = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
        if step_summary:
            Path(step_summary).write_text(summary_text, encoding="utf-8")

    manifest = build_test_artifact_manifest(
        artifact_dir=artifact_dir,
        junit_xml_path=junit_xml_path,
        summary_path=summary_path,
        event_name=event_name,
        run_id=run_id,
        generated_at=generated_at,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_json_file(manifest_path, manifest)
    manifest["files"].append(
        build_file_entry(manifest_path, artifact_dir, "Artifact manifest for test results.")
    )
    write_json_file(manifest_path, manifest)
    return summary_path, manifest_path


def main() -> int:
    args = parse_args()
    write_test_artifact(
        artifact_dir=Path(args.artifact_dir),
        junit_xml_path=Path(args.junit_xml),
        event_name=args.event_name,
        run_id=args.run_id,
        write_step_summary=args.write_step_summary,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
