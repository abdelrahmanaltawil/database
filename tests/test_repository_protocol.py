from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "docs" / "ingestion-reports"
REQUIRED_HEADINGS = {
    "## Status",
    "## Scope and source inventory",
    "## Authoritative documentation",
    "## Metadata crosswalk",
    "## Execution identity",
    "## Snapshots",
    "## Reconciliation",
    "## Rejections and recovery",
    "## Quality flags and data-quality interpretation",
    "## Validation",
    "## Known limitations",
    "## Reproduction commands",
    "## Follow-up",
}


def test_ingestion_protocol_and_template_are_versioned() -> None:
    assert (ROOT / "docs" / "ingestion-protocol.md").is_file()
    assert (REPORTS / "README.md").is_file()


def test_dated_ingestion_reports_follow_repository_protocol() -> None:
    reports = sorted(REPORTS.glob("20??-??-??-*.md"))
    assert reports, "At least one production ingestion report is required"
    for report in reports:
        text = report.read_text()
        headings = {line for line in text.splitlines() if line.startswith("## ")}
        assert REQUIRED_HEADINGS <= headings, report
        assert "/Users/" not in text, report
        assert "ResearchDataStore/raw/objects/" not in text, report

