from __future__ import annotations

import argparse
from pathlib import Path

from research_store import cli


def test_ingest_directory_orders_matches_and_reports_snapshots(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    source_directory = tmp_path / "All Canada" / "Text"
    source_directory.mkdir(parents=True)
    for name in ["HLY01_RCS_P2005", "ignore.txt", "HLY01_RCS_P2004"]:
        (source_directory / name).write_text(name, encoding="ascii")

    calls: list[Path] = []

    def fake_ingest(dataset_id, source, **kwargs):
        assert dataset_id == "eccc_hly01_observations"
        assert kwargs["publisher_vintage"] == "ECCC archive"
        calls.append(source)
        return f"snap_{source.name}"

    monkeypatch.setitem(cli.INGESTERS, "fixed_width_hourly", fake_ingest)
    args = argparse.Namespace(
        store=tmp_path / "store",
        dataset="eccc_hly01_observations",
        directory=source_directory,
        pattern="HLY01_RCS_P*",
        publisher_vintage="ECCC archive",
    )

    assert cli._ingest_directory(args) == 0
    assert [path.name for path in calls] == [
        "HLY01_RCS_P2004",
        "HLY01_RCS_P2005",
    ]
    captured = capsys.readouterr()
    assert "[1/2] ingesting HLY01_RCS_P2004" in captured.err
    assert "HLY01_RCS_P2005\tsnap_HLY01_RCS_P2005" in captured.out
