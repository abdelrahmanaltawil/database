from __future__ import annotations

from pathlib import Path

import pytest

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
    args = cli.parser().parse_args(
        [
            "--store",
            str(tmp_path / "store"),
            "ingest-directory",
            "eccc_hly01_observations",
            str(source_directory),
            "--pattern",
            "HLY01_RCS_P*",
            "--publisher-vintage",
            "ECCC archive",
        ]
    )

    assert args.handler is cli._ingest_directory
    assert cli._ingest_directory(args) == 0
    assert [path.name for path in calls] == [
        "HLY01_RCS_P2004",
        "HLY01_RCS_P2005",
    ]
    captured = capsys.readouterr()
    assert "[1/2] ingesting HLY01_RCS_P2004" in captured.err
    assert "HLY01_RCS_P2005\tsnap_HLY01_RCS_P2005" in captured.out


def test_ingest_directory_passes_producer_options_and_refuses_misplaced_ones(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    source_directory = tmp_path / "UnitValueData"
    source_directory.mkdir()
    (source_directory / "02HA003_QR.csv").write_text("", encoding="ascii")
    metadata = tmp_path / "Hydat.sqlite3"

    received: list[dict] = []

    def fake_ingest(dataset_id, source, **kwargs):
        received.append(kwargs)
        return "snap_unit_values"

    monkeypatch.setitem(cli.INGESTERS, "unit_value_corrected", fake_ingest)
    args = cli.parser().parse_args(
        [
            "--store",
            str(tmp_path / "store"),
            "ingest-directory",
            "hydrometric_discharge_unit_corrected",
            str(source_directory),
            "--pattern",
            "*_QR.csv",
            "--station-metadata",
            str(metadata),
        ]
    )
    assert cli._ingest_directory(args) == 0
    assert received[0]["station_metadata"] == metadata
    assert received[0]["max_drainage_area_km2"] is None

    misplaced = cli.parser().parse_args(
        [
            "--store",
            str(tmp_path / "store"),
            "ingest-directory",
            "eccc_hly01_observations",
            str(source_directory),
            "--pattern",
            "*",
            "--station-metadata",
            str(metadata),
        ]
    )
    with pytest.raises(ValueError, match="--station-metadata"):
        cli._ingest_directory(misplaced)
