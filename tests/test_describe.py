"""The read-side schema API: research_store.describe and `research-store describe`."""

from __future__ import annotations

import json

import pytest

import research_store
from research_store import describe
from research_store.cli import main
from research_store.foundation.models import (
    DatasetKind,
    DatasetReadiness,
    DatasetSpec,
    StorageModel,
    TemporalKind,
)
from research_store.foundation.paths import STORE_ENV
from research_store.foundation.registry import DEFAULT_REGISTRY


@pytest.fixture
def provisional_spec() -> DatasetSpec:
    return DatasetSpec(
        dataset_id="test_provisional",
        description="Declared but not yet usable",
        kind=DatasetKind.EXTERNAL,
        producer="synthetic",
        storage_model=StorageModel.WIDE,
        temporal_kind=TemporalKind.INSTANT,
        readiness=DatasetReadiness.PROVISIONAL,
        unresolved_decisions=("publisher has not confirmed the discharge unit",),
    )


# --- the function ---------------------------------------------------------


def test_describe_returns_the_registry_declaration(registry, wide_spec) -> None:
    assert describe("test_sensor", registry=registry) is wide_spec


def test_unknown_dataset_names_the_known_ones(registry) -> None:
    with pytest.raises(KeyError) as error:
        describe("test_nonexistent", registry=registry)
    message = str(error.value)
    assert "test_nonexistent" in message
    assert "test_sensor" in message and "test_long" in message


def test_describe_reads_no_store(monkeypatch, tmp_path) -> None:
    """Pure: it must answer with no store on disk, so config can be checked first."""

    monkeypatch.setenv(STORE_ENV, str(tmp_path / "does-not-exist"))
    assert describe("eccc_hly01_observations").dataset_id == "eccc_hly01_observations"


def test_provisional_datasets_still_describe(provisional_spec) -> None:
    """load() refuses provisional data; describe must still say why it is."""

    from research_store.foundation.models import Registry

    spec = describe("test_provisional", registry=Registry([provisional_spec]))
    assert spec.readiness is DatasetReadiness.PROVISIONAL
    assert spec.unresolved_decisions
    with pytest.raises(RuntimeError):
        spec.require_ready()


def test_every_declared_dataset_describes() -> None:
    for spec in DEFAULT_REGISTRY:
        assert describe(spec.dataset_id) is spec


# --- the public surface ---------------------------------------------------


def test_describe_and_the_spec_vocabulary_are_public() -> None:
    """A consumer types against the declaration without importing foundation."""

    for name in ("describe", "load", "connect", "DatasetSpec", "VariableSpec"):
        assert name in research_store.__all__
        assert hasattr(research_store, name)
    assert research_store.describe is describe


# --- what the copula workflow relies on -----------------------------------


def test_rainfall_units_and_quality_binding_come_from_the_store() -> None:
    """These are the facts a consumer must not re-declare in its own config."""

    variable = describe("eccc_hly01_observations").variable("precipitation_amount_1h")
    assert variable.unit == "mm"
    assert variable.quality_field == "precipitation_amount_1h_quality"

    discharge = describe("hydrometric_discharge_unit_corrected").variable("discharge")
    assert discharge.unit == "m3/s"


# --- the CLI --------------------------------------------------------------


def test_cli_describe_prints_the_schema(capsys) -> None:
    assert main(["describe", "eccc_hly01_observations"]) == 0
    out = capsys.readouterr().out
    assert "eccc_hly01_observations" in out
    assert "precipitation_amount_1h" in out
    assert "quality=precipitation_amount_1h_quality" in out
    assert "mm" in out
    assert "semantics" in out


def test_cli_describe_renders_a_reference_dataset(capsys) -> None:
    """Reference datasets have no time axis; the formatter must not invent one."""

    assert main(["describe", "eccc_station_inventory"]) == 0
    out = capsys.readouterr().out
    assert "station_name" in out
    assert "\n  time\n" not in out


def test_cli_describe_json_is_the_serializable_spec(capsys) -> None:
    assert main(["describe", "eccc_hly01_observations", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == json.loads(
        json.dumps(describe("eccc_hly01_observations").serializable(), default=str)
    )


def test_cli_verbose_adds_the_source_documentation(capsys) -> None:
    assert main(["describe", "eccc_hly03_observations"]) == 0
    terse = capsys.readouterr().out
    assert main(["describe", "eccc_hly03_observations", "--verbose"]) == 0
    verbose = capsys.readouterr().out
    assert "--verbose to print" in terse
    assert len(verbose) > len(terse)
    assert "Environment and Climate Change Canada" in verbose


def test_cli_describe_shows_text_variables_annotations_and_index_units(capsys) -> None:
    assert main(["describe", "eccc_climate_hourly_observations"]) == 0
    out = capsys.readouterr().out
    assert "weather_description" in out and "string" in out
    assert "annotations" in out and "source_station_id" in out
    humidex = next(line for line in out.splitlines() if line.strip().startswith("humidex "))
    assert "  1  " in humidex
    assert "'' " in out, "a blank sentinel marker must be visible"
    assert "not_applicable -> null  (only wind_direction)" in out
    assert "(only weather_description)" in out
    assert "replace" in out


def test_cli_unknown_dataset_fails_cleanly(capsys) -> None:
    assert main(["describe", "no_such_dataset"]) == 1
    assert "no_such_dataset" in capsys.readouterr().err
