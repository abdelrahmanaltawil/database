# Research Data Store

A local, provenance-first store for multi-domain PhD research data. Raw source
files remain immutable, curated tables are rebuildable Parquet, and DuckDB
provides the catalogue and SQL entry point.

```python
from research_store import load

rain = load(
    "eccc_hly01_observations",
    entity="0100001",
    variable="precipitation_amount_1h",
    start="2015",
    end="2020",
)
```

Research data defaults to the Git-ignored `ResearchDataStore/` directory in this
repository, so it remains local and cannot be committed. `RESEARCH_DATA_ROOT`
can still select another local disk; see [the runbook](docs/runbook.md) for setup
and ingestion commands.

Production loads follow the [ingestion protocol](docs/ingestion-protocol.md):
metadata is checked against authoritative documentation, every supplied record
is reconciled, and a versioned [ingestion report](docs/ingestion-reports/README.md)
is required before a batch is called complete.

The ECCC fixed-width layouts, per-element units and timing, and station workbook
are source-configured. Station coordinates are resolved to IANA timezones and
the national archive's local-standard-time timestamps are converted to UTC
without applying daylight-saving shifts.
Corrected hydrometric unit-value collections are streamed directly from their
compressed publisher files. Optional station selection, such as a gross
drainage-area ceiling, belongs to an ingestion run rather than the dataset name,
and AQUARIUS approval, grade and qualifier annotations remain queryable.
Licensed SCADA mappings live in a private JSON overlay outside Git. The store
refuses to ingest a provisional dataset instead of guessing time zones, units,
missing markers, or confidential column meanings.

Refusing to guess is not enough on its own: a declaration can be confidently
wrong. A variable may therefore declare a plausibility band, and a publication
whose distribution contradicts its declared unit is refused rather than
published. `research-store doctor` re-hashes published fragments against the
digests the catalogue recorded for them, and `research-store gc` reclaims only
what no snapshot references. Fragments are catalogued by a path relative to the
store root, so the store can be moved or restored elsewhere and still read.
