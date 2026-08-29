# Research Data Store

A local, provenance-first store for multi-domain PhD research data. Raw source
files remain immutable, curated tables are rebuildable Parquet, and DuckDB
provides the catalogue and SQL entry point.

```python
from research_store import load

rain = load(
    "weather_family_a",
    entity="0100001",
    variable="precipitation_amount",
    start="2015",
    end="2020",
)
```

The repository contains code only. Data lives below the directory selected by
`RESEARCH_DATA_ROOT`; see [the runbook](docs/runbook.md) for setup and ingestion
commands.

The ECCC fixed-width layouts and station workbook are source-configured. The
station inventory is ready to ingest; the national hourly series remain
`provisional` until station-specific local-standard-time handling is declared.
Licensed SCADA mappings live in a private JSON overlay outside Git. The store
refuses to ingest a provisional dataset instead of guessing time zones, units,
missing markers, or confidential column meanings.
