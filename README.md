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

The source-specific registry entries are intentionally marked `provisional`
until representative files and publisher data dictionaries are available. The
store refuses to ingest a provisional dataset instead of guessing offsets,
units, sentinel meanings, time zones, or quality rules.
