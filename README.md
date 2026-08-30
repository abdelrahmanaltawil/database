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

The repository contains code only. Data lives below the directory selected by
`RESEARCH_DATA_ROOT`; see [the runbook](docs/runbook.md) for setup and ingestion
commands.

The ECCC fixed-width layouts, per-element units and timing, and station workbook
are source-configured. Station coordinates are resolved to IANA timezones and
the national archive's local-standard-time timestamps are converted to UTC
without applying daylight-saving shifts.
Licensed SCADA mappings live in a private JSON overlay outside Git. The store
refuses to ingest a provisional dataset instead of guessing time zones, units,
missing markers, or confidential column meanings.
