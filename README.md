# ECCC Research Data Store

A local, provenance-first store for Environment and Climate Change Canada
(ECCC) station metadata and fixed-width hourly observations. Raw publisher files
remain immutable, curated tables are rebuildable Parquet, and DuckDB provides
the catalogue and SQL entry point.

The active catalogue contains only:

| Dataset | Purpose |
|---|---|
| `eccc_station_inventory` | Relational station metadata and inferred IANA timezone |
| `eccc_hly01_observations` | HLY01 RCS elements 262-280 |
| `eccc_hly03_observations` | HLY03 rainfall element 123 |

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
`RESEARCH_DATA_ROOT`; see [the runbook](docs/runbook.md) for setup, bulk
ingestion, verification, and restart commands.

The ECCC fixed-width layouts, per-element units and interval placement are
source-configured. Station coordinates resolve to pinned IANA timezone data.
The archive's local-standard-time values are converted to UTC using explicit
non-DST transition types, so daylight-saving transitions do not shorten,
duplicate, or reverse source intervals.
