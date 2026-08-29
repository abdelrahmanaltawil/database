# Runbook

These commands go from an empty machine directory to an initialized and tested
store. They do not put research data in the Git repository.

## 1. Install

```bash
git clone https://github.com/abdelrahmanaltawil/database.git
cd database
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Install NetCDF support only on machines that ingest reanalysis:

```bash
python -m pip install -e '.[netcdf]'
```

## 2. Select exactly one store root

Choose a local, non-cloud-synchronized disk with adequate capacity:

```bash
export RESEARCH_DATA_ROOT=/absolute/path/to/research-data
```

Put that export in the shell profile used by every analysis repository. Do not
set repository-relative paths.

## 3. Initialize and verify

```bash
research-store init
research-store doctor
research-store datasets
python -m pytest
```

The station inventory reports `ready`. Sources with unresolved time, unit or
licensed-schema decisions report `provisional`; this is an intentional stop
condition, not an installation failure.

## 4. Ingest the station relationship table

The supplied ECCC workbook has three disclaimer rows followed by the real
header. The ingester detects that header and preserves numeric and alphanumeric
Climate IDs as strings:

```bash
research-store ingest station_inventory \
  '/absolute/path/to/Station Inventory EN.xlsx' \
  --publisher-vintage '2025-01-02 snapshot'
```

Climate observations use the same `entity_id`, so this workbook is the direct
relational station lookup.

## 5. Resolve public and licensed source declarations

Public, non-sensitive declarations are edited in:

```text
src/research_store/foundation/registry.py
```

Licensed mappings must not be written there. Copy the placeholder structure:

```bash
cp config/private_registry.example.json /absolute/private/path/registry.json
export RESEARCH_STORE_PRIVATE_REGISTRY=/absolute/private/path/registry.json
```

Keep that file outside Git and restrict its permissions. Record the evidence
while resolving:

- exact field names or fixed-width offsets and scale;
- every element/column to canonical variable, quantity, unit and float64 type;
- source timezone, interval duration and whether timestamps label starts/ends;
- quality codes and exact era-aware sentinel rules;
- coordinate reference system and longitude convention;
- append versus whole-archive replacement delivery; and
- the approved grid target registry and sampling decision.

Change `readiness` to `ready` only when every placeholder and unresolved item is
removed. For licensed sources, tests committed to Git must use synthetic names
and values. Run:

```bash
python -m pytest
```

For the all-Canada ECCC hourly archive, do not assign one timezone to every
station. The source uses local standard time. Either supply an evidence-backed
station timezone policy or curate a declared station allowlist whose members
share one fixed standard-time offset.

## 6. Ingest immutable sources

All source formats use the same command. Examples:

```bash
research-store ingest weather_family_a /download/archive-2019.txt \
  --source-uri 'https://publisher.example/archive-2019.txt' \
  --publisher-vintage '2019 annual release' \
  --fetched-at '2026-08-29T12:00:00Z'

research-store ingest hydrometric_flow_daily /download/hydrometric.sqlite \
  --publisher-vintage '2026-Q3'

research-store ingest hydrometric_level_daily /download/hydrometric.sqlite \
  --publisher-vintage '2026-Q3'

research-store ingest reanalysis_points_hourly /download/reanalysis.nc
research-store ingest wind_scada_10min /secure/scada.csv
```

The two hydrometric commands intentionally reuse one physical file. Its raw
SHA-256 object is stored once, while the catalogue records two dataset-specific
ingestion runs.

If a command is interrupted, run the identical command again. Completed chunk
keys are reused and a partial snapshot remains invisible.

The climate IDs currently map as follows:

| Dataset | Publisher record | Element | Canonical variable |
|---|---|---:|---|
| `weather_family_a` | ECCC HLY01 RCS | 262 | `precipitation_amount` in mm |
| `weather_family_b` | ECCC HLY03 | 123 | `precipitation_amount` in mm |

## 7. Verify provenance and measured cost

```bash
research-store provenance weather_family_a

research-store benchmark weather_family_a \
  --entity '0100001' \
  --year 2019 \
  --variable precipitation_amount
```

Record benchmark JSON when changing entity bucket counts or fragment sizes.

## 8. Read from Python

```python
from research_store import load

rain = load(
    "weather_family_a",
    entity="0100001",
    variable="precipitation_amount",
    start="2015",
    end="2020",       # exclusive
)

snapshot_used = rain.attrs["snapshot_id"]
units = rain.attrs["units"]
```

Publication workflows should record `snapshot_id`. Passing it back to `load`
reproduces the same fragment manifest after later ingestions.

## 9. Use SQL

```bash
research-store sql \
  'SELECT entity_id, time_start, power FROM wind_scada_10min LIMIT 20'
```

Or from Python:

```python
from research_store import connect

with connect() as sql:
    observations = sql.execute(
        "SELECT * FROM wind_scada_10min WHERE entity_id = ?",
        ["T07"],
    ).fetchdf()
    sources = sql.execute(
        "SELECT * FROM catalog.main.source_files"
    ).fetchdf()
```

Dataset views keep variables in separate columns. Catalogue tables are attached
read-only under `catalog.main`.

Join precipitation to the station workbook using the shared string Climate ID:

```sql
SELECT p.entity_id, s.station_name, s.latitude, s.longitude,
       p.time_start, p.precipitation_amount
FROM weather_family_b AS p
JOIN station_inventory AS s USING (entity_id)
```

## 10. Backup

Back up these durable directories together:

```text
$RESEARCH_DATA_ROOT/raw
$RESEARCH_DATA_ROOT/catalog
```

Also back up the private registry overlay named by
`RESEARCH_STORE_PRIVATE_REGISTRY`. Never add it to a public Git repository.

The warehouse is rebuildable, but do not delete it until a complete regeneration
has been tested from the backed-up raw objects and catalogue metadata.
