# Runbook

These commands initialize and ingest the ECCC research store without putting
research data in Git.

## 1. Install or update

```bash
git clone https://github.com/abdelrahmanaltawil/database.git
cd database
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

For an existing clone, replace the clone and environment-creation steps with:

```bash
cd ~/Developer/database
git pull origin main
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

## 2. Select the local store

```bash
export RESEARCH_DATA_ROOT=/Users/abdelrahmanaltawil/ResearchDataStore
research-store init
research-store doctor
research-store datasets
```

`research-store datasets` should list exactly the three ready ECCC datasets.
The old hydrometric, reanalysis, and SCADA declarations are not active.

## 3. Ingest the station relationship table

```bash
research-store ingest eccc_station_inventory \
  '/absolute/path/to/Station Inventory EN.xlsx' \
  --publisher-vintage '2025 snapshot'
```

The ingester preserves numeric and alphanumeric Climate IDs as strings, derives
an IANA timezone from each station's coordinates, and records the exact timezone
boundary and rule-package versions. Observation rows use the same `entity_id`,
so they join directly to station metadata.

## 4. Ingest All Canada HLY01 files

Point to the `All Canada/Data/Original/Text` directory. Do not point to the
separate Hamilton or Toronto directories. Hamilton and Toronto station IDs
inside the All Canada files remain included.

Test one file first:

```bash
ECCC_DIR='/absolute/path/to/precipitation/All Canada/Data/Original/Text'

caffeinate -i research-store ingest eccc_hly01_observations \
  "$ECCC_DIR/HLY01_RCS_P2004" \
  --publisher-vintage 'ECCC HLY01 archive'
```

After it returns a `snap_...` identifier, ingest every matching file in filename
order:

```bash
caffeinate -i research-store ingest-directory eccc_hly01_observations \
  "$ECCC_DIR" \
  --pattern 'HLY01_RCS_P*' \
  --publisher-vintage 'ECCC HLY01 archive'
```

The command prints progress as `[current/total]`. Already committed identical
files return their existing snapshot, and interrupted files reuse completed
checkpoints when the same command is rerun.

If HLY03 production files are available, ingest them separately with their
actual filename pattern:

```bash
research-store ingest-directory eccc_hly03_observations \
  '/absolute/path/to/HLY03/Text' \
  --pattern 'HLY03*' \
  --publisher-vintage 'ECCC HLY03 archive'
```

## 5. Registered ECCC elements

| Dataset | Elements | Canonical variables |
|---|---|---|
| `eccc_hly01_observations` | 262-280 | Hourly/15-minute precipitation, gauge weight, 2 m wind, and snow depth |
| `eccc_hly03_observations` | 123 | `precipitation_amount_1h` in mm |

The long physical table retains `source_element`. Each declaration owns its
scale, unit, and interval placement. Add any documented ECCC temperature or
other climate element to the same HLY dataset before ingesting files containing
it; an undeclared code intentionally stops ingestion.

## 6. Verify

```bash
research-store provenance eccc_hly01_observations

research-store sql \
  'SELECT count(*) AS rows, min(time_start) AS first_utc, max(time_end) AS last_utc FROM eccc_hly01_observations'
```

Join observations to their relational station record:

```bash
research-store sql \
  'SELECT p.entity_id, s.station_name, s.latitude, s.longitude, p.time_start, p.precipitation_amount_1h FROM eccc_hly01_observations AS p JOIN eccc_station_inventory AS s USING (entity_id) LIMIT 20'
```

From Python:

```python
from research_store import load

rain = load(
    "eccc_hly01_observations",
    entity="0100001",
    variable="precipitation_amount_1h",
    start="2015",
    end="2020",  # exclusive
)

snapshot_used = rain.attrs["snapshot_id"]
units = rain.attrs["units"]
```

## 7. Backup

Back up these durable directories together:

```text
$RESEARCH_DATA_ROOT/raw
$RESEARCH_DATA_ROOT/catalog
```

The warehouse is rebuildable, but do not delete it until a complete regeneration
has been tested from the backed-up raw objects and catalogue metadata.
