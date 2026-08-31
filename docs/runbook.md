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

## 2. Use the local-only store root

By default, the store is created at `ResearchDataStore/` inside this repository.
That directory is explicitly ignored by Git, including all raw sources, Parquet
files and the DuckDB catalogue. No environment setup is required.

To use another local, non-cloud-synchronized disk with adequate capacity, set:

```bash
export RESEARCH_DATA_ROOT=/absolute/path/to/research-data
```

Put that export in the shell profile used by every analysis repository. Use an
absolute path. An explicit CLI `--store` option takes precedence over it.

## 3. Initialize and verify

```bash
research-store init
research-store doctor
research-store datasets
python -m pytest
```

`doctor` prints the store's facts as JSON and then any findings, exiting
non-zero if it found a `problem`. On a healthy store it reports no absolute
fragment paths, no missing fragments and no legacy snapshot directories. To
check the bytes rather than the bookkeeping:

```bash
research-store doctor --verify-sample 200   # re-hash a sample
research-store doctor --verify-all          # re-hash everything (slow)
```

Sources with unresolved time, unit or licensed-schema decisions report
`provisional`; this is an intentional stop condition, not an installation
failure.

### Upgrading a store created before 2026-08-31

Older stores filed fragments under `warehouse/<tier>/<dataset>/snapshots/<id>/`
and catalogued them by absolute path. `doctor` reports both as problems. To
convert in place:

```bash
research-store migrate            # dry run: reports what would move
research-store migrate --apply
```

The migration is resumable and each batch commits its own catalogue update, so
it can be interrupted. On a very large store, bound each pass:

```bash
research-store migrate --apply --budget-seconds 600   # repeat until complete
```

It reports `"complete": true` when there is nothing left to do. Back up
`catalog/store.duckdb` first, and if you ever restore that backup, delete
`catalog/store.duckdb.wal` alongside it — a stale write-ahead log will be
replayed onto the restored file.

## 4. Ingest the station relationship table

The supplied ECCC workbook has three disclaimer rows followed by the real
header. The ingester detects that header and preserves numeric and alphanumeric
Climate IDs as strings:

```bash
research-store ingest eccc_station_inventory \
  '/absolute/path/to/Station Inventory EN.xlsx' \
  --publisher-vintage '2025-01-02 snapshot'
```

Climate observations use the same `entity_id`, so this workbook is the direct
relational station lookup. The ingester also derives an IANA timezone from each
station's coordinates and records the timezone boundary package version.
Stores created before the ECCC naming migration must run this command again
under `eccc_station_inventory`; the immutable workbook object is deduplicated by
SHA-256.

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

For the all-Canada ECCC hourly archive, the source uses local standard time. The
ingester uses each station's IANA timezone from `eccc_station_inventory`, removes
the daylight-saving component, and converts the standard-time interval to UTC.
An observation whose Climate ID has no defensible timezone is recorded in
`ingestion_rejections` and omitted from the UTC observation view; its exact
source line remains in the immutable raw object. This avoids inventing a UTC
instant while keeping the source fully auditable.

The DLY04 daily archive is different: its climatological day closes at a fixed
UTC hour rather than a station-local one, so no timezone lookup is involved and
`eccc_station_inventory` is not a prerequisite. Each day slot is stored as the
interval `[day 06:00Z, day+1 06:00Z)`. That boundary is declared as an era rule
covering 1961-07-01 onward; earlier records closed the maximum- and
minimum-temperature days at different hours, so they are not covered and
ingestion stops rather than restamping them with the modern convention. The
`station_day_class` option must stay `synoptic_24h`: stations that reported at
morning and afternoon observation times closed their day at station-specific
clock times that only the historical inspection reports carry. Restrict a file
to confirmed 24-hour stations with `entity_allowlist` when it mixes both.

## 6. Ingest immutable sources

All source formats use the same command. Examples:

```bash
research-store ingest eccc_hly01_observations /download/HLY01_RCS_P2019 \
  --source-uri 'https://publisher.example/archive-2019.txt' \
  --publisher-vintage '2019 annual release' \
  --fetched-at '2026-08-29T12:00:00Z'

research-store ingest eccc_dly04_observations /download/DLY04_P2019 \
  --publisher-vintage '2019 annual release'

research-store ingest hydrometric_flow_daily /download/hydrometric.sqlite \
  --publisher-vintage '2026-Q3'

research-store ingest hydrometric_level_daily /download/hydrometric.sqlite \
  --publisher-vintage '2026-Q3'

research-store ingest hydrometric_discharge_unit_corrected \
  /download/corrected/snapshot-2026-08-31/corrected_files.tsv \
  --station-metadata /download/HYDAT/2026-07-17/Hydat.sqlite3 \
  --max-drainage-area-km2 10 \
  --source-uri 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/UnitValueData/Discharge/corrected/' \
  --publisher-vintage 'corrected snapshot 2026-08-31' \
  --fetched-at '2026-08-31T00:00:00Z'

research-store ingest reanalysis_points_hourly /download/reanalysis.nc
research-store ingest wind_scada_10min /secure/scada.csv
```

The two hydrometric commands intentionally reuse one physical file. Its raw
SHA-256 object is stored once, while the catalogue records two dataset-specific
ingestion runs.

The corrected unit-value command streams each selected `.csv.xz` member; it
does not extract the CSV collection. The optional threshold uses HYDAT
`STATIONS.DRAINAGE_AREA_GROSS`. Stations that are unmatched or lack that field
are excluded when a threshold is supplied rather than assigned a guessed area.
The dataset identifier remains `hydrometric_discharge_unit_corrected`: the
threshold is recorded in collection provenance and can be changed or omitted on
a later replacement ingestion. Approval Level, Grade and Qualifiers are
preserved on every observation, and no ingestion-time quality filtering occurs.
`qualifiers` is a JSON string array because AQUARIUS can emit more than one
trailing qualifier field even though the CSV declares one `Qualifiers` heading.

If a command is interrupted, run the identical command again. Completed chunk
keys are reused and a partial snapshot remains invisible.

The configured ECCC source elements currently map as follows:

| Dataset | Elements | Canonical variables |
|---|---|---|
| `eccc_hly01_observations` | 262-280 | Hourly/15-minute precipitation, gauge weight, 2 m wind and snow depth |
| `eccc_hly03_observations` | 123 | `precipitation_amount_1h` in mm |
| `eccc_dly04_observations` | 001-003, 010-012 | Daily maximum, minimum and mean air temperature in degC; rainfall and total precipitation in mm; snowfall in cm |

Element 013, snow on the ground, is deliberately unregistered. The archive
documents no observation time for it, so it cannot be given an interval without
guessing. DLY02 shares this record layout and element numbering; it is a
separate publisher product and needs its own registry entry before its files can
be ingested.

The physical long table retains `source_element`. Each element declaration owns
its scale, unit and interval placement. Register additional documented HLY01
elements, such as temperature, in the same dataset before ingesting a file that
contains them; undeclared codes stop ingestion.

The 2004 HLY01 RCS publisher file contains four `######` value fields for snow
depth element 275. The documented field is signed numeric, so no numeric value
can be recovered; these four fields are registered as null sentinels. Their
blank source quality flags and the original bytes remain available for audit.

Five retired or publisher-only Climate IDs are absent from the current station
inventory but occur in recent HLY files: `1102259`, `6112335`, `611E001`,
`6158434`, and `6158435`. Older HLY03 files contain another 37 retired IDs.
They have evidence-backed timezone overrides based on a uniform ECCC
climatological district or a named adjacent station; the evidence is stored
with every override in the registry. An inventory entry that later resolves
differently causes ingestion to stop on a conflict. The outside-Canada ID
`9040900` and special ID `9052008` have no defensible location metadata, so
their records are quarantined instead of being given a guessed UTC conversion.

HLY03 publisher files contain a small number of malformed physical lines. For
that dataset only, the ingester searches a malformed line for complete,
structurally valid 186-character records, publishes those recovered records,
and records the physical line in `ingestion_rejections`. Truncated or invalid
records with no recoverable observation are quarantined. HLY01 remains strict
for structural corruption. For both hourly datasets, a daily record on which
the station's documented standard UTC offset changes is quarantined: the fixed
24-slot line does not identify how a skipped or repeated civil hour should map
onto UTC, so coercing it would invent an interval or a duplicate timestamp.

Audit committed rejections with:

```bash
research-store sql "
SELECT r.dataset_id, a.original_name, r.record_locator, r.reason,
       r.raw_length, r.recovered_record_count, r.raw_sha256, r.details_json
FROM catalog.main.ingestion_rejections AS r
JOIN catalog.main.ingestion_runs AS i ON i.run_id = r.run_id
JOIN catalog.main.source_aliases AS a ON a.source_id = r.source_id
WHERE i.state = 'committed'
ORDER BY r.dataset_id, a.original_name, r.record_locator"
```

The source hash and locator point back to the immutable object recorded in
`catalog.main.source_files`; no malformed bytes are silently discarded.

## 7. Close a production ingestion

Follow the [production ingestion protocol](ingestion-protocol.md). Reconcile
every supplied physical record to accepted, recovered or quarantined records,
and add a dated report under `docs/ingestion-reports/` using its template. A
committed snapshot without that evidence is not a completed production
ingestion.

Then check the store rather than assuming it:

```bash
research-store doctor --verify-sample 200
research-store gc            # reports reclaimable staging and orphans
research-store gc --apply    # reclaims them
```

A failed run deliberately keeps its staging so it can resume without
re-parsing. `gc` is what reclaims the staging of runs that will never resume;
`doctor` lists them as `abandoned_staging`.

## 7a. Correct data that was published wrong

An append cannot fix a value that was already wrong. Withdraw the affected
snapshots first, with a reason that will still make sense in a year:

```bash
research-store supersede eccc_hly01_observations \
  --reason "snow_depth elements 275-278 were published at scale 1.0; the
            delivered RCS bytes are hundredths of a centimetre, so every
            published snow depth was 100x too large. Corrected to scale 0.01
            in ingester version 10 on 2026-08-31."
```

Superseded snapshots stop being readable and stop being inherited by the next
append, but they stay in the catalogue with their reason and keep their
fragments, so the record of what was published survives. Then replay every
archived source for that dataset; the corrected runs start a clean lineage:

```bash
research-store reingest eccc_hly01_observations --dry-run
research-store reingest eccc_hly01_observations
```

Reading the dataset raises `LookupError` until the re-ingest has published at
least one snapshot. That is intentional: it is better than serving values known
to be wrong.

## 8. Verify provenance and measured cost

```bash
research-store provenance eccc_hly01_observations

research-store benchmark eccc_hly01_observations \
  --entity '0100001' \
  --year 2019 \
  --variable precipitation_amount_1h
```

Record benchmark JSON when changing entity bucket counts or fragment sizes.

## 9. Read from Python

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

Publication workflows should record `snapshot_id`. Passing it back to `load`
reproduces the same fragment manifest after later ingestions.

## 10. Use SQL

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
    sources = sql.execute("SELECT * FROM catalog.main.source_files").fetchdf()
```

Dataset views keep variables in separate columns. Catalogue tables are attached
read-only under `catalog.main`.

Join precipitation to the station workbook using the shared string Climate ID:

```sql
SELECT p.entity_id, s.station_name, s.latitude, s.longitude,
       p.time_start, p.precipitation_amount_1h
FROM eccc_hly03_observations AS p
JOIN eccc_station_inventory AS s USING (entity_id)
```

## 11. Backup and restore

Back up these durable directories together:

```text
ResearchDataStore/raw
ResearchDataStore/catalog
```

Also back up the private registry overlay named by
`RESEARCH_STORE_PRIVATE_REGISTRY`. Never add it to a public Git repository.

### Restoring

Put `raw/` and `catalog/` back — at any path, since catalogue fragment paths
are relative to the store root — then re-run the ingest for each archived
source:

```bash
export RESEARCH_DATA_ROOT=/new/absolute/path
research-store doctor                                   # missing_fragments > 0
research-store reingest DATASET --dry-run               # what will be replayed
research-store reingest DATASET                         # for each dataset
research-store doctor --verify-sample 200               # expect no problems
```

`reingest` reads the archived sources out of the catalogue and hands each back
to its ingester under the publisher filename it arrived with, so aliases and
vintages are not invented. It continues past a failure and reports at the end;
re-run it to resume.

An ingest whose run is already committed verifies that the fragments that run
produced are present. If they are, it returns immediately and costs nothing. If
they are missing, it rebuilds them **into the same snapshot**, so the restored
catalogue and the rebuilt warehouse still describe each other. Fragment paths
are content-addressed, so a rebuild lands exactly where the catalogue points.

Test a restore into a scratch directory before relying on it. Do not delete the
warehouse until you have.
