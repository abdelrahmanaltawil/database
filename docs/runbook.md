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

A catalogue created before code versions were recorded (schema 2) makes
`doctor` report `schema_behind`. `research-store init`, or the next ingest,
adds the `run_code_versions` table; nothing else changes. Runs published before
then have no code record.

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

Commit the code first. Every write records the git commit of the installed
package, and `ingest`, `ingest-directory` and `reingest` refuse to run while the
package or `pyproject.toml` has uncommitted changes or untracked files:

```text
error: Refusing to write to the store from commit 4229b00… plus uncommitted
changes to src/research_store/ingestion/geomet_climate_hourly.py, …
```

Edits to docs, tests or notebooks do not count. For development or a scratch
store, `--allow-uncommitted-code` (or `RESEARCH_STORE_ALLOW_UNCOMMITTED_CODE=1`)
writes anyway and records the uncommitted paths with each run; never use it for
production ingestion.

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

To ingest every matching file of a delivered directory in filename order:

```bash
caffeinate -i research-store ingest-directory eccc_hly01_observations \
  "$ECCC_DIR" \
  --pattern 'HLY01_RCS_P*' \
  --publisher-vintage 'ECCC HLY01 archive'
```

It prints progress as `[current/total]` and accepts the same producer options
as `ingest` (for example `--station-metadata`). Already committed identical
files return their existing snapshot, and interrupted files reuse completed
checkpoints when the same command is rerun, so a failed directory run resumes
by being run again.

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
| `eccc_climate_hourly_observations` | GeoMet `climate-hourly` fields (precipitation is element 262) | Air and dew point temperature in degC, relative humidity in %, `precipitation_amount_1h` in mm, wind direction in degrees true (publisher tens of degrees x 10; calm is null), wind speed in km/h, visibility in km, station pressure in kPa, humidex and wind chill (unitless indices), English present-weather text |

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

## 6b. Fetch and ingest ECCC hourly climate observations from MSC GeoMet

`eccc_climate_hourly_observations` is downloaded rather than delivered, so it
takes two commands. `fetch` is the only step that uses the network; `ingest` is
offline and replayable.

```bash
research-store fetch eccc_climate_hourly_observations \
  --station 6153301 \
  --start 2000-01-01 --end 2026-09-01
```

`--station` repeats for several Climate IDs, and `--start`/`--end` pairs repeat
for several disjoint periods. Days are local standard time; `--end` is
exclusive, so end at the first day of a month to keep the last window a
complete month. Each station and LST calendar year is one window: the command
asks for the window's record count (`resulttype=hits`), then for its CSV pages
(`limit=10000`, `offset` paging, sorted by `LOCAL_DATE`). The API's `datetime`
filter is inclusive, so a window's request ends one second before the next
window starts; that keeps the 23:30 records of UTC-3:30 (Newfoundland)
stations, whose `LOCAL_DATE` falls on the half hour. Requests are strictly
sequential and at least `--delay-seconds` apart (default and minimum 1 s), a
busy server (429 or 5xx) is retried at most three times with 5/10/20 s
back-off honouring `Retry-After`, and the User-Agent names this store. The
example above is 27 windows, 54 requests. A station with no records in the
range stops the fetch: the collection serves only a subset of ECCC stations.

The command prints the selection manifest it wrote, for example
`ResearchDataStore/downloads/eccc_climate_hourly_observations/20260925T063700Z_<digest>.tsv`,
and a JSON summary whose `open_windows` counts windows that ended less than
the registry's `settle_days` (7) before they were retrieved and can still gain
hours. Responses are cached by request URL: running the same fetch again makes
no requests for settled windows, and asks again for open ones. Ingest the
printed manifest:

```bash
research-store ingest eccc_climate_hourly_observations \
  ResearchDataStore/downloads/eccc_climate_hourly_observations/<manifest>.tsv \
  --publisher-vintage 'MSC GeoMet climate-hourly (pygeoapi 0.20.0), retrieved 2026-09-25'
```

The source URI defaults to the collection URL and the fetch time to the newest
retrieval in the manifest; each response is archived with its own request URL
and retrieval time. Every ingest publishes a replacement snapshot. It is
refused when it would hide hours readers can see, the `LOCAL_DATE` range each
window of the published snapshot held (so starting at a station's first
record, or ending after its last, is not a shrink); pass
`--allow-selection-shrink` only when publishing a smaller selection on purpose.
The run records how the check was applied as `selection_guard`.

To refresh, because ECCC revises live values:

```bash
research-store fetch eccc_climate_hourly_observations \
  --station 6153301 --start 2000-01-01 --end 2026-10-01 --refresh
```

`--refresh` requests every response again. A response whose bytes (for a
count, whose `numberMatched`) are unchanged keeps its original file and
retrieval time, so an unchanged refresh prints the same manifest and its
ingest is a no-op; the exception is an open window that has settled since,
which is recorded again with the retrieval that makes it final. Without
`--refresh`, only windows that are new, not cached or still open are
downloaded. Manifest names begin with their newest retrieval time, so
`reingest` replays them in the order they were fetched.

`downloads/eccc_climate_hourly_observations/` is only a cache. Once ingested,
every byte is in `raw/`, and `research-store reingest
eccc_climate_hourly_observations` rebuilds the dataset without it. Other
directories under `downloads/` hold delivered publisher downloads that
ingestion reports cite; keep them. `reingest` replays the archived manifests
oldest first. One that was published before is not measured against the newer
snapshot it precedes, so the newest published selection ends up live; one that
was refused is refused again, and `reingest` then exits 1 after reporting it.
A rebuild keeps the rebuilt snapshot's original commit time, so it never
changes which snapshot is live.

Reconcile the live snapshot window by window. The count, the physical rows and
accepted plus quarantined rows must agree:

```bash
research-store sql "
WITH latest AS (
  SELECT run_id FROM catalog.main.snapshots
  WHERE dataset_id = 'eccc_climate_hourly_observations' AND state = 'committed'
  ORDER BY committed_at DESC LIMIT 1
), counts AS (
  SELECT json_extract_string(i.details_json, '$.climate_id') AS climate_id,
         json_extract_string(i.details_json, '$.window_start_lst') AS window_start,
         CAST(json_extract(i.details_json, '$.number_matched') AS BIGINT) AS number_matched,
         json_extract_string(i.details_json, '$.first_local_date') AS first_local_date,
         json_extract_string(i.details_json, '$.last_local_date') AS last_local_date,
         json_extract_string(i.details_json, '$.settled_at_retrieval') AS settled
  FROM catalog.main.ingestion_inputs AS i JOIN latest USING (run_id)
  WHERE i.input_role = 'completeness_evidence'
), pages AS (
  SELECT json_extract_string(i.details_json, '$.climate_id') AS climate_id,
         json_extract_string(i.details_json, '$.window_start_lst') AS window_start,
         CAST(sum(CAST(json_extract(i.details_json, '$.rows') AS BIGINT)) AS BIGINT) AS physical_rows,
         CAST(sum(CAST(json_extract(i.details_json, '$.rows_published') AS BIGINT)) AS BIGINT) AS published,
         CAST(sum(CAST(json_extract(i.details_json, '$.rows_quarantined') AS BIGINT)) AS BIGINT) AS quarantined
  FROM catalog.main.ingestion_inputs AS i JOIN latest USING (run_id)
  WHERE i.input_role = 'observation_source'
  GROUP BY 1, 2
)
SELECT c.climate_id, c.window_start, c.number_matched,
       coalesce(p.physical_rows, 0) AS physical_rows,
       coalesce(p.published, 0) AS published,
       coalesce(p.quarantined, 0) AS quarantined,
       c.first_local_date, c.last_local_date, c.settled
FROM counts AS c LEFT JOIN pages AS p USING (climate_id, window_start)
ORDER BY 1, 2"
```

Quarantined records carry `missing_station_timezone`,
`standard_timezone_transition` or `publisher_utc_mismatch` in
`ingestion_rejections`, with the response's source ID, a `line:N` locator and
the publisher and derived UTC times.

Each row is the hour ending at `LOCAL_DATE`: `time_end` is the publisher's
`UTC_DATE`, `precipitation_amount_1h` is the total over the row, and the other
variables are observed at its end. HLY01 files the same element-262 totals one
hour later (a documented HLY01 defect awaiting a controlled replacement), so
subtract one hour from HLY01 keys before comparing the two. The publisher's
calm direction `0` is stored as a null `wind_direction`; a null with a blank
flag is a calm report, a missing one carries `M`, and a `wind_speed` of 0 is a
measured calm. Cite the data as "Data Source: Environment and Climate Change
Canada".

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
research-store provenance eccc_hly01_observations --code

research-store benchmark eccc_hly01_observations \
  --entity '0100001' \
  --year 2019 \
  --variable precipitation_amount_1h
```

`--code` lists every run whose fragments the snapshot reads, with the commit
that wrote it and any uncommitted paths; an append snapshot lists the runs it
inherited too. Cite that commit in a report's Execution identity. Runs written
before code versions were recorded show no commit.

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
