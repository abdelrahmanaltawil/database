# Ingestion report: National hydrometric station inventory (HYDAT STATIONS)

## Status

Complete for source accounting, timezone resolution and publication. All 8,057
station records in the supplied national HYDAT SQLite database are fully
reconciled to one committed replacement snapshot with zero quarantined or
rejected records.

## Scope and source inventory

- Dataset: `hydrometric_station_inventory`.
- Storage model: `reference` table (`storage_model=reference`, `temporal_kind=reference`).
- Publisher delivery: Environment and Climate Change Canada (ECCC) National Water Data (HYDAT).
- Release vintage: HYDAT 2026-07-17 release snapshot.
- Source container: SQLite database (`Hydat.sqlite3` from `Hydat_sqlite3_20260717.zip`).
- Ingested table: `STATIONS`.
- Total source records: 8,057 hydrometric stations across Canada and adjacent boundary waters.
- Snapshot mode: `replace`.

## Authoritative documentation

- Environment and Climate Change Canada, [HYDAT Database Definition](https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf),
  specifically the `STATIONS` table schema, status codes and drainage area definitions.
- Environment and Climate Change Canada, [HYDAT Release Notes, 2026-07-17](https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_ReleaseNotes_20260717_EN.pdf).
- Environment and Climate Change Canada, [Water Survey of Canada (WSC) Hydrometric Manuals](https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey.html).
- Pinned `timezonefinder` and `tzdata` package definitions used for IANA timezone derivations from station coordinates.

## Metadata crosswalk

| Source field in HYDAT `STATIONS` | Canonical field | Type / Unit | Transformation / Meaning |
|---|---|---|---|
| `STATION_NUMBER` | `entity_id` | String | 7-character canonical hydrometric station ID (e.g. `02GA010`) |
| `STATION_NAME` | `station_name` | String | Official station / watercourse name |
| `PROV_TERR_STATE_LOC` | `province` | String | 2-character province, territory, or state abbreviation |
| `REGIONAL_OFFICE_ID` | `regional_office_id` | String | Regional administrative office identifier |
| `HYD_STATUS` | `hyd_status` | String | Operational status: `A` (Active) or `D` (Discontinued) |
| `SED_STATUS` | `sed_status` | String | Sediment monitoring status: `A` (Active), `D` (Discontinued), or null |
| `LATITUDE` | `latitude` | Float64 (`degree_north`) | Station latitude in signed decimal degrees (WGS84 / EPSG:4326) |
| `LONGITUDE` | `longitude` | Float64 (`degree_east`) | Station longitude in signed decimal degrees (WGS84 / EPSG:4326) |
| `DRAINAGE_AREA_GROSS` | `drainage_area_gross` | Float64 (`km2`) | Total upstream catchment drainage area |
| `DRAINAGE_AREA_EFFECT` | `drainage_area_effective` | Float64 (`km2`) | Effective contributing drainage area (excluding non-contributing storage) |
| `RHBN` | `is_rhbn` | String | `1` = Reference Hydrometric Basin Network (pristine/unregulated baseline), `0` = standard |
| `REAL_TIME` | `is_real_time` | String | `1` = automated telemetry active, `0` = manual / recorded |
| `CONTRIBUTOR_ID` | `contributor_id` | String | Agency contributing data |
| `OPERATOR_ID` | `operator_id` | String | Agency operating the gauging station |
| `DATUM_ID` | `datum_id` | String | Vertical reference datum identifier |
| *Derived from coordinates* | `timezone_name` | String | IANA standard timezone inferred from coordinates via `timezonefinder` |
| *Derived from environment* | `timezone_source` | String | Pinned library and boundary version string |

## Execution identity

- Registry SHA-256: `de7aa1bf79ad72637fe3969fdc64dfa4391d9e16c2f4b7cd44b186e2480f1763`.
- Ingester module: `inventory_sqlite` (version `1`).
- Ingestion run: `run_d74ea4e0a45441799ccfe46af8e1df26`.
- Publisher vintage: `HYDAT 2026-07-17 snapshot`.

## Snapshots

- Committed replacement snapshot: `snap_869394c4d800453b80701b410afd89f3`.
- Snapshot state: `committed` in `snapshots` and `ingestion_runs`.
- Fragment count: 1 Parquet fragment (unpartitioned reference table).
- Fragment row count: 8,057.
- Compression: `zstd` with dictionary encoding and column statistics.

## Reconciliation

An independent inspection of the raw SQLite `STATIONS` table against the published
canonical Parquet fragment demonstrates exact row-level and key-level equality:

```text
  8,057 physical rows in SQLite STATIONS
= 8,057 published station observations
+     0 quarantined or rejected records
```

```sql
SELECT count(*) AS total_stations, count(DISTINCT entity_id) AS distinct_entities
FROM hydrometric_station_inventory;
-- Result: 8057 total, 8057 distinct
```

All 8,057 stations have unique, non-null `entity_id` values. No coordinates are null.

## Rejections and recovery

Zero records were rejected or quarantined during ingestion (`ingestion_rejections` count = 0).
Every station coordinate resolved successfully to an authorized Canadian or adjacent
boundary IANA timezone.

## Quality flags and data-quality interpretation

- **Operational Status**: 2,878 stations are active (`hyd_status = 'A'`) and 5,160
  are discontinued historical stations (`hyd_status = 'D'`). 19 stations have null
  `hyd_status` (primarily sediment-only or non-standard stations).
- **RHBN Basins**: 314 stations are designated as Reference Hydrometric Basin
  Network stations (`is_rhbn = '1'`), representing high-quality pristine catchments
  with stable, long-term hydrological records suitable for climate trend analysis.
- **Real-Time Telemetry**: 2,627 stations have active real-time data transmission
  flags (`is_real_time = '1'`).
- **Drainage Area Completeness**: 7,164 stations have gross drainage area declared;
  3,018 have effective drainage area declared.

### Geographic Distribution

```sql
SELECT 
    province, 
    count(*) AS total_stations, 
    count(*) FILTER (WHERE hyd_status = 'A') AS active_count,
    count(*) FILTER (WHERE is_rhbn = '1') AS rhbn_count,
    count(*) FILTER (WHERE is_real_time = '1') AS real_time_count
FROM hydrometric_station_inventory
GROUP BY province
ORDER BY total_stations DESC;
```

| Province / Jurisdiction | Total Stations | Active Gauges | RHBN Pristine Gauges | Real-Time Gauges |
| :--- | ---: | ---: | ---: | ---: |
| **British Columbia (BC)** | 2,324 | 466 | 85 | 434 |
| **Ontario (ON)** | 1,119 | 580 | 30 | 536 |
| **Alberta (AB)** | 1,104 | 496 | 40 | 428 |
| **Quebec (QC)** | 1,001 | 243 | 22 | 233 |
| **Saskatchewan (SK)** | 748 | 308 | 19 | 292 |
| **Manitoba (MB)** | 659 | 362 | 21 | 298 |
| **Northwest Territories (NT)** | 245 | 108 | 20 | 106 |
| **Newfoundland & Labrador (NL)** | 230 | 105 | 19 | 104 |
| **Nova Scotia (NS)** | 144 | 35 | 13 | 35 |
| **New Brunswick (NB)** | 144 | 50 | 18 | 50 |
| **Yukon (YT)** | 114 | 74 | 13 | 63 |
| **Nunavut (NU)** | 109 | 24 | 9 | 23 |
| **Prince Edward Island (PE)** | 43 | 9 | 2 | 9 |
| *US / Boundary (MT, WA, ND, ME, NY, AK, MN, ID)* | 73 | 18 | 3 | 16 |

## Validation

- `python -m pytest`: 59 tests passed.
- `research-store doctor`: Passed, reporting healthy catalog and current registry SHA-256.
- Architecture tests: Passed with no layer violations or unauthorized Parquet writers.
- Duplicate entity scan: Zero duplicates across the snapshot.
- Timezone derivation audit: All coordinates resolved to valid IANA timezones (e.g. `America/Vancouver`, `America/Toronto`, `America/Edmonton`, `America/Winnipeg`, `America/Halifax`, `America/St_Johns`).
- SQL relational joins: Verified joining with observation series (`hydrometric_flow_daily` and `hydrometric_discharge_unit_corrected`).

## Known limitations

- Station metadata reflects the HYDAT 2026-07-17 snapshot vintage. Real-time operational statuses can change between national HYDAT release cycles.
- Gross and effective drainage area estimates are compiled by the Water Survey of Canada; ungauged or non-contributing drainage delineations are absent for certain small or arctic basins.
- Boundary stations located in the United States operate under USGS / International Joint Commission (IJC) agreements and are assigned US IANA timezones (e.g. `America/New_York`, `America/Denver`).

## Reproduction commands

```bash
# Ingest HYDAT STATIONS table
research-store ingest hydrometric_station_inventory \
  path/to/Hydat.sqlite3 \
  --source-uri 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/Hydat_sqlite3_20260717.zip#Hydat.sqlite3' \
  --publisher-vintage 'HYDAT 2026-07-17 snapshot'

# Verify provenance
research-store provenance hydrometric_station_inventory \
  --snapshot snap_869394c4d800453b80701b410afd89f3
```

## Follow-up

- Use `hydrometric_station_inventory` as the standard relational lookup for daily flow (`hydrometric_flow_daily`) and level (`hydrometric_level_daily`) datasets once their daily interval semantics are finalized.
- Cross-reference with `eccc_station_inventory` via spatial proximity queries or `entity_match_candidates` to link river flow stations with nearby climate precipitation stations.
