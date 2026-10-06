# Ingestion report: ECCC climate-hourly observations (MSC GeoMet), HAMILTON RBG CS

## Status

**Complete for source accounting and publication on the live store. The
dataset code, the registry correction and this report are committed together
as `979d171`, which `main` contains since the merge `c4f50a9` (2026-10-05).**

> **Correction, 2026-10-06.** This report named its code by the branch
> `feature/eccc-climate-hourly` and said the branch was not pushed. It was
> pushed, merged into `main` by `c4f50a9` on 2026-10-05, and deleted, so the
> code references now cite the commits `main` keeps: `4229b00` and `979d171`.
> No result, identity or evidence changed.

Times in this report are UTC unless marked LST or EDT. The live checks dated
2026-10-04 here ran on the evening of 2026-10-03 EDT (UTC-4), the date the
forecasting repository's documents give them.

- **Production: complete.** The live ingest ran on 2026-09-28 and committed
  `snap_79a496ff47a1448da3a09bbda6a7ce94`. It ingested the selection manifest
  and all 54 publisher responses it lists, published all 225,689 records and
  quarantined none, and every one of the 27 windows reconciles exactly.
- **Live equals the rehearsal.** This report was drafted on 2026-09-25 from a
  scratch store built from the same fetched bytes. The live values were read
  from the live store, read-only, on 2026-10-04 UTC. The ingester version,
  registry SHA-256 and dataset digest, the reconciliation (row for row), the
  rejection count and the flag and calm counts all equal the scratch values.
- **Idempotency on the live store: checked.** The user repeated the ingest
  (reproduction step 6) on 2026-10-04 at 03:14:23Z (2026-10-03 23:14 EDT). It
  printed `snap_79a496ff47a1448da3a09bbda6a7ce94` and left 1 run, 1 snapshot
  and 52 fragments (see Validation).
- **Store integrity: checked.** `doctor --verify-all` on the live store,
  after the repeated ingest, re-hashed all 52,893 committed fragments: 0
  failed, 0 missing, no problem.
- **Committed afterwards:** the live run used the dataset's code as
  working-tree changes on top of `4229b00` (see Execution identity). That code,
  the registry correction and this report were then committed together as
  `979d171`. Since the run, only the documentation in `registry.py` changed,
  which no identity hash covers.
- **Data quality is a separate question.** The values are the live National
  Climate Archive as served on 2026-09-25. They carry only automatic assessment
  (status R before 2013-12-10, Q from then on), ECCC can revise them, and flags
  are kept verbatim. Where both sources have a value, the values equal the ECCC
  bulk hourly CSVs for 2013-2025 hour for hour. The exceptions are 659
  wind-speed hours: 650 follow a different calm-wind convention and 9 changed
  in July 2022. The API also serves no record for 21 hours in which the CSVs
  have values, and it reports 289 July-2022 precipitation hours as missing
  where the CSVs have values (see Validation).

## Scope and source inventory

- Dataset: `eccc_climate_hourly_observations`. Wide storage, interval rows,
  replacement snapshots.
- Publisher: Environment and Climate Change Canada, MSC GeoMet OGC API
  collection `climate-hourly` (pygeoapi 0.20.0).
- Station: HAMILTON RBG CS, Climate ID `6153301` (publisher `STN_ID` 27529).
  Its timezone is America/Toronto; local standard time (LST) is UTC-5 all year.
- Selection: `research-store fetch --station 6153301 --start 2000-01-01 --end
  2026-09-01`. That is 27 LST calendar-year windows, 2000 to 2026, where the
  2026 window runs from 01-01 to 09-01.
- Retrieval: 2026-09-25, 08:11:12Z to 08:12:58Z. There are 54 responses: for
  each window, one `resulttype=hits` count and one CSV page (every window has
  fewer than 10,000 records).
- Selection manifest: `20260925T081258Z_6a06a98ed15f.tsv`, 24,483 bytes, in
  `ResearchDataStore/downloads/eccc_climate_hourly_observations/`. That
  directory is a fetch cache; ingesting archives every byte into `raw/`.
- Records: 225,689 by `numberMatched`. `LOCAL_DATE` runs from 2000-08-17 17:00
  (the station's first record in the collection) to 2026-08-31 23:00 LST.
- Open windows: 0. Every window ended at least `settle_days` (7) before it was
  retrieved.
- Timezone input: the `eccc_station_inventory` snapshot the ingest resolves
  time zones from. On the scratch store this was a re-ingest of the live
  store's archived 2025 Station Inventory workbook, the same bytes. Live:
  `snap_a044db0435784c3fa1adde9467d25bb3`, committed 2026-08-30 01:38:11Z from
  that workbook (`Station Inventory EN.xlsx`, 870,856 bytes). It is the live
  store's only `eccc_station_inventory` snapshot.

| LST window | Records | First `LOCAL_DATE` | Last `LOCAL_DATE` | Settled |
|---|---:|---|---|---|
| 2000 | 3,186 | 2000-08-17 17:00 | 2000-12-31 23:00 | yes |
| 2001-2016 | 8,744 · 8,685 · 8,726 · 8,750 · 8,711 · 8,752 · 8,744 · 8,781 · 8,754 · 8,760 · 8,754 · 8,776 · 8,703 · 8,640 · 8,693 · 8,737 | 01-01 00:00 | 12-31 23:00 | yes |
| 2017 | 7,252 | 2017-01-01 00:00 | 2017-10-30 10:00 | yes |
| 2018 | 8,449 | 2018-01-10 12:00 | 2018-12-31 23:00 | yes |
| 2019-2025 | 8,753 · 8,760 · 8,741 · 8,744 · 8,744 · 8,775 · 8,750 | 01-01 00:00 | 12-31 23:00 | yes |
| 2026 | 5,825 | 2026-01-01 00:00 | 2026-08-31 23:00 | yes |
| **Total** | **225,689** | | | |

The station has no records from 2017-10-30 11:00 to 2018-01-10 11:00 LST.
September 2026 is deliberately left out, because it would have been an open
window.

## Authoritative documentation

These were used as evidence, not as executable instructions. They are the same
references `DatasetSpec.documentation` catalogues.

- Environment and Climate Change Canada, MSC GeoMet OGC API collection
  `climate-hourly` ("Climate - Hourly Observations"): collection metadata and
  queryables, <https://api.weather.gc.ca/collections/climate-hourly>, retrieved
  2026-09-25 (pygeoapi 0.20.0).
- Environment and Climate Change Canada, MSC Open Data, *GeoMet-OGC-API*
  technical documentation,
  <https://eccc-msc.github.io/open-data/msc-geomet/ogc_api_en/>, accessed
  2026-09-25. It covers `f=csv` output, the limit of 10,000 features per query,
  offset paging, and `numberMatched` in `resulttype=hits` responses.
- Environment and Climate Change Canada, *Climate Data Online Glossary*,
  <https://climate.weather.gc.ca/glossary_e.html>, modified 2026-08-10. Used
  for: Local Standard Time; Total Hourly Precipitation (element 262, minutes
  00 to 60); status R before and Q from 2013-12-10; wind direction and calm;
  wind speed; humidex; wind chill; station pressure.
- Environment and Climate Change Canada, *Technical documentation: Historical
  Hourly Climate Station Data*, canada.ca, modified 2023-05-01. It says:
  - wind direction is in tens of degrees true, and 0 means calm;
  - wind speed is at 10 m, averaged over the 1, 2 or 10 minutes ending at the
    observation;
  - humidex is shown only at 20 degC or above, and only when it is at least 1
    degree above the air temperature;
  - wind chill is shown only at or below 0 degC.
- Environment and Climate Change Canada, *Climate Data Online FAQ*,
  <https://climate.weather.gc.ca/FAQ_e.html>: flag `M` means missing.
- *Environment and Climate Change Canada Data Services End-use Licence*,
  version 2.1.1, August 2026,
  <https://eccc-msc.github.io/open-data/licence/readme_en/>. The information is
  licensed "as is", and attribution is required: **"Data Source: Environment
  and Climate Change Canada"**.

## Metadata crosswalk

Each response is a UTF-8 CSV with CRLF line endings and a fixed header of 41
columns, one row per Climate ID and `LOCAL_DATE`; an empty field means null.
`foundation/registry.py` declares every column, and the table below accounts
for all 41.

| Publisher column(s) | Stored as | Unit and scale | Place in the row's hour |
|---|---|---|---|
| `CLIMATE_IDENTIFIER` | `entity_id` | - | - |
| `LOCAL_DATE` (H, LST) | `time_end` = H converted to UTC with the station's standard offset; `time_start` = `time_end` - 1 h | - | the row is the hour ending at H, [H-1 h, H) |
| `UTC_DATE` | not stored; must equal `time_end`, or the record is quarantined as `publisher_utc_mismatch` | - | - |
| `TEMP`, `TEMP_FLAG` | `air_temperature`, `air_temperature_quality` | degC, 1.0 | instant at H |
| `DEW_POINT_TEMP`, `_FLAG` | `dew_point_temperature`, `_quality` | degC, 1.0 | instant at H |
| `RELATIVE_HUMIDITY`, `_FLAG` | `relative_humidity`, `_quality` | %, 1.0 | instant at H |
| `PRECIP_AMOUNT`, `_FLAG` | `precipitation_amount_1h`, `_quality` | mm, 1.0 | total over [H-1 h, H) |
| `WIND_DIRECTION`, `_FLAG` | `wind_direction`, `_quality` | degree_true, **x10**; raw 0 (calm) becomes null | 2-minute mean ending at H |
| `WIND_SPEED`, `_FLAG` | `wind_speed`, `_quality` | km/h, 1.0 | 1-, 2- or 10-minute mean ending at H |
| `VISIBILITY`, `_FLAG` | `visibility`, `_quality` | km, 1.0 | instant at H |
| `STATION_PRESSURE`, `_FLAG` | `station_pressure`, `_quality` | kPa, 1.0 | instant at H |
| `HUMIDEX`, `_FLAG` | `humidex`, `_quality` | index (unit `1`), 1.0 | instant at H |
| `WINDCHILL`, `WINDCHILL_FLAG` | `wind_chill`, `wind_chill_quality` | index (unit `1`), 1.0 | instant at H |
| `WEATHER_ENG_DESC` | `weather_description` (string) | - | at H |
| `STN_ID` | annotation `source_station_id`, verbatim | - | - |
| `FLAG` | annotation `record_flag`, verbatim; the collection does not document its codes | - | - |
| `ID`; `LOCAL_YEAR`, `LOCAL_MONTH`, `LOCAL_DAY`, `LOCAL_HOUR`; `UTC_YEAR`, `UTC_MONTH`, `UTC_DAY` | not republished; must agree with `LOCAL_DATE` and `UTC_DATE`, or ingestion stops | - | - |
| `x`, `y`, `STATION_NAME`, `PROVINCE_CODE`, `LONGITUDE_DECIMAL_DEGREES`, `LATITUDE_DECIMAL_DEGREES` | not republished; must be constant within each response (recorded as `page_constants`), with `x`/`y` equal to the coordinates, or ingestion stops | - | - |
| `WEATHER_FRE_DESC` | not republished and not checked | - | - |

**Checks on raw values.** Every raw `WIND_DIRECTION` must be a whole number
from 0 to 36 before it is scaled, and any other value stops ingestion. A
registry test checks that 36 times the scale is 360, so a forgotten x10 cannot
pass. Numbers must be plain decimals, an hour must not repeat within a window,
each window must stay inside one calendar year, and each window's page offsets
must cover its count exactly.

**Missing-value markers.** Each marker applies only to the fields named for
it:

- empty field: null in every value field. Flags and annotations keep a blank
  verbatim.
- `NA`: null in `weather_description` only. An `NA` in a numeric field stops
  ingestion.
- `0`: null in `wind_direction` only (calm has no direction). A wind speed of 0
  is a measured zero.

**Timing and its evidence.** Two facts come from the documentation: the
glossary defines `LOCAL_DATE` as local standard time, and it gives element 262
as the total over minutes 00 to 60. That the row is the hour *ending* at H is
shown by three pieces of evidence:

1. At 410 rain onsets in the 2013-2025 bulk hourly CSVs, relative humidity
   rises 10.3 points and temperature falls 1.5 degC between the H-1 and H
   readings, and both barely change from H to H+1. Those CSVs equal this
   collection wherever the two were compared.
2. ECCC documents the slots of `eccc_hly03_observations` element 123 as hours
   ending 01-24. At its onsets, the bulk-CSV total labelled with the same
   ending hour is non-zero in 125 of 128 cases, and the one labelled an hour
   earlier in only 4 of 126.
3. Across the full record, 12,027 non-zero hours are equal when the join uses
   HLY01 `time_start` = this dataset's `time_end`, against 558 when it uses
   `time_start` = `time_start` (see Validation).

`variable_timing` in the registry declares where each variable sits in the row.

**UTC cross-check.** Each row's `LOCAL_DATE` is converted to UTC through the
inventory's `timezone_name` and its standard offset, and the result is compared
with the publisher's own `UTC_DATE`. All 225,689 rows agree, so none was
quarantined as `publisher_utc_mismatch`.

## Execution identity

| Item | Scratch run (rehearsal) | Live run |
|---|---|---|
| Producer | `geomet_climate_hourly`, `VERSION` 1 | same |
| Ingester version | `1+selection.7a77b0c953d837ef5ee8ec54adc03c85e577c57b7b382405e00f06ff1e620dcb` | `1+selection.7a77b0c953d837ef5ee8ec54adc03c85e577c57b7b382405e00f06ff1e620dcb`, identical. The suffix fingerprints the manifest entries (window, role, offset, SHA-256, count). |
| Registry SHA-256 | `88861b99aadeb4ed16b3a375a504868dfba23e0e45e9da6eaecaa7aba30e6a57` | `88861b99aadeb4ed16b3a375a504868dfba23e0e45e9da6eaecaa7aba30e6a57`, identical |
| Dataset digest | `f94893c97082a59e27f7edcd45e1a0260c4492e3703a9a5010fcd568d1634053` | `f94893c97082a59e27f7edcd45e1a0260c4492e3703a9a5010fcd568d1634053`, identical |
| Run | `run_1ec1a176d8b74335ac73f1933aa9320b` | `run_aeb9e44fd6b84699a9ae2a90d52c3b6d`, started 2026-09-28 17:40:51.943Z, completed 17:41:14.554Z |
| pyarrow | 25.0.1, pinned in `pyproject.toml`. The Parquet footer carries it, so fragment paths depend on it. | 25.0.1. The environment the run used reports `pyarrow.__version__` 25.0.1, and the footers of all 52 live fragments read `parquet-cpp-arrow version 25.0.1`. |
| Publisher vintage | `MSC GeoMet climate-hourly (pygeoapi 0.20.0), retrieved 2026-09-25` | same string |
| Fetch time | 2026-09-25T08:12:58Z, the newest retrieval in the manifest | same |
| Source URI | `https://api.weather.gc.ca/collections/climate-hourly`; each response is archived with its own request URL and retrieval time | same |
| Code | branch `feature/eccc-climate-hourly`, uncommitted when this draft was written | commit `4229b001bfe8e7a284c25dba3469f8ee59f4a3f3`, the head of the branch `feature/eccc-climate-hourly` since 2026-09-25 (the branch was deleted after its merge into `main`, whose history keeps the commit), **plus uncommitted working-tree changes that hold all of this dataset's code**: the new acquisition, ingester and station-time modules, the registry entry, the changes to the CLI, catalogue, models, paths and writer, and their tests. No commit contained the code when the run used it; it was committed afterwards as `979d171` (Follow-up 1). Every file under `src/` except `foundation/registry.py` was last modified at or before 2026-09-25 08:05:07Z, before the scratch run committed (08:17:48Z), so none of them changed between the draft and the run. `foundation/registry.py` was last modified on 2026-10-04 at 03:49Z (2026-10-03 23:49 EDT) by the documentation corrections (Follow-up 2), so its modification time proves nothing about the run. For that file the proof is hash equality: the run's registry SHA-256 and dataset digest equal the scratch run's and the current file's. |

Neither hash covers `DatasetSpec.documentation`, so a documentation-only
registry correction (see Follow-up) changes neither of them. The corrections
made on 2026-10-04 changed neither (Follow-up 2). Source SHA-256 values stay in
the catalogue and are left out of this report.

## Snapshots

- **Scratch (evidence only, not reader-visible):**
  `snap_bcd96b958898461283db7339ecfd4816`. It was committed at 2026-09-25
  08:17:48.404907 UTC and holds 52 fragments with 225,689 rows. The UTC
  interval runs from `time_start` 2000-08-17 21:00Z to `time_end`
  2026-09-01 04:00Z. The store has 1 committed snapshot of this dataset, no
  staging run and no failed run.
- **Live:**
  - snapshot ID: `snap_79a496ff47a1448da3a09bbda6a7ce94`;
  - `committed_at`: 2026-09-28 17:41:14.554001 UTC (13:41:14 EDT);
  - fragments and rows: 52 and 225,689, as expected. The UTC interval runs
    from `time_start` 2000-08-17 21:00Z to `time_end` 2026-09-01 04:00Z, as on
    the scratch store;
  - staging, failed or abandoned runs of this dataset: none. On 2026-10-04,
    after the repeated ingest, the live store held exactly 1 run (committed)
    and 1 snapshot (committed) of this dataset, and no staging, running or
    abandoned run of any dataset.

## Reconciliation

This is the runbook §6b query run against the scratch store. Its output
contains no identifiers, so the live output must match it row for row:

```text
climate_id        window_start  number_matched  physical_rows  published  quarantined    first_local_date     last_local_date settled
   6153301 2000-01-01T00:00:00            3186           3186       3186            0 2000-08-17T17:00:00 2000-12-31T23:00:00    true
   6153301 2001-01-01T00:00:00            8744           8744       8744            0 2001-01-01T00:00:00 2001-12-31T23:00:00    true
   6153301 2002-01-01T00:00:00            8685           8685       8685            0 2002-01-01T00:00:00 2002-12-31T23:00:00    true
   6153301 2003-01-01T00:00:00            8726           8726       8726            0 2003-01-01T00:00:00 2003-12-31T23:00:00    true
   6153301 2004-01-01T00:00:00            8750           8750       8750            0 2004-01-01T00:00:00 2004-12-31T23:00:00    true
   6153301 2005-01-01T00:00:00            8711           8711       8711            0 2005-01-01T00:00:00 2005-12-31T23:00:00    true
   6153301 2006-01-01T00:00:00            8752           8752       8752            0 2006-01-01T00:00:00 2006-12-31T23:00:00    true
   6153301 2007-01-01T00:00:00            8744           8744       8744            0 2007-01-01T00:00:00 2007-12-31T23:00:00    true
   6153301 2008-01-01T00:00:00            8781           8781       8781            0 2008-01-01T00:00:00 2008-12-31T23:00:00    true
   6153301 2009-01-01T00:00:00            8754           8754       8754            0 2009-01-01T00:00:00 2009-12-31T23:00:00    true
   6153301 2010-01-01T00:00:00            8760           8760       8760            0 2010-01-01T00:00:00 2010-12-31T23:00:00    true
   6153301 2011-01-01T00:00:00            8754           8754       8754            0 2011-01-01T00:00:00 2011-12-31T23:00:00    true
   6153301 2012-01-01T00:00:00            8776           8776       8776            0 2012-01-01T00:00:00 2012-12-31T23:00:00    true
   6153301 2013-01-01T00:00:00            8703           8703       8703            0 2013-01-01T00:00:00 2013-12-31T23:00:00    true
   6153301 2014-01-01T00:00:00            8640           8640       8640            0 2014-01-01T00:00:00 2014-12-31T23:00:00    true
   6153301 2015-01-01T00:00:00            8693           8693       8693            0 2015-01-01T00:00:00 2015-12-31T23:00:00    true
   6153301 2016-01-01T00:00:00            8737           8737       8737            0 2016-01-01T00:00:00 2016-12-31T23:00:00    true
   6153301 2017-01-01T00:00:00            7252           7252       7252            0 2017-01-01T00:00:00 2017-10-30T10:00:00    true
   6153301 2018-01-01T00:00:00            8449           8449       8449            0 2018-01-10T12:00:00 2018-12-31T23:00:00    true
   6153301 2019-01-01T00:00:00            8753           8753       8753            0 2019-01-01T00:00:00 2019-12-31T23:00:00    true
   6153301 2020-01-01T00:00:00            8760           8760       8760            0 2020-01-01T00:00:00 2020-12-31T23:00:00    true
   6153301 2021-01-01T00:00:00            8741           8741       8741            0 2021-01-01T00:00:00 2021-12-31T23:00:00    true
   6153301 2022-01-01T00:00:00            8744           8744       8744            0 2022-01-01T00:00:00 2022-12-31T23:00:00    true
   6153301 2023-01-01T00:00:00            8744           8744       8744            0 2023-01-01T00:00:00 2023-12-31T23:00:00    true
   6153301 2024-01-01T00:00:00            8775           8775       8775            0 2024-01-01T00:00:00 2024-12-31T23:00:00    true
   6153301 2025-01-01T00:00:00            8750           8750       8750            0 2025-01-01T00:00:00 2025-12-31T23:00:00    true
   6153301 2026-01-01T00:00:00            5825           5825       5825            0 2026-01-01T00:00:00 2026-08-31T23:00:00    true
```

Live output (step 3, run on 2026-10-04 against
`snap_79a496ff47a1448da3a09bbda6a7ce94`): **identical to the block above, row
for row and byte for byte**. A `diff` of the two outputs is empty: the same 27
windows, the same counts, 0 quarantined in every window, the same first and
last `LOCAL_DATE`, and every window settled.

The totals close exactly, on the scratch store and on the live store:

```text
225,689 records by numberMatched (27 count responses)
= 225,689 physical rows in the 27 CSV pages
= 225,689 published + 0 quarantined
= 225,689 rows in the snapshot's 52 fragments (sum of fragment row_count)
= 225,689 distinct time_end values for entity 6153301 (no repeated hour)
```

Inputs: 55 ingestion inputs, all committed. They are 1 `selection_manifest`,
27 `completeness_evidence` responses and 27 `observation_source` responses,
which is exactly the manifest's 54 responses plus the manifest itself. The
selection guard ran normally (`selection_guard` = `checked`) with
`open_windows` 0. On the live store it compares against no earlier snapshot,
because this is the dataset's first publication there. The live run has the
same 55 inputs (1, 27 and 27), and its selection record holds
`selection_guard` `checked`, `open_windows` 0, 27 windows, 27 pages,
`number_matched_total` 225,689, `rows_published` 225,689 and
`rows_quarantined` 0.

## Rejections and recovery

The scratch run has no `ingestion_rejections`:

| Reason | Records |
|---|---:|
| `missing_station_timezone` | 0 |
| `standard_timezone_transition` | 0 |
| `publisher_utc_mismatch` | 0 |

No line was malformed, so nothing was recovered. The checks that stop
ingestion instead of quarantining (numeric parsing, the 0-36 direction range,
per-response constants, component agreement, repeated hours, page coverage of
the count) all passed; otherwise no snapshot would exist. Live: empty, as
expected. The live run has 0 `ingestion_rejections` (step 4), and its
selection record's `quarantined_by_reason` is empty, so the table above holds
for it unchanged.

## Quality flags and data-quality interpretation

Nothing was filtered at ingestion. The only non-blank code in any of the ten
`_quality` fields is `M`. `record_flag` is blank on every row, and
`source_station_id` is 27529 on every row. No `M`-flagged row carries a value.

The live snapshot gives the same counts. On 2026-10-04, step 5 printed exactly
the eight `M` counts in the table below and no non-blank `record_flag`. It
printed the calm counts 48,301, 48,168, 131 and 2, no stored direction of 0,
34,872 humidex values (minimum 25), 36,722 wind-chill values (maximum 0), and
183,782 precipitation values (12,840 non-zero, maximum 56.5 mm). A further
count over the live view matches the Non-null values column for every
variable. It also confirmed `source_station_id` 27529 on all 225,689 rows, and
225,689 distinct `time_end` values for the one entity.

| Variable | `M` rows | Where the `M` flags are | Non-null values |
|---|---:|---|---:|
| `air_temperature` | 7,727 | 4,445 in 2019, 3,240 in 2020, 42 elsewhere | 217,960 |
| `dew_point_temperature` | 10,634 | mostly 2016-2020 (1,996 in 2018, 4,482 in 2019, 3,752 in 2020) | 215,054 |
| `relative_humidity` | 2,989 | mostly 2016-2018 and 2020 | 222,699 |
| `precipitation_amount_1h` | 9,051 | 5,560 in 2019, 3,201 in 2020, 289 in 2022-07, 1 in 2023 | 183,782 |
| `wind_direction` | 585 | 158 in 2003, 385 in 2014, 42 elsewhere | 176,803 |
| `wind_speed` | 260 | 158 in 2003, 97 in 2024-10, 5 elsewhere | 225,427 |
| `visibility` | 38,091 | every row of 2000-2004 | 0 |
| `station_pressure` | 108,304 | every row of 2000-2012 except the partly reported hour 2009-03-31 20:00 LST, and 182 hours of 2013 (the last at 2013-07-03 18:00 LST) | 117,383 |
| `humidex` | 0 | - | 34,872 |
| `wind_chill` | 0 | - | 36,722 |
| `weather_description` | no flag field | - | 0 |

How to read these counts:

- **A blank flag does not always mean a value was reported.**
  - Precipitation is null with a blank flag in 32,856 hours, all before 2019.
    The publisher uses `M` for missing precipitation only from 2019. Of these
    hours:
    - 31,953 fall before the first gauge value, at 2004-04-19 13:00 LST;
    - 18 run from 2004-04-19 16:00 to 2004-04-20 09:00 LST, after three
      observed hours;
    - 885 fall in 2007-2016: 226 in 2007, 24 in 2008, 26 in 2009, 11 in 2010,
      232 in 2011, 364 in 2012, and 1 each in 2013 and 2016.
  - Visibility is null with a blank flag from 2005 onwards.
  - Two hours were only partly reported, and their null values carry blank
    flags:
    - 2009-03-31 20:00 LST: precipitation is 0.0 mm. Temperature, dew point,
      humidity, wind direction, wind speed and station pressure are null.
    - 2016-08-22 03:00 LST: temperature 16.0 degC, dew point 11.1 degC and
      humidity 73 % are reported. Precipitation, wind direction, wind speed
      and station pressure are null.
  - At 2020-06-10 23:00 LST air temperature is null with a blank flag. Dew
    point, humidity, precipitation, wind and station pressure are reported in
    that hour.
  - Outside these three hours, no temperature, dew-point, humidity, wind-speed
    or station-pressure value is null with a blank flag.
- **Calm wind.**
  - 48,301 rows have a null direction with a blank flag. Of these, 48,168 have
    a speed of 0, which is a measured calm.
  - 131 have a speed of 1 km/h: every one is in 2018, and the publisher gives
    direction 0 (calm).
  - The remaining 2 are the two partly reported hours above, whose wind was
    not reported; they are not calm.
  - Identify a calm report as a null direction, a blank flag and a non-null
    speed: 48,299 rows.
  - No direction of 0 is stored. Directions run from 10 to 360 in steps of 10,
    and no speed of 0 carries a direction.
- **Humidex and wind chill** exist only under their display rules, so a null
  there means the index does not apply, not that data is missing.
  - Humidex: 34,872 values, minimum 25, none at an air temperature below
    20 degC.
  - Wind chill: 36,722 values, maximum 0, none at an air temperature above
    0 degC.
- **Precipitation:**
  - 12,840 values are non-zero; the maximum is 56.5 mm.
  - Every value is on the 0.1 mm grid.
  - The first observed hour is 2004-04-19 13:00 LST.
- **Coverage gaps worth knowing:**
  - no records from 2017-10-30 11:00 to 2018-01-10 11:00 LST;
  - 2019 has 4,308 temperature, 4,271 dew-point and 3,193 precipitation
    values in 8,753 rows;
  - 2020 has 5,519, 5,008 and 5,559 in 8,760 rows;
  - 2022 has 8,455 precipitation values, because of the July `M` window;
  - pressure starts in 2013 (8,521 values that year);
  - visibility and present weather are never reported, as the station is
    automatic.
- **QC status.** The glossary's R (raw, before 2013-12-10) and Q (automatic
  assessment, from 2013-12-10) are archive-level statuses. They are not
  delivered per record and are not imputed.

## Validation

### Against the ECCC bulk hourly CSVs, 2013-01 to 2025-12

The comparison used the 156 monthly Climate Data Online files
`en_climate_hourly_ON_6153301_MM-YYYY_P1H.csv` held by the 3Sigma-STR
forecasting study; they are not part of this repository. The join is on the LST
label, `time_end` - 5 h. "Equal" means exactly the same float value; nothing
needed a tolerance.

**Hours.** The CSVs have 113,952 rows, one for every hour. The store has
111,741 of those hours, and none that the CSVs lack. The API serves no record
for the other 2,211:

- 2,190 are blank in the CSVs too; 1,729 of these are the 2017-2018 outage.
- **21 have values in the CSVs:** 2015-05-25 14:00 to 2015-05-26 09:00 (20 h)
  and 2022-04-22 19:00 (1 h). They are absent from the publisher's responses
  and its counts, so they are not ingestion losses.

| Variable | Equal | Differ | Store only | CSV only | Both null |
|---|---:|---:|---:|---:|---:|
| `air_temperature` | 96,836 | 0 | 7,190 | 21 | 9,905 |
| `dew_point_temperature` | 93,931 | 0 | 7,190 | 21 | 12,810 |
| `relative_humidity` | 101,577 | 0 | 7,190 | 21 | 5,164 |
| `precipitation_amount_1h` | 87,307 | 0 | 15,381 | 310 | 10,954 |
| `wind_direction` (store / 10) | 81,305 | 0 | 5,776 | 30 | 26,841 |
| `wind_speed` | 103,793 | 659 | 7,190 | 21 | 2,289 |
| `visibility` | 0 | 0 | 0 | 0 | 113,952 |
| `station_pressure` | 104,368 | 0 | 7,190 | 21 | 2,373 |
| `humidex` | 15,180 | 0 | 1,436 | 17 | 97,319 |
| `wind_chill` | 16,086 | 0 | 756 | 0 | 97,110 |

Every difference falls into one of these classes:

- **The CSV files are blank from 2025-03-07 00:00 LST** (7,190 hours), which
  fits a download in early March 2025. For direction this class is 5,160
  hours, for humidex 1,436 and for wind chill 654.
- **The CSVs have no precipitation before 2013-12-10 16:00** (8,191 hours). In
  those hours the store's values equal HLY01 on all 8,191; 460 are non-zero.
- **The 21 hours the API does not serve** are CSV-only for every variable
  (humidex in 17 of them). Their precipitation is 0.0.
- **July 2022, most likely revised by ECCC after the CSV download.** Between
  2022-07-08 14:00 and 2022-07-20 15:00 LST:
  - 289 hours have null precipitation flagged `M` in the API, where the CSV has
    values and a blank flag. Nine of them are wet: 32.8 mm in total, 11.0 mm at
    most. HLY01 equals the CSV in all 289.
  - In 9 hours the API reports calm (speed 0, raw direction 0, stored null)
    where the CSV has 3 km/h and a direction. These are 9 of the wind-speed
    differences and 9 of the 30 CSV-only directions.
- **Calm-wind convention: 650 hours** where the CSV has 0 km/h and no
  direction, but the API has 1 km/h (267 hours) or 2 km/h (383 hours).
  - By month: 2013-01 105; 2018-07 16; 2018-08 176; 2018-09 137; 2018-10 131;
    2018-11 85.
  - In 519 of them the API gives a direction, which is therefore store-only.
  - In 131, all in 2018 and all at 1 km/h, the API reports calm.
  - All 102 wind-chill values found only in the store before the blank tail
    fall in these hours.
- **2024-10 (97 hours):** both sources flag wind speed `M`. The API gives a
  direction; the CSV leaves it blank with a blank flag.
- **Calm:** 24,226 hours are null in both. The store holds the calm marker
  (null, blank flag), and the CSV speed is 0 or the hour is in the blank tail.
- **Flags:** all ten flag columns agree on all 113,952 hours, except
  precipitation in the 289 revised July-2022 hours (store `M`, CSV blank). The
  weather text is null on both sides in every hour.

### Precipitation against `eccc_hly01_observations` over the full overlap

The comparison used the live store's HLY01 snapshot
`snap_07ef2a40d5de49f88031d65d80332550`, read only. It took element 262 rows
with an interval of exactly 1 h (173,496 rows) and joined HLY01 `time_start` to
this dataset's `time_end`, because of the one-hour HLY01 shift. The overlap
runs from 2004-04-19 05:00Z to 2025-05-16 04:00Z.

| Quantity | Hours |
|---|---:|
| Keys in both | 173,009 |
| HLY01-only keys | 487, of which 21 have values: the 21 hours the API does not serve |
| Climate-hourly-only keys | 9,404, none with a value |
| Both observed | 172,423 |
| Equal exactly | 167,700 |
| Equal after rounding HLY01 to 0.1 mm | 172,423 (all) |
| Differ | 0 |
| Observed in HLY01 only | 289: the revised July-2022 window, 9 of them wet |
| Observed in climate-hourly only | 23: 2025-05-15 01:00-23:00 LST, all 0.0, HLY01 flag `M` |
| Both missing | 274 |

The 4,723 hours that are equal only after rounding differ in the last binary
digit, because HLY01 stores integer tenths multiplied by 0.1. When the join
has no shift (`time_start` = `time_start`), only 558 non-zero hours are equal
instead of 12,027. That confirms the one-hour shift.

### Consequence for analyses built on the bulk-CSV download

Over the span in which the bulk CSVs report precipitation, [2013-12-10 16:00,
2025-03-07 00:00) LST (98,504 hours):

- the CSVs have 87,617 observed hours and this snapshot 87,307;
- the 87,307 hours both have are equal bit for bit, and rounding to 0.1 mm
  changes none of them;
- the 310 CSV-only hours are the 21 hours the API does not serve plus the 289
  revised hours.

An analysis that must reproduce numbers computed from that download cannot
take precipitation from this snapshot. HLY01 (LST label = `time_start` - 5 h,
rounded to 0.1 mm) still equals the CSV on all 87,617 hours. Under the
3Sigma-STR study's own rule, its precipitation therefore stays on HLY01, and
its other variables can come from this dataset.

### Store integrity and replay

- **`doctor --verify-all` on the scratch store:** exit 0. All 53 committed
  fragments (52 here plus the station inventory) were re-hashed: 0 failed, 0
  missing, 0 unreferenced, no staging, abandoned or running runs, and no
  absolute raw or fragment paths.
- **Re-ingest, on a copy of the scratch store:**
  - Ingesting the same manifest again printed the same snapshot ID. The store
    still held 1 committed snapshot and 1 committed run.
  - After the dataset's 52 fragments were deleted, `doctor` reported 52
    missing and exited 1. `research-store reingest
    eccc_climate_hourly_observations` then rebuilt
    `snap_bcd96b958898461283db7339ecfd4816` from `raw/`: the same ID, the same
    `committed_at`, and all 225,689 rows identical (`pandas` frame equality).
    `doctor --verify-all` was clean again.
- **Live `doctor --verify-all`** (2026-10-04 03:19:49Z to 03:20:16Z,
  read-only, after the repeated ingest below): exit 0. It printed
  `schema_version` 2, `registry_sha256`
  `88861b99aadeb4ed16b3a375a504868dfba23e0e45e9da6eaecaa7aba30e6a57`,
  `committed_fragments` 52,893, `checksums_verified` 52,893,
  `checksums_failed` 0, `missing_fragments` 0, `unreferenced_fragments` 0,
  `staging_runs` 0, `abandoned_staging_runs` 0, `running_runs` 0,
  `absolute_raw_paths` 0, `absolute_fragment_paths` 0 and
  `legacy_snapshot_directories` 0. It listed this dataset with 1 committed and
  0 superseded snapshots. There were no `problem:` lines. The only finding was a
  note about another dataset: `declared_but_empty: eccc_dly04_observations is
  declared ready but has never been ingested, so load() will raise
  LookupError`. The user also ran `doctor --verify-all` after the repeated
  ingest, with the same counts, the same registry SHA-256 and the same single
  note.
- **Live repeated ingest: passed.** The user ran reproduction step 6 on the
  live store on 2026-10-04 at 03:14:23Z (2026-10-03 23:14 EDT). This is the
  command, run from the repository root with the `research-store` installed
  in the 3Sigma-STR forecasting repository's environment:

  ```bash
  ../short-term-extreme-weather-forecasting/.venv/bin/research-store ingest eccc_climate_hourly_observations \
    ResearchDataStore/downloads/eccc_climate_hourly_observations/20260925T081258Z_6a06a98ed15f.tsv \
    --publisher-vintage 'MSC GeoMet climate-hourly (pygeoapi 0.20.0), retrieved 2026-09-25'
  ```

  - It printed `snap_79a496ff47a1448da3a09bbda6a7ce94`, the snapshot of the
    first ingest, and nothing on stderr. The first ingest's own command was
    not recorded; it was equivalent to this one.
  - Steps 2-4, run afterwards, still list 1 run, 1 snapshot and 52
    fragments of this dataset.
  - Its only catalogue write re-registered the registry: `datasets` and
    `store_meta` were updated at 03:14:22Z. That was before the
    limitations-note correction of 03:22Z (Follow-up 2), so the catalogue's
    `datasets.spec_json` still holds the pre-correction wording.

### Tests

- `python -m pytest`: 140 passed when this draft was written. The count
  includes this report's own check, `test_repository_protocol.py`, which
  requires the thirteen sections and rejects absolute workstation paths and
  raw-object paths. Final run (2026-10-04 UTC, after the live values, the
  repeated-ingest result and both registry corrections were written):
  `python -m pytest -q`, 140 passed, 0 failed.
- The registry and ingester tests cover the timing declarations, the x10
  direction scale and its 0-36 raw range, the scoped missing-value markers,
  the per-response constants, the UTC cross-check, the selection guard and
  replay.

## Known limitations

- **Station subset.** This snapshot holds one station. The collection serves
  only some ECCC climate stations (cities of 10,000 or more, Regional Basic
  Climatological Network stations, and stations with 30 or more years of
  data), and a fetch for a station it does not serve stops.
- **Revisable data.** Values are live, and ECCC revises them. Compared with the
  early-2025 bulk-CSV download, 289 July-2022 precipitation hours and 9
  July-2022 wind hours differ, and the API serves no record for 21 hours the
  CSV has. Pin `frame.attrs['snapshot_id']` in every analysis, and expect a
  refresh to change values.
- **Open windows.** This selection has none. A window that ended less than 7
  days before it was retrieved is recorded as open
  (`settled_at_retrieval` false) and can still gain hours; `fetch` requests it
  again instead of reusing its cache.
- **The HLY01 timing defect.** `eccc_hly01_observations` stores each hour one
  hour late: the total for the hour ending at H LST is keyed [H, H+1 h), not
  [H-1 h, H). Until it is corrected, join HLY01 `time_start` to this dataset's
  `time_end`.
- **Calm conventions.**
  - The publisher reports calm as direction 0, and the store keeps no
    direction.
  - In 131 hours of 2018 the publisher reports calm at 1 km/h.
  - In 650 hours the bulk CSV has 0 km/h where the API has 1-2 km/h.
  - Wind statistics computed from this dataset and from the bulk CSVs will
    therefore differ slightly.
  - The registry's limitations note now gives these full-record figures. It
    described only July 2018 until the correction of 2026-10-04.
- **Blank flags.** A blank flag is not proof of a reported value. See Quality
  flags for precipitation before 2019, visibility from 2005, two partly
  reported hours and one hour without a temperature.
- **Anemometer height and averaging window** vary by station and era and are
  not delivered per record.
- **Undocumented flags.** The collection documents only blank and `M` for
  hourly flags, and `record_flag` codes not at all. None besides blank and `M`
  occurs here. Any other code is kept verbatim and must be documented before it
  is interpreted.
- **French text.** `WEATHER_FRE_DESC` is neither stored nor checked. It is
  kept only in `raw/`.
- **QC.** Only archive-level automatic assessment is documented; there is no
  endorsed per-record scientific QC.

## Reproduction commands

Run from the repository root in an environment with the package installed
(`pip install -e .`), with `RESEARCH_DATA_ROOT` unset so the store is the
repository-local `ResearchDataStore/`. No other process may hold the catalogue,
because DuckDB refuses a writer while another process has the file open.

Acquisition. This step already ran, from 08:11:12Z to 08:12:58Z on
2026-09-25. It is the only step that uses the network: 27 windows and 54
requests.

```bash
research-store fetch eccc_climate_hourly_observations \
  --station 6153301 --start 2000-01-01 --end 2026-09-01
# wrote ResearchDataStore/downloads/eccc_climate_hourly_observations/20260925T081258Z_6a06a98ed15f.tsv
```

Re-running this fetch reuses the cached settled responses and prints the same
manifest. `--refresh` asks ECCC again and gives a new manifest whenever the
publisher has revised a window.

Ingestion and closure. Step 1 ran on the live store on 2026-09-28 at 17:41Z.
Step 6 ran on it on 2026-10-04 at 03:14Z, and steps 2-5 and 7 after it, from
03:18Z to 03:20Z (all on 2026-10-03 EDT). `research-store sql`
attaches the catalogue `READ_ONLY` to an in-memory DuckDB, and `doctor` opens it
read-only, so those steps can run while other processes read the store. Only
steps 1 and 6 write.

```bash
# 1. Ingest (offline). Prints the snapshot ID.
research-store ingest eccc_climate_hourly_observations \
  ResearchDataStore/downloads/eccc_climate_hourly_observations/20260925T081258Z_6a06a98ed15f.tsv \
  --publisher-vintage 'MSC GeoMet climate-hourly (pygeoapi 0.20.0), retrieved 2026-09-25'

# 2. Snapshot, commit time and execution identity
research-store sql "
SELECT s.snapshot_id, s.state, s.committed_at, s.run_id, r.ingester_version,
       r.registry_hash, r.dataset_digest
FROM catalog.main.snapshots AS s JOIN catalog.main.ingestion_runs AS r USING (run_id)
WHERE s.dataset_id = 'eccc_climate_hourly_observations'
ORDER BY s.committed_at"

# 3. Reconciliation per window: the runbook section 6b query
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

# 4. Fragments, rejections, and the selection record (guard, timezone inventory snapshot)
research-store sql "
WITH latest AS (
  SELECT snapshot_id, run_id FROM catalog.main.snapshots
  WHERE dataset_id = 'eccc_climate_hourly_observations' AND state = 'committed'
  ORDER BY committed_at DESC LIMIT 1
)
SELECT (SELECT count(*) FROM catalog.main.fragments JOIN latest USING (snapshot_id)) AS fragments,
       (SELECT sum(row_count) FROM catalog.main.fragments JOIN latest USING (snapshot_id)) AS fragment_rows,
       (SELECT count(*) FROM catalog.main.ingestion_rejections JOIN latest USING (run_id)) AS rejections,
       (SELECT i.details_json FROM catalog.main.ingestion_inputs AS i JOIN latest USING (run_id)
        WHERE i.input_role = 'selection_manifest') AS selection"

# 5. Non-blank flags, calm and the index rules
research-store sql "
SELECT field, flag, count(*) AS rows
FROM (UNPIVOT (SELECT COLUMNS('_quality\$|^record_flag\$') FROM eccc_climate_hourly_observations)
      ON COLUMNS(*) INTO NAME field VALUE flag)
WHERE coalesce(flag, '') <> ''
GROUP BY ALL ORDER BY field, flag"
research-store sql "
SELECT count(*) FILTER (WHERE wind_direction IS NULL AND coalesce(wind_direction_quality, '') = '') AS calm_or_blank_null,
       count(*) FILTER (WHERE wind_direction IS NULL AND coalesce(wind_direction_quality, '') = '' AND wind_speed = 0) AS calm_speed_0,
       count(*) FILTER (WHERE wind_direction IS NULL AND coalesce(wind_direction_quality, '') = '' AND wind_speed > 0) AS calm_speed_positive,
       count(*) FILTER (WHERE wind_direction IS NULL AND coalesce(wind_direction_quality, '') = '' AND wind_speed IS NULL) AS both_null_blank_flag,
       count(*) FILTER (WHERE wind_direction = 0) AS direction_zero,
       count(humidex) AS humidex_values, min(humidex) AS humidex_min,
       count(wind_chill) AS wind_chill_values, max(wind_chill) AS wind_chill_max,
       count(precipitation_amount_1h) AS precipitation_values,
       count(*) FILTER (WHERE precipitation_amount_1h > 0) AS precipitation_nonzero,
       max(precipitation_amount_1h) AS precipitation_max
FROM eccc_climate_hourly_observations"

# 6. Idempotency: the same command as step 1 must print the same snapshot ID
research-store ingest eccc_climate_hourly_observations \
  ResearchDataStore/downloads/eccc_climate_hourly_observations/20260925T081258Z_6a06a98ed15f.tsv \
  --publisher-vintage 'MSC GeoMet climate-hourly (pygeoapi 0.20.0), retrieved 2026-09-25'

# 7. Whole-store check. This re-hashes every committed fragment of every dataset, so it is slow.
research-store doctor --verify-all
research-store provenance eccc_climate_hourly_observations

# Review gate
python -m pytest
git diff --check
```

### Completing this report from the live run

The live values were filled in on 2026-10-04. The last column records the
outcome.

| Placeholder | Printed by | Expected | Live result |
|---|---|---|---|
| Live snapshot ID (Status, Snapshots) | step 1 (last line); step 2 `snapshot_id` | a new `snap_...` (IDs are random) | `snap_79a496ff47a1448da3a09bbda6a7ce94` |
| Live `committed_at` (Snapshots) | step 2 | the time of the run, in UTC | 2026-09-28 17:41:14.554001 UTC |
| Live run ID, ingester version, registry SHA-256, dataset digest (Execution identity) | step 2 | a new `run_...`; the other three equal to the scratch values above | `run_aeb9e44fd6b84699a9ae2a90d52c3b6d`; the other three identical |
| pyarrow version (Execution identity) | `python -c "import pyarrow; print(pyarrow.__version__)"` in the environment used | `25.0.1` | 25.0.1; all 52 fragment footers agree |
| Code commit (Execution identity) | `git rev-parse HEAD` | the commit the run used | `4229b00…`, with the dataset code uncommitted on top |
| Timezone inventory snapshot (Scope) | step 4, `selection` -> `timezone_inventory_snapshot_id` | the live store's `eccc_station_inventory` snapshot | `snap_a044db0435784c3fa1adde9467d25bb3` |
| Live reconciliation output (Reconciliation) | step 3 | identical to the scratch block, row for row | identical, byte for byte |
| Fragments, rows, rejections (Snapshots, Rejections) | step 4 | 52, 225,689, 0; `selection_guard` `checked`, `open_windows` 0 | 52, 225,689, 0; `checked`, 0 |
| Flag and calm counts (Quality flags) | step 5 | the counts in Quality flags, unchanged | unchanged |
| Repeated-ingest result (Validation) | step 6 | the same snapshot ID as step 1 | `snap_79a496ff47a1448da3a09bbda6a7ce94`, run by the user on 2026-10-04 03:14:23Z (2026-10-03 23:14 EDT); still 1 run, 1 snapshot and 52 fragments afterwards |
| `doctor --verify-all` output (Validation) | step 7 | exit 0; `checksums_failed` 0, `missing_fragments` 0, `unreferenced_fragments` 0, no `problem:` lines. Record `committed_fragments` and `checksums_verified` as printed; notes about other datasets are recorded as they appear. | exit 0; 52,893 committed and verified, 0 failed, 0 missing, 0 unreferenced; one note, about `eccc_dly04_observations` |
| Final test result (Validation) | `python -m pytest` | all passed | 140 passed |

If a live value differs from what is expected, stop and record the
difference as a blocker. Do not adjust the expected value to match. A
different ingester version, registry hash or dataset digest means the code
changed after this draft. Different reconciliation or flag counts mean the
live inputs differ from the scratch inputs. No live value differed.

## Follow-up

1. **Complete this report from the live run.** Done on 2026-10-04 (table
   above), the repeated ingest included. The live run used the dataset code
   as working-tree changes on top of `4229b00`; that code, the registry
   correction and this report were then committed together as `979d171` on
   the branch `feature/eccc-climate-hourly`. The branch was later pushed to
   GitHub, merged into `main` by `c4f50a9` on 2026-10-05, and deleted; `main`
   keeps both commits.
2. **Correct the registry's limitations note.** Done on 2026-10-04 at 03:22Z
   (2026-10-03 23:22 EDT). Its blank-flag figures were corrected again later
   that day, to the split and the three hours given under Quality flags. The
   note cited 4 calm-with-speed hours and 16 hours at 1-2 km/h, all in July
   2018. It now gives the full-record figures: 131 hours of calm at 1 km/h,
   and 650 hours at 1-2 km/h where the bulk CSV has 0 km/h. It also gives:
   - the July-2022 revision;
   - the 21 hours the API does not serve;
   - the 97 hours of 2024-10 with a direction and an `M`-flagged speed;
   - the 2017-10-30 to 2018-01-10 outage;
   - blank-flag nulls, which are not always calm or not-applicable;
   - the 2019-2020 coverage gaps.

   Only `documentation.limitations` of this dataset changed, both times: a
   field-by-field comparison of every dataset's `serializable()` before and
   after the second correction differs in that field alone. `Registry.digest`
   is still `88861b99…`, and `Registry.dataset_digest` is unchanged for all 11
   datasets. Each dataset digest also equals the one computed from the
   catalogue's registered `spec_json`, and the catalogue's `registry_hash` is
   `88861b99…` too. The repeated ingest re-registered the
   registry at 03:14:22Z, before the first correction, so the live
   catalogue's `datasets.spec_json` keeps the pre-correction wording until the
   next write re-registers the registry. `describe()` reads the registry in
   code, so readers see the new wording now. The notes' promise that "the
   ingestion report repeats it for the full record" is met by Validation above.
3. **Correct HLY01 timing.** This needs a registry change to element 262's slot
   placement, an ingester version bump, `research-store supersede` and a
   re-ingest (runbook §7a). Elements 263-280 share the slot and need the same
   review. After the correction, readers that label HLY01 hours by
   `time_start`, as the 3Sigma-STR study does, must switch to `time_end`.
4. **Ask ECCC** (Applied Climatology Services) three things: to confirm that
   the hourly `LOCAL_DATE` is the end of the precipitation hour, the reason
   for the July-2022 changes, and why the API serves no record for 21 hours
   that the bulk CSVs have.
5. **Set a refresh schedule.** For example, fetch quarterly with `--refresh`
   and write a new dated report for each replacement snapshot. Add September
   2026 once it has settled, that is, from 2026-10-08.
6. **Fix restore for append datasets with several snapshots.** Restoring such
   a dataset from `raw/` fails, for example `eccc_hly01_observations`. The bug
   predates this dataset and does not affect it (a replacement dataset with
   one snapshot, whose restore is shown under Validation), but HLY01's backups
   cannot be relied on until it is fixed.
