# Ingestion report: ECCC corrected discharge unit values

## Status

Complete for source accounting and publication. The selected publisher records
are fully reconciled to one committed replacement snapshot. This status does
not imply that the preliminary unit values are scientifically final or suitable
for every analysis.

## Scope and source inventory

- Dataset: `hydrometric_discharge_unit_corrected`.
- Publisher collection snapshot: 2026-08-31.
- Publisher manifest: 2,001 corrected discharge `.csv.xz` files.
- Run selection: `STATIONS.DRAINAGE_AREA_GROSS <= 10 km2` using HYDAT
  2026-07-17.
- Selected observation sources: 54 files for 54 unique stations, covering gross
  drainage areas from 0.53 to 9.99 km2.
- Selected compressed bytes: 244,512,056.
- Other immutable inputs: one collection manifest and one HYDAT SQLite file.
- Excluded manifest rows: 1,867 above the threshold, 42 unmatched to HYDAT and
  38 matched stations with null gross drainage area. The four groups reconcile
  exactly: `54 + 1,867 + 42 + 38 = 2,001`.
- No raw unit-value product was supplied or ingested.

The drainage-area threshold is an ingestion-run parameter. It is deliberately
absent from the dataset identifier and can be changed or omitted in a later
replacement ingestion.

## Authoritative documentation

- Environment and Climate Change Canada, [Unit Value Data Disclaimer](https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/UnitValueData/UnitValueData_Disclaimer.pdf).
- Environment and Climate Change Canada, [HYDAT Database Definition](https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf),
  especially `STATIONS.DRAINAGE_AREA_GROSS`.
- Environment and Climate Change Canada, [HYDAT release notes, 2026-07-17](https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_ReleaseNotes_20260717_EN.pdf).
- Environment and Climate Change Canada, [corrected discharge unit-value directory](https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/UnitValueData/Discharge/corrected/),
  captured in the local manifest on 2026-08-31.
- AQUARIUS metadata embedded in every selected publisher file, including units,
  parameter, interpolation type, corrected-signal status and UTC timestamp.

## Metadata crosswalk

| Source field or evidence | Canonical field or action |
|---|---|
| Filename and `Time-series identifier` | String `entity_id` / HYDAT station number |
| `ISO 8601 UTC` | `time_start`, an instantaneous UTC timestamp |
| `Value`; metadata `m^3/s`, `Discharge` | Float64 `discharge`, unit `m3/s` |
| `Approval Level` | String annotation `approval_level` |
| `Grade` | String annotation `grade`; codes are not filtered at ingestion |
| One or more trailing `Qualifiers` fields | Ordered JSON string array `qualifiers` |
| HYDAT `STATIONS.DRAINAGE_AREA_GROSS` | Optional run selection; null or unmatched stations excluded when active |

The redundant fixed-offset local timestamp is not republished because the
explicit UTC timestamp is canonical. Native frequency is unset: station cadence
varies and can change within one record.

## Execution identity

- Registry SHA-256:
  `c04c1a95a0923853fa2fdd381db6cc2d2213810f4e9af612a240d50db0ba8dd0`.
- Successful producer identity:
  `2+selection.e608e4aa4701595dce48733b193e5b31587fbc8a1d3d514f701c30023f93fe0c`.
- Successful run:
  `run_b28aa1e82b2c418abcc4f5697d5c037d`.
- Publisher vintage: corrected snapshot 2026-08-31; HYDAT 2026-07-17.
- Fetch timestamp recorded for this local collection: 2026-08-31T04:35:12Z.
- Catalogue inputs: 54 observation sources, one publisher manifest and one
  selection-metadata source.

The selection fingerprint covers the manifest, HYDAT bytes, threshold, selected
station/area pairs and all selected member hashes. Source hashes are retained in
the local catalogue and intentionally omitted from this version-controlled
report.

## Snapshots

- Committed replacement snapshot:
  `snap_1ccbb30942604e50b542dd29007cc54f`.
- State: committed in both `snapshots` and `ingestion_runs`.
- Fragments: 791.
- Fragment row-count total: 40,236,172.
- On-disk Parquet snapshot size: approximately 533 MiB.
- The successful run has no remaining staging directory.

## Reconciliation

An independent CSV-reader pass over all 54 compressed publisher members found
40,236,172 physical data records. Catalogue fragment counts and a logical view
scan both found 40,236,172 observations:

```text
40,236,172 physical CSV records
= 40,236,172 accepted observations
+          0 quarantined records
```

The snapshot contains 54 distinct entities and 54 distinct row-level source
identifiers. Its UTC time coverage is 2011-01-01T00:00:00Z through
2026-05-07T23:55:00Z. There are no null discharge values and no duplicate
`(entity_id, time_start)` keys.

## Rejections and recovery

The committed run has zero `ingestion_rejections`.

The first preflight run, producer version 1, stopped on station `01AK006`
because AQUARIUS emitted more trailing qualifier fields than the single
`Qualifiers` header suggests. Inspection across the full selection found
278,907 such records, with at most three qualifier fields. Producer version 2
retains every qualifier in order as a JSON string array; the independent pass
found zero short or missing-field records. The version-1 run remains marked
failed in the catalogue, and its 6.4 MiB checkpoint was moved from `staging` to
the local `failed-runs` audit area. It has no reader-visible snapshot.

## Quality flags and data-quality interpretation

No quality-based row filtering was applied. All source annotations were
preserved.

| Grade | Publisher meaning | Rows |
|---:|---|---:|
| -2 | Unusable | 564,164 |
| -1 | Unspecified | 32,400,709 |
| 0 | Undefined | 360 |
| 10 | Ice | 4,207,640 |
| 20 | Estimated | 2,988,322 |
| 30 | Partial day | 14,257 |
| 40 | Dry | 60,626 |
| 50 | Revised | 94 |

Approval-level counts are 36,596,738 Approved, 1,731,402 Preliminary, 855,933
Reviewed, 531,807 Ready for Approval and 520,292 Checked. A total of 1,551,946
rows have at least one qualifier, represented by 33 distinct qualifier arrays.

These fields are analysis filters, not ingestion exclusions. In particular,
grade -2 remains present and must be excluded explicitly when an analysis calls
for usable values only.

## Validation

- `python -m pytest`: 58 tests passed before production execution.
- `research-store doctor`: local non-cloud path, catalogue present and current
  registry hash reported.
- XZ integrity had previously passed for all 2,001 corrected collection files.
- Publisher headers were verified per selected file: corrected signal,
  Discharge, `m^3/s`, instantaneous values and matching station identifier.
- Physical-to-canonical count equality: passed.
- Duplicate observation-key scan: zero.
- Null discharge scan: zero.
- Qualifier JSON validity scan: zero invalid rows.
- Selected observation-source lineage: 54 of 54.
- Committed-run rejection count: zero.
- Idempotency rerun returned the same snapshot identifier.
- `git diff --check`: passed during implementation validation.

## Known limitations

- ECCC describes unit values as preliminary; values and annotations may change
  in later publisher snapshots.
- This snapshot contains only the current `<= 10 km2` gross-area selection. It
  is not a complete national corrected-unit-value snapshot.
- Stations unmatched to HYDAT or lacking gross drainage area were excluded from
  this filtered run rather than assigned an inferred value.
- Gross drainage area was used because it represents the total physical basin
  and is much more complete for these stations than effective drainage area.
- Sampling cadence is not uniform, and no ingestion resampling was performed.
- HYDAT daily flows and levels are separate products and are not part of this
  snapshot.

## Reproduction commands

From the repository root, with the downloaded snapshot and HYDAT paths adapted
to the local machine:

```bash
research-store ingest hydrometric_discharge_unit_corrected \
  ResearchDataStore/downloads/eccc_hydrometrics/UnitValueData/Discharge/corrected/snapshot-2026-08-31/corrected_files.tsv \
  --station-metadata ResearchDataStore/downloads/eccc_hydrometrics/HYDAT/2026-07-17/Hydat.sqlite3 \
  --max-drainage-area-km2 10 \
  --source-uri 'https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/UnitValueData/Discharge/corrected/' \
  --publisher-vintage 'corrected snapshot 2026-08-31; HYDAT 2026-07-17' \
  --fetched-at '2026-08-31T04:35:12Z'

research-store provenance hydrometric_discharge_unit_corrected \
  --snapshot snap_1ccbb30942604e50b542dd29007cc54f
```

The command is idempotent for the same manifest, HYDAT file, threshold and
selected member bytes.

## Follow-up

- For a future national replacement snapshot, rerun without
  `--max-drainage-area-km2`; do not create another dataset identifier.
- Apply analysis-specific rules to grade, approval level and qualifiers in
  queries or derived datasets, leaving the external snapshot unchanged.
- Resolve HYDAT daily-time semantics separately before enabling the provisional
  daily-flow and daily-level dataset declarations.
