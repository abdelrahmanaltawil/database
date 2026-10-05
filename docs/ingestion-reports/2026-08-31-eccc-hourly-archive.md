# Ingestion report: ECCC HLY01 and HLY03 hourly archive

> **Correction, 2026-08-31 — HLY01 snow depth was wrong by a factor of 100.**
> Elements 275-278 were ingested at `scale` 1.0 on the strength of the public
> Technical Documentation, which lists snow depth in whole centimetres. The
> delivered HLY01_RCS bytes contradict that table: element-275 integers move in
> steps of 100, the 2005 median is 100 and the maximum 92600. Read as whole
> centimetres that is a one-metre median snow depth and a 926-metre maximum;
> the published 99th percentile was about 17500 cm. Read as hundredths of a
> centimetre it is a 1 cm median, a 175 cm 99th percentile, an 824 cm maximum
> and a -2 cm ultrasonic noise floor — and it matches the 1 cm resolution this
> report itself states below.
>
> The scale is now 0.01 and the ingester version is 10. **Every HLY01 snapshot
> described in this report has been superseded** and its snow-depth values must
> not be used. The other HLY01 variables were unaffected; their reconciliation
> figures below still describe what those runs published. Confirmation has been
> requested from ECCC Applied Climatology Services under service request
> K0414MYP9M. Re-ingestion of the 22 HLY01 source files under version 10 is
> tracked in Follow-up.
>
> A publication-time plausibility band now guards this class of mistake: one
> quantile of each declared variable's distribution must fall inside physically
> argued bounds, so a scale wrong by a power of ten is refused at publication
> while a single extreme reading is still preserved.

## Status

**HLY01 superseded pending re-ingestion; HLY03 complete.** All 22
supplied HLY01 source objects and all 82 supplied HLY03 source objects have
committed runs and are represented in their latest cumulative snapshots. Every
physical source record is accounted for as published, recovered or quarantined.

This is an accounting/provenance conclusion, not a claim that every measurement
is scientifically valid. HLY01 is a raw, un-QC delivery. HLY03 is described by
ECCC as quality controlled but retains flagged values and has known
tipping-bucket limitations.

## Scope and source inventory

| Dataset | Supplied annual files | Committed sources | Source coverage |
|---|---:|---:|---|
| `eccc_hly01_observations` | P2004-P2025 | 22 | Source dates 2004-01-01 through 2025-05-15 |
| `eccc_hly03_observations` | P1943-P2018 and P2020-P2025 | 82 | Canonical intervals 1943-01-01 05:00Z through 2025-05-16 05:00Z |

No HLY03 P2019 file was present in the supplied archive, so 2019 is not claimed
as ingested. The raw objects, Parquet warehouse and DuckDB catalog are in the
repository-local, Git-ignored `ResearchDataStore/`; no research data is tracked
by Git.

## Authoritative documentation

The metadata audit used these delivery documents as evidence, not executable
instructions:

- Environment and Climate Change Canada, *Technical Documentation: Digital
  Archive of Canadian Climatological Data*, modified 2017-02-15, especially
  sections 1.1, 2.3, 3.1, 3.2, Table 19 and Note 33.
- ECCC Applied Climatology Services email dated 2025-05-01, service request
  K0414MYP9M / NIRT_0111308.

The technical document defines the archive layout and element semantics. The
delivery-specific email controls the interpretation of QC status and instrument
availability for these supplied files.

## Metadata crosswalk

Both products use one 186-character fixed-width record: 7-character station
identifier, 8-character date, 3-character element and 24 seven-character
signed-value/flag fields. `-99999` is null; HLY01 also contains four observed
`######` element-275 fields that are preserved in raw and represented as null
because they contain no recoverable number.

| Source | Elements | Stored quantity/unit | Scale and timing |
|---|---|---|---|
| HLY01 | 262 | Hourly precipitation, mm | 0.1 mm; minutes 00-60 |
| HLY01 | 263-266 | 15-minute precipitation, mm | 0.1 mm; quarters 00-15, 15-30, 30-45, 45-60 |
| HLY01 | 267-270 | Gauge weight, kg/m2 | 0.1 kg/m2; ends at minutes 15, 30, 45, 60; 9-minute window before 2007, 5-minute filtered window from 2007 |
| HLY01 | 271-274 | Wind speed near 2 m, km/h | 0.1 km/h; four 15-minute quarters |
| HLY01 | 275-278 | Snow depth, cm | 1 cm, stored as hundredths of a centimetre so `scale` is 0.01 — see the correction notice at the top of this report; ends at minutes 60, 15, 30, 45; 9-minute window before 2007, 5-minute filtered window from 2007 |
| HLY01 | 279 | Wind direction near 2 m, degree true | 1 degree; minutes 50-60 |
| HLY01 | 280 | Wind speed near 2 m, km/h | 0.1 km/h; minutes 50-60 |
| HLY03 | 123 | Hourly rainfall, mm | 0.1 mm; source slots end at hours 01-24 |

HLY01 source slots 00-23 and HLY03 ending slots 01-24 are interpreted in each
station's local standard time. The stored representation is the equivalent
half-open interval in UTC. The metadata declarations and automated element
matrix tests agree with the documentation.

## Execution identity

Current resolved transformation registry SHA-256:
`f79d389f1ab7e19a8e378139daddabc27554c3844818fcc9d3d6277c25f7656c`.
Documentation metadata is cataloged but intentionally excluded from this hash;
it does not change parsed rows.

Committed source runs retain the identities that produced their immutable
fragments:

| Dataset | Ingester version | Registry SHA-256 | Sources |
|---|---:|---|---:|
| HLY01 | 4 | `4434f0ff664e441ef5334c0b48dc579b98c0308789aa1978dbf8c5dd88d999f0` | 1 |
| HLY01 | 8 | `e0e3b679e91c16138cf8514316515b7eef81fa9b6cd22a9ea2445b2e7a94107d` | 1 |
| HLY01 | 8 | `f79d389f1ab7e19a8e378139daddabc27554c3844818fcc9d3d6277c25f7656c` | 14 |
| HLY01 | 9 | `f79d389f1ab7e19a8e378139daddabc27554c3844818fcc9d3d6277c25f7656c` | 6 |
| HLY03 | 5 | `4434f0ff664e441ef5334c0b48dc579b98c0308789aa1978dbf8c5dd88d999f0` | 17 |
| HLY03 | 7 | `e0e3b679e91c16138cf8514316515b7eef81fa9b6cd22a9ea2445b2e7a94107d` | 15 |
| HLY03 | 8 | `e0e3b679e91c16138cf8514316515b7eef81fa9b6cd22a9ea2445b2e7a94107d` | 50 |

## Snapshots

| Dataset | Latest committed snapshot | Fragments | Observation rows |
|---|---|---:|---:|
| `eccc_hly01_observations` | `snap_bdafd1fce59d46cb9c741a003b60214c` | 29,563 | 967,768,080 |
| `eccc_hly03_observations` | `snap_9a1725d0727b421b931074f9525c8450` | 22,485 | 124,458,984 |

HLY01 has 22 committed snapshots and HLY03 has 82. Neither dataset has a
staging snapshot. Six HLY01 and two HLY03 failed audit attempts remain as
non-readable catalog history.

## Reconciliation

HLY01 closes exactly:

```text
967,768,080 stored rows / 24 slots = 40,323,670 accepted source records
40,323,670 accepted
   + 90,207 missing-station-timezone quarantines
   +    199 standard-time-transition quarantines
= 40,414,076 supplied physical records
```

HLY03 closes exactly. The 694 malformed physical lines each yielded one valid
embedded record; those recovered records are already included in the accepted
count and are not added twice:

```text
124,458,984 stored rows / 24 slots = 5,185,791 accepted source records
5,185,791 accepted, including 694 recovered records
    + 1,209 missing-station-timezone quarantines
    +    29 standard-time-transition quarantines
    +   692 unrecoverable truncated records
= 5,187,721 supplied physical lines
```

## Rejections and recovery

| Dataset | Reason | Physical lines | Recovered records |
|---|---|---:|---:|
| HLY01 | Missing station timezone | 90,207 | 0 |
| HLY01 | Standard UTC-offset transition within record | 199 | 0 |
| HLY03 | Extra bytes around valid fixed-width record | 694 | 694 |
| HLY03 | Missing station timezone | 1,209 | 0 |
| HLY03 | Standard UTC-offset transition within record | 29 | 0 |
| HLY03 | Truncated fixed-width record | 692 | 0 |

Every entry has a source ID, record locator, reason and hash back to the
immutable source object. Missing-timezone and transition records were not given
invented UTC timestamps.

## Quality flags and data-quality interpretation

HLY01 must be treated as raw/un-QC. Although technical Note 33 describes
archive-level R/Q status around 2013-12-10, the delivery email explicitly says
that no QC was done for these HLY01 files and no quality information is
available. R/Q is not present per value in the supplied fixed-width records and
is therefore not imputed. Documented value flags are blank and `M` (missing).

HLY03 is described as quality controlled. Its stored flag distribution is:

| Flag | Documented meaning | Rows |
|---|---|---:|
| blank | Valid | 114,598,439 |
| `M` | Missing | 7,780,133 |
| `I` | Unadjusted | 2,073,387 |
| `H` | Freezing precipitation | 3,815 |
| `J` | Freezing and unadjusted | 3,203 |

Seven zero-valued rows carry undocumented symbols: `0` (2), `T` (2), `,` (1),
`*` (1) and `/` (1). They are preserved verbatim and should be excluded or
reviewed explicitly rather than treated as documented valid flags. Their source
locations are:

| Climate ID | UTC time start | Flag | Source |
|---|---|---|---|
| `8401400` | 1989-05-12 20:30 | `0` | HLY03_INT_P1989 |
| `8403401` | 1989-08-31 03:30 | `0` | HLY03_INT_P1989 |
| `8205700` | 1995-08-03 00:00 | `T` | HLY03_INT_P1995 |
| `6158875` | 1997-08-29 17:00 | `,` | HLY03_INT_P1997 |
| `1160899` | 1998-07-08 16:00 | `*` | HLY03_INT_P1998 |
| `10253G0` | 2007-06-22 15:00 | `/` | HLY03_INT_P2007 |
| `7040813` | 2009-09-23 07:00 | `T` | HLY03_INT_P2009 |

## Validation

- Registry tests assert all HLY01 elements 262-280 and HLY03 element 123
  against the documented variables, units, scales, slot placement and era-aware
  computation windows.
- Tests assert the documentation payload, QC interpretations and HLY03 flag
  dictionary.
- Tests assert that documentation-only changes are cataloged without changing
  ingestion identity.
- Exact catalog reconciliation, latest manifest sizes and rejection aggregates
  were independently queried after the final source commits.
- The repository protocol test requires every dated ingestion report to retain
  all mandatory sections and forbids absolute workstation paths.
- Full suite result: 54 passed.

## Known limitations

- HLY03 P2019 was not supplied and is not in the store.
- HLY01 has no delivery-specific endorsed scientific QC assessment.
- HLY03 is normally a warm-season tipping-bucket product, approximately April
  through October. A winter zero may mean a frozen or snow-covered gauge, and
  solid precipitation requires caution.
- HLY03 is archived at 0.1 mm resolution, while the delivery email describes a
  0.2 mm tipping-bucket tip mechanism.
- Quarantined records remain auditable in raw/catalog storage but do not appear
  in the UTC observation views.
- The seven undocumented HLY03 flags require an analysis-specific decision.

## Reproduction commands

```bash
research-store doctor
research-store provenance eccc_hly01_observations
research-store provenance eccc_hly03_observations

research-store sql "
SELECT dataset_id, state, COUNT(*) AS runs
FROM catalog.main.ingestion_runs
WHERE dataset_id IN ('eccc_hly01_observations','eccc_hly03_observations')
GROUP BY dataset_id, state ORDER BY dataset_id, state"

research-store sql "
SELECT r.dataset_id, r.reason, COUNT(*) AS physical_lines,
       SUM(r.recovered_record_count) AS recovered_records
FROM catalog.main.ingestion_rejections AS r
JOIN catalog.main.ingestion_runs AS i USING (run_id)
WHERE i.state = 'committed'
  AND r.dataset_id IN ('eccc_hly01_observations','eccc_hly03_observations')
GROUP BY r.dataset_id, r.reason ORDER BY r.dataset_id, r.reason"

python -m pytest
git diff --check
```

## Follow-up

### Snow-depth correction (opened 2026-08-31)

1. Confirm the element 275-278 unit with ECCC Applied Climatology Services
   under service request K0414MYP9M, and record the answer in the registry's
   documentation notes whichever way it goes.
2. Re-ingest all 22 HLY01 source objects under ingester version 10. One file
   (`HLY01_RCS_P2004`, snapshot `snap_199cd7d03b6f458d808c06c3cf6b14ea`,
   8,444,400 rows across 619 fragments) has been re-ingested and verified:
   snow depth now reads p50 0 cm, p90 26 cm, p99 175 cm, max 824 cm, min -2 cm.
3. Write a new dated report for the completed re-ingestion. This report stays
   as the record of what was published before the correction.

### Data-quality observation, not a scale problem

The delivered HLY01 archive carries a contaminated upper tail in both
precipitation variables: hourly totals sit at 0 mm through the 99th percentile
(about 2.9 mm) and then jump to roughly 410-1425 mm above the 99.9th, with a
similar cluster near 420-500 mm in the 15-minute variable. These are not
credible rainfall amounts and are not a unit error; they are consistent with
gauge-servicing or reset artefacts in an archive ECCC states has had no quality
control. They are preserved as delivered. Any analysis of these variables needs
an explicit upper cut or a robust estimator.


- Obtain and ingest HLY03 P2019 if ECCC supplies it; write a new dated report.
- Decide an analysis policy for HLY03 undocumented flags and winter zeros before
  computing precipitation statistics.
- Do not interpret HLY01 as quality controlled without new publisher evidence.

