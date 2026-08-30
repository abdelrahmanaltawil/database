# Architecture

## Purpose and invariants

This store is the canonical data boundary for research code. Analysis projects
know dataset identifiers, entity identifiers, variables and time ranges; they
never know filesystem paths. The store preserves publisher bytes, semantic
conventions, snapshot identity and transitive provenance.

An observation may describe an instant or an interval. This distinction matters
for hourly accumulations and daily statistics, especially across daylight-saving
time changes. Every time-series dataset therefore declares its temporal kind,
native frequency, source timezone and timestamp labelling convention.

## Layers and dependency rule

```mermaid
flowchart TD
    CLI[CLI composition root] --> ING[Ingestion adapters]
    CLI --> DER[Derived producers]
    CLI --> API[Access API]
    ING --> FND[Foundation]
    DER --> FND
    API --> FND
    FND --> STORE[Raw, Parquet and DuckDB]
```

The foundation owns configuration, the ECCC registry, optional local overlay
resolution, schemas, conventions, partitioning, hashing, the catalogue,
checkpoints and the only Parquet writer.
An ingester parses one external format and emits canonical Arrow chunks. A
derived producer emits the same chunks but records parent snapshots and its
query. The access layer reads committed catalogue entries only.

`tests/test_architecture.py` parses the Python syntax tree and fails when:

- the foundation imports access, ingestion or derived code;
- an ingestion module imports another ingestion module or the access layer;
- a public dataset is declared outside `foundation/registry.py`; or
- Parquet is written outside `foundation/writer.py`.

The CLI is the composition root and is the only layer allowed to import the
independent application layers together.

## On-disk layout

```text
$RESEARCH_DATA_ROOT/
├── raw/
│   ├── objects/sha256/<prefix>/<digest>
│   └── source-manifests/<digest>.json
├── warehouse/
│   ├── external/<dataset>/snapshots/<snapshot>/...
│   └── derived/<dataset>/snapshots/<snapshot>/...
├── staging/runs/<run>/fragments/...
├── catalog/store.duckdb
└── locks/
```

`raw` and `catalog` are durable backup material. `warehouse` and `staging` are
rebuildable. Raw objects are addressed by SHA-256 and made read-only on
filesystems that support it. Original filenames, URI, download time and
publisher vintage remain in `source_aliases`; they are not used as identity.

The environment variable is `RESEARCH_DATA_ROOT`. The fallback chain is:

1. an explicit `store=` argument or CLI `--store` option;
2. `RESEARCH_DATA_ROOT`;
3. the operating system's per-user application-data directory.

Writes warn when the fallback is used. All resolutions warn for common
cloud-synchronization directory names.

## Physical and logical observations

The registry permits two physical representations:

- **long**, for sparse element/value sources; and
- **wide**, retained as a validated foundation capability for future documented
  sources.

`load()` exposes one logical form: entity and interval keys followed by
variable-named columns. Different quantities never share a generic `value`
column in the returned dataframe. Long Parquet is pivoted only after entity,
time and variable filtering.

Every variable has one quantity, unit, Arrow type and optional quality field in
the registry. Numeric research values remain float64. Entity identifiers must
already be strings; numeric autocasting is rejected.

## Relational model

DuckDB holds the small relational catalogue. Parquet observations reference it
with stable identifiers.

| Relationship | Purpose |
|---|---|
| `variables.dataset_id → datasets.dataset_id` | One canonical schema declaration |
| `source_aliases.source_id → source_files.source_id` | Names and vintages for immutable bytes |
| `ingestion_runs.dataset_id/source_id` | Idempotency is dataset plus source, not hash alone |
| `fragments.snapshot_id/source_id` | Exact files and sources behind a result |
| `derivation_edges.child → parent` | Transitive provenance for derived tables |
| `snapshot_parents.child → parent` | Cumulative append snapshots |
| `entity_match_candidates` | Cross-source matches plus evidence and decision state |

The large Parquet relations cannot have database-enforced foreign keys. The
foundation writer instead validates their schema and injects catalogue-owned
source and producer-run identifiers before publication. SQL clients attach the
catalogue read-only and expose only logical dataset views.

Climate observations and `eccc_station_inventory` deliberately use the same
string `entity_id`: the ECCC Climate ID. Numeric and alphanumeric values are
both preserved, so an observation can be joined directly to its station
metadata.

ECCC HLY products are source-specific datasets rather than anonymous weather
families. `eccc_hly01_observations` is long-form and supports multiple registered
elements with per-element scale, unit and interval placement. This allows the
same relation to hold hourly precipitation, quarter-hour precipitation, gauge
weight, near-gauge wind and snow depth without losing the publisher element
code. Additional HLY01 variables such as temperature can be registered without
creating another physical model. `eccc_hly03_observations` remains separate
because its source product and timestamp semantics differ.

The timezone polygon data package is pinned. Its installed version is stored in
each station row, so a future boundary-data update is an explicit ingester and
registry change rather than a silent reinterpretation of historical timestamps.

## Source configuration

Public publisher formats and canonical meanings are declared in
`foundation/registry.py`. `RESEARCH_STORE_PRIVATE_REGISTRY` may optionally point
to a local JSON overlay. An overlay can refine an existing dataset but cannot
invent a new public dataset identifier.

Resolved values participate in `registry_hash`. Consequently, a mapping or unit
change creates a different ingestion identity even when the raw file is
unchanged.

## Idempotency, checkpoints and publication

An external run is uniquely identified by:

```text
(dataset_id, source_sha256, ingester_version, registry_hash)
```

A derived run uses the dataset, ordered parent snapshots, query, producer
version and registry hash. Each output chunk has a deterministic key and a
catalogue checkpoint. Restarting retains completed chunks.

All fragments for a run are written below one staging directory. Publication:

1. validates every fragment and duplicate key across the complete logical
   snapshot;
2. atomically renames the staging fragment directory into its final snapshot;
3. commits fragment rows, lineage and snapshot state in one DuckDB transaction.

A crash before step 2 leaves restartable staging. A crash between steps 2 and 3
leaves an unreferenced directory that readers cannot see; rerunning completes
the catalogue transaction. Readers never glob staging or arbitrary output
directories.

Append datasets reference the preceding committed snapshot as their manifest
parent. Readers recursively resolve those parents without copying old fragment
rows into every annual snapshot. Duplicate validation scans new fragments and
only prior fragments in overlapping year/entity-bucket partitions. Replacement
datasets create an independent snapshot. Old snapshots remain queryable by ID.

## Partitioning and query cost

Time series are partitioned by UTC year and a stable BLAKE2 entity bucket. Rows
are sorted and Parquet statistics are enabled. `load()` computes the relevant
year and bucket before asking the catalogue for fragment paths, so a query for
one entity and one year does not open unrelated Parquet files. It then projects
only requested variable columns.

For `B` reasonably balanced buckets, the candidate row population is expected
to be approximately one `year/B` slice, but this is not a performance claim.
Actual skew, compressed bytes and elapsed time must be measured after sample
ingestion:

```bash
research-store benchmark DATASET --entity ENTITY --year 2024 --variable VARIABLE
```

The command reports candidate fragment count, compressed bytes, result rows and
wall time. Bucket counts and row-group sizes must be changed only using those
measurements. No size or timing estimate has been invented from filenames.

## Silent-failure controls

| Failure | Control |
|---|---|
| Different quantities in one value column | Logical output uses separately named variable columns and registered units |
| Numeric sentinels | Source adapter applies registered rules before Arrow conversion |
| Sentinel meaning changes | Inclusive-start/exclusive-end era rules; overlaps and uncovered eras fail |
| Numeric-looking identifiers | String schema required before publication |
| Longitude conventions | Explicit conversion to signed EPSG:4326; unknown convention raises |
| Local/DST/UTC confusion | ECCC station coordinates resolve to pinned IANA zones; explicit TZif standard types are used instead of Python's inferred `dst()` value |
| Different sampling frequencies | Native frequency remains registry metadata; no ingestion resampling |
| float64 narrowed to float32 | Writer schema validation rejects float32 |
| Cloud-synchronized root | Resolver and `doctor` warn |
| Empty/partial producer output | Empty chunks fail and only committed catalogue fragments are readable |
| Duplicate observations | Keys are checked within chunks and across the full published snapshot |

## Current scope

The active registry contains only the ready ECCC station inventory and
HLY01/HLY03 declarations. Hydrometric, reanalysis, and offshore SCADA sources
are outside the current project scope. New documented ECCC climate elements,
including temperature, belong in the existing HLY product declaration with
their source code, unit, scale, timing, and quality semantics.
