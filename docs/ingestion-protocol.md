# Production ingestion protocol

This is the repository's required closure protocol for every production
ingestion. An ingestion is not complete merely because a snapshot committed.
It is complete only when the metadata is supported by authoritative evidence,
every supplied source record is accounted for, and a versioned ingestion report
has passed review.

## 1. Metadata preflight

Before ingesting source bytes, record and reconcile:

- dataset and entity identifiers;
- physical record structure, encoding and source elements or columns;
- canonical variable, quantity, unit, numeric scale and missing sentinels;
- instant or interval semantics, slot labels, source timezone and UTC conversion;
- quality-control status, flag meanings and instrument limitations;
- append or replacement snapshot behavior; and
- authoritative documentation title, revision/date and relevant sections.

Put transformative declarations in `foundation/registry.py` and test them.
Put explanatory provenance, QC interpretation, flag meanings and limitations in
`DatasetSpec.documentation`. When sources conflict, record the conflict and use
the most specific evidence for the delivered files; do not silently combine
incompatible claims.

A documentation-only correction may update `DocumentationSpec` without changing
`registry_hash`. Any change that can alter parsed values, timestamps, units,
missingness or analytical meaning must change a transformative registry field
and, when parser behavior changes, the ingester version. Determine whether old
fragments require a controlled replacement before publishing new data.

## 2. Preserve source identity

Ingest the original source file, not an edited copy. The store must retain its
SHA-256 object, alias, source URI when available, publisher vintage and fetch
time. Never commit raw or restricted data, source hashes that are sensitive, or
absolute workstation paths to the Git repository.

## 3. Execute and inspect

Run the ingestion idempotently. Inspect committed and failed runs, snapshot
state, source aliases, fragments and `ingestion_rejections`. A quarantined line
must retain a source identifier, locator, reason and raw hash; partial recovery
must also record the recovered-record count.

No staging snapshot may remain when the batch is declared complete. Failed
audit attempts may remain in the catalog when they are documented and no
reader-visible snapshot depends on them.

## 4. Reconcile exactly

For every supplied file or batch, show an integer equality from physical source
records to accepted, recovered and quarantined records. Avoid double-counting a
malformed physical line whose valid embedded record was recovered. Also verify:

- committed source count equals the intended supplied source inventory;
- accepted source records expand to the expected observation-row count;
- every rejected physical record has one documented reason;
- unexpected flags and sentinels are counted and preserved; and
- the latest committed snapshot contains the intended cumulative manifest.

If exact reconciliation is impossible, status is `incomplete` and the mismatch
is a blocker, not a rounding error.

## 5. Write the ingestion report

Create `docs/ingestion-reports/YYYY-MM-DD-short-name.md` for each production
batch or material backfill. Use the template in
`docs/ingestion-reports/README.md`. Reports are evidence: do not rewrite a past
result when a later append changes it. Create a new dated report, or clearly
label a correction with its reason and date.

The report must contain these sections:

- Status
- Scope and source inventory
- Authoritative documentation
- Metadata crosswalk
- Execution identity
- Snapshots
- Reconciliation
- Rejections and recovery
- Quality flags and data-quality interpretation
- Validation
- Known limitations
- Reproduction commands
- Follow-up

Status must distinguish source/accounting completeness from scientific data
quality. “Complete” means that all intended supplied files and records are
cataloged or quarantined; it never means that every measurement is valid.

## 6. Review gate

The reviewer checks the report, registry diff and tests, then runs:

```bash
research-store doctor
python -m pytest
git diff --check
```

The report may be merged only when its required sections are present, the
reconciliation closes, metadata tests pass, and raw data remains outside Git.

