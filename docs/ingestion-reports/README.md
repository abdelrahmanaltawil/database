# Ingestion reports

Production ingestion reports live here and are required by the
[production ingestion protocol](../ingestion-protocol.md). Name each report
`YYYY-MM-DD-short-name.md` and start from this template.

```markdown
# Ingestion report: DATASET OR BATCH

## Status

## Scope and source inventory

## Authoritative documentation

## Metadata crosswalk

## Execution identity

## Snapshots

## Reconciliation

## Rejections and recovery

## Quality flags and data-quality interpretation

## Validation

## Known limitations

## Reproduction commands

## Follow-up
```

Do not include raw data, restricted metadata, sensitive source hashes or
absolute local paths. Reports should contain enough catalog SQL and aggregate
counts to reproduce their claims without depending on one workstation layout.

