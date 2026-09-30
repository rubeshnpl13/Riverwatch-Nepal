# ADR: Separate Data Quality Evaluation from Source Data

## Status

Accepted

## Context

RiverWatch ingests river and station data originating from BIPAD/DHM.

The upstream datasets contain conditions such as stale observations, missing measurements, incomplete station metadata, unusual water-level values, and threshold inconsistencies.

Automatically correcting or deleting these values during ingestion or Spark transformation would make it difficult to distinguish source data from RiverWatch interpretations and could destroy useful lineage.

RiverWatch also needs to distinguish successful API ingestion from operationally healthy current data. An API request can succeed while much of the returned data is stale or incomplete.

## Decision

RiverWatch will preserve upstream values in the raw and processed data layers and implement quality evaluation as a separate layer.

Quality evaluation will:

1. preserve the source row;
2. attach machine-readable warning and failure reasons;
3. distinguish `pass`, `warn`, and `fail`;
4. evaluate station and observation rows independently;
5. detect duplicate identities within processing runs;
6. calculate current-data freshness and station coverage separately from row validity;
7. aggregate row-level results into dataset-level summaries;
8. write one immutable JSON quality report for each processing run.

Historical observations will not be classified as stale merely because they are old.

Current-data freshness is evaluated only where current data is expected.

RiverWatch quality statuses are internal data-quality classifications and must not be presented as official flood warnings.

## Consequences

### Positive

- Raw and processed source values remain auditable.
- Suspicious data can be investigated without being silently discarded.
- Data consumers can distinguish clean, warned, and failed rows.
- Successful ingestion can be distinguished from fresh and complete current data.
- Quality trends can later be monitored across runs.
- Quality reports preserve lineage with their processing run.

### Trade-offs

- Consumers must decide how to handle warned rows.
- Some suspicious values remain in processed datasets.
- Quality rules will require future calibration as source behavior becomes better understood.
- Warning counts can overlap because one row may trigger multiple rules.

## Alternatives considered

### Remove suspicious rows

Rejected because this would hide upstream data problems and reduce auditability.

### Correct suspicious values during transformation

Rejected because RiverWatch does not currently have sufficient authoritative information to determine the correct replacement values.

### Treat all old observations as stale

Rejected because historical observations are expected to be old and should not fail current-data freshness checks.

### Fail an entire run whenever warnings exist

Rejected because incomplete metadata or stale observations do not necessarily make the entire dataset unusable.