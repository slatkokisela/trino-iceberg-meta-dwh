# Trino Usage Meta DWH — Proposed Model and Reference Implementation

## Scope

This package is a reference design for historical Trino usage reporting over Iceberg. It includes schema DDL, a raw-event adapter template, batch validation, a current-state fact upsert, checkpoint advancement, and example analytics SQL. The raw-event schema, catalog configuration, and target cluster were not supplied; the SQL has not been executed against that deployment.

## Data model

| Object | Grain | Purpose |
|---|---|---|
| `meta_staging.query_events` | One delivered query-event revision; replays may repeat it. | Append-only normalized input with batch and source provenance. The logical revision key is `(cluster_id, query_id, event_version)`. |
| `meta_staging.v_canonical_events` | One canonicalized representation per delivered event row. | Sorts/deduplicates access entries and computes the versioned payload hash used by conflict checks. It does not choose a winning revision. |
| `meta_ops.workload_team_map` | One effective-dated workload/team assignment. | Resolves the responsible workload team at execution time. Intervals are half-open; `valid_to` is exclusive and NULL means open-ended. |
| `meta_core.fact_query_execution` | One current row per `(cluster_id, query_id)`. | Holds the latest accepted revision, resource measures, user, workload/team attribution, access list, and provenance. Iceberg does not enforce the logical key. |
| `meta_core.v_query_table_access` | One query/table/role row. | Read-time `UNNEST` of the current fact's access array. It is not a second physical fact table. |
| `meta_ops.pipeline_checkpoint` | One row per pipeline. | Highest contiguous batch sequence fully validated and published. The runner must enforce uniqueness and serialize publishers. |

A table-owner dimension is optional for these questions. If added, distinguish data ownership from compute-consuming team attribution.

## Metric and mapping assumptions

- **Identity:** `(cluster_id, query_id)` represents one execution attempt. Retries with different query IDs count separately. A higher authoritative `event_version` replaces the current fact; same-version/different-payload events are rejected.
- **Users:** `user_id` is a stable authenticated principal, not necessarily a person. The example counts distinct non-null principals observed in successful (`FINISHED`) reads. Service accounts count as principals. Partial access metadata can make this an undercount.
- **Teams:** `workload_id` is resolved through the effective-dated mapping at query end time. This attributes compute to the responsible workload team, not the user's employer/team. Unmapped workloads remain NULL; no team is guessed. The fact stores `team_mapping_id` and `team_mapping_version`, while the mapping table retains effective-date history.
- **Multiple sources/tables:** Normalize completion and table-access sources by stable query identity before writing staging. A query may access many tables and may both read and write one table. Its resource measures remain on the single execution row and are not apportioned across tables. Do not aggregate CPU after joining the access view.
- **Access completeness:** `accesses_complete=true` means the list is complete, including a known empty list; `false` means partial; NULL means unknown. Observed access counts should be interpreted with a separate coverage measure.
- **Resources:** CPU milliseconds and physical input bytes are additive across executions. Peak memory is a per-execution peak; report `MAX` or a distribution, never a sum. NULL means unavailable, not zero. Resource totals include `FINISHED`, `FAILED`, and `CANCELED` executions because all may consume resources; table-user examples count only `FINISHED` reads.
- **Time and names:** Timestamps are normalized to UTC and stored as `TIMESTAMP(6)` without time zone in this example. Periods are half-open `[start, end)`. Preserve quoted/case-sensitive table identity where the source distinguishes it.

## Hash and replay contract

`v_canonical_events` canonicalizes the access array and hashes the business payload: cluster/query identity, normalization version, workload/user, start/end time, terminal state, resource measures, access-completeness flag, and canonical accesses. Ingestion must normalize roles to uppercase `READ`/`WRITE`. Batch IDs, delivery references, ingestion timestamps, and source Trino version are provenance and are excluded from the hash. A change to any hashed field at the same event version is a conflict.

Hash contract `trino-483-row-json-v1` assumes Trino 483 and `deprecated.legacy-row-to-json-cast=false`. Keep the engine/session behavior pinned for this contract; `hash_contract_version` is persisted with the fact and checked by preflight. Do not silently change the serialization or canonicalization. A change requires a planned, tested historical rebuild and reconciliation. If the target cannot pin these settings, compute the hash in a versioned normalizer using a stable serialization and replace the view hash expression accordingly.

## Ingestion contract

The upstream normalizer writes the columns defined in `meta_staging.query_events`. It assigns immutable batch ID and monotonic sequence, stable source event reference, authoritative event version, normalization version, source Trino version when available, and UTC timestamps. It maps raw states and resource units, joins separate access events by stable query identity, and marks incomplete access metadata honestly. The adapter in `sql/00b_ingest_adapter_template.sql` is a placeholder because actual source fields were not supplied.

The team map is maintained separately from query events. Validate that no workload has overlapping validity intervals at a query's execution time. If source metadata already provides authoritative team attribution, it may be loaded directly instead, but retain its attribution source and historical version.

## Publish sequence

1. Run `sql/00_schema.sql` once in a disposable catalog. It creates the model and seeds a zero checkpoint only if absent. Do not reset an existing checkpoint.
2. Load approved team mappings and append one normalized batch. Never update delivered staging rows in place.
3. Run `sql/01_preflight.sql` after substituting `__BATCH_SEQ__`. Stop if it returns any rows. It checks required fields, event states, measures, mappings, access roles, canonical payload conflicts across deliveries, conflicts with the current fact, duplicate target keys, and contiguous batch sequence.
4. Run `sql/02_transform.sql` with the same sequence after preflight has passed. The merge selects the highest revision per execution in the batch and updates only when its version is greater than the current fact version. An identical replay is a no-op only after conflict checks pass.
5. Run `sql/05_post_write_checks.sql`; reconcile counts/versions and confirm it returns no violations. Do not checkpoint a failed or unreconciled batch.
6. Run `sql/03_advance_checkpoint.sql` with the sequence and batch ID. Confirm exactly one row was updated.

The checkpoint table has no enforced primary key or compare-and-set guarantee. The external runner must enforce exactly one row per `pipeline_id` and serialize publishers using a lock/lease (or equivalent). The sequence predicate guards against a stale update, but it does not replace that coordination. SQL preflight returns rows rather than aborting the run; the runner must inspect the result and stop on violations.

## History and operations

The fact is current corrected state, not a bitemporal audit. A correction can change the event day, team, measures, or access list, so recalculated past-period results may change. Validate that the target Iceberg connector correctly handles updates that move the partition key. Late events arrive in a new batch but are attributed to execution time. A retroactive team-map change does not automatically rewrite existing facts; rebuild or explicitly correct affected facts and retain the new mapping version.

For “as published” reports, snapshot the aggregates or add a bitemporal/audit table. Transformation-rule changes require rebuilding affected data from retained events under a documented version and reconciling before cutover; an unchanged event version will not rerun under new logic.

Before production, test duplicate deliveries, conflicting same-version payloads, higher-version corrections, canceled/failed executions, partial access data, unmapped workloads, checkpoint retry, hash-contract upgrade, and partition movement against a disposable deployment. Add least-privilege access, identity protection, retention, audit, monitoring, alerting, and an operational owner.

## Files

- `sql/00_schema.sql` — schemas, staging and fact tables, canonical hash view, team map, checkpoint, access view.
- `sql/00b_ingest_adapter_template.sql` — placeholder source adapter.
- `sql/00c_team_mapping_example.sql` — illustrative effective-dated team assignment.
- `sql/01_preflight.sql` — batch and revision-conflict checks; replace `__BATCH_SEQ__`.
- `sql/02_transform.sql` — current-state fact `MERGE`; replace `__BATCH_SEQ__`.
- `sql/03_advance_checkpoint.sql` — guarded checkpoint update; replace `__BATCH_SEQ__` and `__BATCH_ID__`.
- `sql/04_analytics_examples.sql` — user, resource, coverage, and trend queries.
- `sql/05_post_write_checks.sql` — uniqueness and revision reconciliation; replace `__BATCH_SEQ__`.
- `model.mmd` — model flow diagram source.
