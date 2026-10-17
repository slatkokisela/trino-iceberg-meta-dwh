-- Adapter template: raw source table and column names were not provided.
-- Replace placeholders with the source-specific normalization logic.
-- Produce one row per query revision; join/aggregate a separate access stream first.
-- The canonical view sorts and de-duplicates accesses, then computes payload_hash.
-- Do not mark accesses complete until the source confirms the access feed is complete.

INSERT INTO iceberg.meta_staging.query_events (
    batch_id, ingestion_batch_seq, source_event_ref, source_trino_version,
    ingested_at, normalization_version, cluster_id, query_id, event_version,
    workload_id, user_id, query_state, start_time, end_time, cpu_time_ms,
    physical_input_bytes, peak_memory_bytes, accesses_complete, accesses
)
SELECT
    <batch_id>,
    <ingestion_batch_seq>,
    <stable_source_event_ref>,
    <source_trino_version>,
    CAST(<ingestion_timestamp_utc> AS timestamp(6)),
    <normalization_version>,
    <cluster_id>,
    <query_id>,
    <authoritative_event_version>,
    <workload_id>,
    <authenticated_principal_id>,
    upper(<terminal_query_state>),
    CAST(<start_time_utc> AS timestamp(6)),
    CAST(<end_time_utc> AS timestamp(6)),
    <cpu_time_ms>,
    <physical_input_bytes>,
    <peak_memory_bytes>,
    <access_metadata_complete_boolean>,
    <array_of_rows_catalog_schema_table_role>
FROM <normalized_raw_completion_source>;

-- For a separate table-access feed, aggregate it by query and revision before joining.
-- Preserve queries with no observed accesses using an empty array only when completeness
-- is known; a missing/late access feed is not evidence of zero table usage.
