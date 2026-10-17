-- Execute only if EVERY issue_count in 01_preflight.sql is ZERO.
-- Source events are immutable, completions only, and batches are serialized.
-- Select one authoritative revision per query in THIS batch. Exact duplicates
-- are harmless; same-revision conflicts were rejected by preflight.
-- This is a SINGLE Iceberg target-table MERGE: measures and table accesses are
-- published together, including on a corrected end_time/partition.
MERGE INTO iceberg.meta_core.fact_query_execution AS t
USING (
    SELECT cluster_id, query_id, event_version, payload_hash,
           source_event_ref, source_trino_version, ingested_at,
           normalization_version, ingestion_batch_seq,
           user_id, team_id, attribution_source, end_time, query_state,
           cpu_time_ms, physical_input_bytes, peak_memory_bytes,
           accesses_complete, accesses
    FROM (
        SELECT e.*, ROW_NUMBER() OVER (
            PARTITION BY cluster_id, query_id
            ORDER BY event_version DESC, ingested_at ASC, source_event_ref ASC
        ) AS rn
        FROM iceberg.meta_staging.v_canonical_events AS e
        WHERE ingestion_batch_id = 'batch_001'
    ) AS ranked
    WHERE rn = 1
) AS s
ON t.cluster_id = s.cluster_id AND t.query_id = s.query_id
WHEN MATCHED AND s.event_version > t.event_version THEN UPDATE SET
    event_version = s.event_version,
    payload_hash = s.payload_hash,
    source_event_ref = s.source_event_ref,
    source_trino_version = s.source_trino_version,
    ingested_at = s.ingested_at,
    normalization_version = s.normalization_version,
    published_batch_seq = s.ingestion_batch_seq,
    user_id = s.user_id,
    team_id = s.team_id,
    attribution_source = s.attribution_source,
    end_time = s.end_time,
    query_state = s.query_state,
    cpu_time_ms = s.cpu_time_ms,
    physical_input_bytes = s.physical_input_bytes,
    peak_memory_bytes = s.peak_memory_bytes,
    accesses_complete = s.accesses_complete,
    accesses = s.accesses
WHEN NOT MATCHED THEN INSERT (
    cluster_id, query_id, event_version, payload_hash,
    source_event_ref, source_trino_version, ingested_at,
    normalization_version, published_batch_seq,
    user_id, team_id, attribution_source, end_time, query_state,
    cpu_time_ms, physical_input_bytes, peak_memory_bytes,
    accesses_complete, accesses
) VALUES (
    s.cluster_id, s.query_id, s.event_version, s.payload_hash,
    s.source_event_ref, s.source_trino_version, s.ingested_at,
    s.normalization_version, s.ingestion_batch_seq,
    s.user_id, s.team_id, s.attribution_source, s.end_time, s.query_state,
    s.cpu_time_ms, s.physical_input_bytes, s.peak_memory_bytes,
    s.accesses_complete, s.accesses
);

-- Optional discoverability registry. A retryable, non-critical enrichment:
-- missing/delayed rows here cannot change usage or resource reports.
MERGE INTO iceberg.meta_core.dim_table AS t
USING (
    SELECT DISTINCT a.catalog_name, a.schema_name, a.table_name
    FROM iceberg.meta_staging.v_canonical_events AS e
    CROSS JOIN UNNEST(e.accesses) AS a(
        catalog_name, schema_name, table_name, access_role
    )
    WHERE e.ingestion_batch_id = 'batch_001'
) AS s
ON t.catalog_name = s.catalog_name
AND t.schema_name = s.schema_name
AND t.table_name = s.table_name
WHEN NOT MATCHED THEN INSERT (
    catalog_name, schema_name, table_name, object_type, owner_team_id, description
) VALUES (
    s.catalog_name, s.schema_name, s.table_name, NULL, NULL, NULL
);
