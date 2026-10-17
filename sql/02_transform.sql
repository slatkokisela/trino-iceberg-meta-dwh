-- Replace __BATCH_SEQ__ with the sequence after preflight returns no violations.
-- The runner must serialize publishers; Iceberg does not enforce the logical key.
MERGE INTO iceberg.meta_core.fact_query_execution AS target
USING (
    WITH batch AS (
        SELECT *
        FROM iceberg.meta_staging.v_canonical_events
        WHERE ingestion_batch_seq = __BATCH_SEQ__
    ),
    latest_revision AS (
        SELECT *
        FROM (
            SELECT b.*,
                   row_number() OVER (
                       PARTITION BY cluster_id, query_id
                       ORDER BY event_version DESC, ingested_at DESC, source_event_ref DESC
                   ) AS revision_rank
            FROM batch b
        ) ranked
        WHERE revision_rank = 1
    )
    SELECT
        e.cluster_id,
        e.query_id,
        e.event_version,
        e.payload_hash,
        e.hash_contract_version,
        e.workload_id,
        m.team_id,
        m.mapping_id AS team_mapping_id,
        m.mapping_version AS team_mapping_version,
        e.user_id,
        e.query_state,
        e.start_time,
        e.end_time,
        CAST(e.end_time AS date) AS event_day,
        e.cpu_time_ms,
        e.physical_input_bytes,
        e.peak_memory_bytes,
        e.accesses_complete,
        e.accesses,
        e.batch_id AS source_batch_id,
        e.ingestion_batch_seq AS source_batch_seq,
        e.source_event_ref,
        e.source_trino_version,
        e.ingested_at AS source_ingested_at,
        e.normalization_version
    FROM latest_revision e
    LEFT JOIN iceberg.meta_ops.workload_team_map m
      ON m.workload_id = e.workload_id
     AND coalesce(e.end_time, e.start_time) >= m.valid_from
     AND (m.valid_to IS NULL OR coalesce(e.end_time, e.start_time) < m.valid_to)
) AS source
ON target.cluster_id = source.cluster_id
AND target.query_id = source.query_id
WHEN MATCHED AND source.event_version > target.event_version THEN
    UPDATE SET
        event_version = source.event_version,
        payload_hash = source.payload_hash,
        hash_contract_version = source.hash_contract_version,
        workload_id = source.workload_id,
        team_id = source.team_id,
        team_mapping_id = source.team_mapping_id,
        team_mapping_version = source.team_mapping_version,
        user_id = source.user_id,
        query_state = source.query_state,
        start_time = source.start_time,
        end_time = source.end_time,
        event_day = source.event_day,
        cpu_time_ms = source.cpu_time_ms,
        physical_input_bytes = source.physical_input_bytes,
        peak_memory_bytes = source.peak_memory_bytes,
        accesses_complete = source.accesses_complete,
        accesses = source.accesses,
        source_batch_id = source.source_batch_id,
        source_batch_seq = source.source_batch_seq,
        source_event_ref = source.source_event_ref,
        source_trino_version = source.source_trino_version,
        source_ingested_at = source.source_ingested_at,
        normalization_version = source.normalization_version,
        published_at = CAST(current_timestamp(6) AT TIME ZONE 'UTC' AS timestamp(6))
WHEN NOT MATCHED THEN
    INSERT (
        cluster_id, query_id, event_version, payload_hash, hash_contract_version,
        workload_id, team_id, team_mapping_id, team_mapping_version, user_id, query_state,
        start_time, end_time, event_day, cpu_time_ms, physical_input_bytes,
        peak_memory_bytes, accesses_complete, accesses, source_batch_id,
        source_batch_seq, source_event_ref, source_trino_version,
        source_ingested_at, normalization_version, published_at
    ) VALUES (
        source.cluster_id, source.query_id, source.event_version, source.payload_hash,
        source.hash_contract_version, source.workload_id, source.team_id,
        source.team_mapping_id, source.team_mapping_version, source.user_id, source.query_state,
        source.start_time, source.end_time, source.event_day, source.cpu_time_ms,
        source.physical_input_bytes, source.peak_memory_bytes, source.accesses_complete,
        source.accesses, source.source_batch_id, source.source_batch_seq,
        source.source_event_ref, source.source_trino_version,
        source.source_ingested_at, source.normalization_version,
        CAST(current_timestamp(6) AT TIME ZONE 'UTC' AS timestamp(6))
    );
