-- Replace __BATCH_SEQ__ with the batch's ingestion_batch_seq.
-- Stop the publisher if this query returns any rows.
WITH batch AS (
    SELECT *
    FROM iceberg.meta_staging.v_canonical_events
    WHERE ingestion_batch_seq = __BATCH_SEQ__
),
checks AS (
    SELECT 'batch_not_found' AS check_name,
           CASE WHEN count(*) = 0 THEN 1 ELSE 0 END AS violation_count
    FROM batch

    UNION ALL
    SELECT 'missing_required_identity_or_provenance', count(*)
    FROM batch
    WHERE batch_id IS NULL OR source_event_ref IS NULL OR ingested_at IS NULL
       OR normalization_version IS NULL OR cluster_id IS NULL OR query_id IS NULL
       OR event_version IS NULL OR event_version < 1 OR workload_id IS NULL
       OR query_state IS NULL OR end_time IS NULL OR payload_hash IS NULL

    UNION ALL
    SELECT 'unsupported_terminal_state', count(*)
    FROM batch
    WHERE query_state NOT IN ('FINISHED', 'FAILED', 'CANCELED')

    UNION ALL
    SELECT 'unexpected_hash_contract_version', count(*)
    FROM batch
    WHERE hash_contract_version <> 'trino-483-row-json-v1'

    UNION ALL
    SELECT 'negative_resource_measure', count(*)
    FROM batch
    WHERE coalesce(cpu_time_ms < 0, false)
       OR coalesce(physical_input_bytes < 0, false)
       OR coalesce(peak_memory_bytes < 0, false)

    UNION ALL
    SELECT 'end_time_before_start_time', count(*)
    FROM batch
    WHERE start_time IS NOT NULL AND end_time < start_time

    UNION ALL
    SELECT 'invalid_team_mapping_interval', count(*)
    FROM iceberg.meta_ops.workload_team_map
    WHERE mapping_id IS NULL OR workload_id IS NULL OR team_id IS NULL
       OR valid_from IS NULL OR (valid_to IS NOT NULL AND valid_to <= valid_from)

    UNION ALL
    SELECT 'complete_access_list_is_null', count(*)
    FROM batch
    WHERE accesses_complete = true AND accesses IS NULL

    UNION ALL
    SELECT 'invalid_access_identity_or_role', count(*)
    FROM batch
    WHERE coalesce(any_match(accesses, a ->
           a IS NULL OR a.catalog_name IS NULL OR a.schema_name IS NULL OR a.table_name IS NULL
           OR a.access_role IS NULL OR a.access_role NOT IN ('READ', 'WRITE')
       ), false)

    UNION ALL
    SELECT 'same_version_conflicting_payloads_across_deliveries', count(*)
    FROM (
        SELECT c.cluster_id, c.query_id, c.event_version
        FROM iceberg.meta_staging.v_canonical_events c
        JOIN (SELECT DISTINCT cluster_id, query_id, event_version FROM batch) b
          ON b.cluster_id = c.cluster_id
         AND b.query_id = c.query_id
         AND b.event_version = c.event_version
        GROUP BY c.cluster_id, c.query_id, c.event_version
        HAVING count(DISTINCT c.payload_hash) > 1
            OR count(DISTINCT c.hash_contract_version) > 1
    ) conflicts

    UNION ALL
    SELECT 'same_version_conflicts_with_current_fact', count(*)
    FROM (
        SELECT DISTINCT b.cluster_id, b.query_id, b.event_version
        FROM batch b
        JOIN iceberg.meta_core.fact_query_execution f
          ON f.cluster_id = b.cluster_id
         AND f.query_id = b.query_id
         AND f.event_version = b.event_version
        WHERE f.payload_hash IS DISTINCT FROM b.payload_hash
           OR f.hash_contract_version IS DISTINCT FROM b.hash_contract_version
    ) conflicts

    UNION ALL
    SELECT 'duplicate_current_fact_keys', count(*)
    FROM (
        SELECT cluster_id, query_id
        FROM iceberg.meta_core.fact_query_execution
        GROUP BY cluster_id, query_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT 'multiple_team_mappings_at_execution_time', count(*)
    FROM (
        SELECT b.cluster_id, b.query_id, b.event_version
        FROM batch b
        JOIN iceberg.meta_ops.workload_team_map m
          ON m.workload_id = b.workload_id
         AND coalesce(b.end_time, b.start_time) >= m.valid_from
         AND (m.valid_to IS NULL OR coalesce(b.end_time, b.start_time) < m.valid_to)
        GROUP BY b.cluster_id, b.query_id, b.event_version
        HAVING count(*) > 1
    ) ambiguous

    UNION ALL
    SELECT 'batch_sequence_reused_for_multiple_batch_ids',
           CASE WHEN count(DISTINCT batch_id) > 1 THEN 1 ELSE 0 END
    FROM batch

    UNION ALL
    SELECT 'batch_not_next_checkpoint_sequence',
           CASE WHEN max(b.ingestion_batch_seq) <> max(c.last_batch_seq) + 1 THEN 1 ELSE 0 END
    FROM batch b
    CROSS JOIN iceberg.meta_ops.pipeline_checkpoint c
    WHERE c.pipeline_id = 'trino_usage'
)
SELECT check_name, violation_count
FROM checks
WHERE violation_count > 0
ORDER BY check_name;
