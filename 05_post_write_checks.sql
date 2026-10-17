-- Replace __BATCH_SEQ__ with the validated sequence. Any returned row blocks checkpoint advancement.
WITH latest_batch AS (
    SELECT *
    FROM (
        SELECT e.*,
               row_number() OVER (
                   PARTITION BY cluster_id, query_id
                   ORDER BY event_version DESC, ingested_at DESC, source_event_ref DESC
               ) AS revision_rank
        FROM iceberg.meta_staging.v_canonical_events e
        WHERE ingestion_batch_seq = __BATCH_SEQ__
    ) ranked
    WHERE revision_rank = 1
),
checks AS (
    SELECT 'duplicate_current_fact_keys' AS check_name, count(*) AS violation_count
    FROM (
        SELECT cluster_id, query_id
        FROM iceberg.meta_core.fact_query_execution
        GROUP BY cluster_id, query_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL
    SELECT 'batch_revision_missing_or_inconsistent', count(*)
    FROM latest_batch b
    LEFT JOIN iceberg.meta_core.fact_query_execution f
      ON f.cluster_id = b.cluster_id
     AND f.query_id = b.query_id
    WHERE f.query_id IS NULL
       OR f.event_version < b.event_version
       OR (f.event_version = b.event_version
           AND (f.payload_hash IS DISTINCT FROM b.payload_hash
                OR f.hash_contract_version IS DISTINCT FROM b.hash_contract_version))
)
SELECT check_name, violation_count
FROM checks
WHERE violation_count > 0
ORDER BY check_name;
