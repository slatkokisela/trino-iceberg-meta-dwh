-- After both example batches, return ZERO ROWS for all failing assertions.
WITH tests AS (
    SELECT 'current_query_count' AS test_name, COUNT(*) AS actual, BIGINT '6' AS expected
    FROM iceberg.meta_core.fact_query_execution
    UNION ALL
    SELECT 'analytics_cpu_ms', COALESCE(SUM(cpu_time_ms),0), BIGINT '46000'
    FROM iceberg.meta_core.fact_query_execution WHERE team_id='analytics'
    UNION ALL
    SELECT 'platform_cpu_ms', COALESCE(SUM(cpu_time_ms),0), BIGINT '28000'
    FROM iceberg.meta_core.fact_query_execution WHERE team_id='platform'
    UNION ALL
    SELECT 'q1_current_version', MAX(event_version), BIGINT '2'
    FROM iceberg.meta_core.fact_query_execution
    WHERE cluster_id='main' AND query_id='q1'
    UNION ALL
    SELECT 'q1_moved_to_2026_09_12', COUNT(*), BIGINT '1'
    FROM iceberg.meta_core.fact_query_execution
    WHERE cluster_id='main' AND query_id='q1'
      AND CAST(end_time AT TIME ZONE 'UTC' AS DATE) = DATE '2026-09-12'
    UNION ALL
    SELECT 'obsolete_customers_accesses', COUNT(*), BIGINT '0'
    FROM iceberg.meta_core.v_query_table_access WHERE table_name='customers'
    UNION ALL
    SELECT 'successful_orders_read_queries', COUNT(*), BIGINT '3'
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name='iceberg' AND schema_name='analytics'
      AND table_name='orders' AND access_role='READ' AND query_state='FINISHED'
    UNION ALL
    SELECT 'orders_write_queries', COUNT(*), BIGINT '1'
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name='iceberg' AND schema_name='analytics'
      AND table_name='orders' AND access_role='WRITE' AND query_state='FINISHED'
    UNION ALL
    SELECT 'daily_summary_write_queries', COUNT(*), BIGINT '1'
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name='iceberg' AND schema_name='reports'
      AND table_name='daily_summary' AND access_role='WRITE'
    UNION ALL
    SELECT 'unknown_team', COUNT(*), BIGINT '1'
    FROM iceberg.meta_core.fact_query_execution WHERE team_id IS NULL
    UNION ALL
    SELECT 'incomplete_access_lists', COUNT(*), BIGINT '1'
    FROM iceberg.meta_core.fact_query_execution WHERE accesses_complete = FALSE
    UNION ALL
    SELECT 'missing_cpu', COUNT(*), BIGINT '1'
    FROM iceberg.meta_core.fact_query_execution WHERE cpu_time_ms IS NULL
)
SELECT * FROM tests WHERE actual IS DISTINCT FROM expected;

-- Provenance is complete for this fixture; returns zero rows if correct.
SELECT cluster_id, query_id
FROM iceberg.meta_core.fact_query_execution
WHERE source_event_ref IS NULL OR ingested_at IS NULL
   OR normalization_version IS NULL OR published_batch_seq IS NULL;

-- Corrected Q1 must point to batch 002: returns ZERO rows on success.
SELECT 'q1_batch_provenance' AS failing_check
WHERE NOT EXISTS (
  SELECT 1 FROM iceberg.meta_core.fact_query_execution
  WHERE cluster_id='main' AND query_id='q1'
    AND published_batch_seq=2
    AND source_event_ref='s3://example-raw-events/batch_002/q1.json'
);

-- Mandatory uniqueness gate. If rows exist: HALT and rebuild affected keys;
-- another MERGE cannot be relied upon to repair duplicate target matches.
SELECT cluster_id, query_id, COUNT(*) AS duplicate_rows
FROM iceberg.meta_core.fact_query_execution
GROUP BY 1,2 HAVING COUNT(*) > 1;

-- Integrity of canonical access arrays (should return zero rows).
SELECT cluster_id, query_id
FROM iceberg.meta_core.fact_query_execution
WHERE accesses IS NULL
   OR accesses_complete IS NULL
   OR accesses IS DISTINCT FROM array_sort(array_distinct(accesses));

-- Observe coverage separately from numeric resource totals (not zero-error checks).
SELECT COUNT(*) AS completed_queries,
       COUNT_IF(accesses_complete = TRUE) AS complete_access_metadata,
       COUNT(cpu_time_ms) AS with_cpu_measure,
       COUNT(physical_input_bytes) AS with_input_measure,
       COUNT_IF(team_id IS NULL) AS team_unknown
FROM iceberg.meta_core.fact_query_execution;

-- Operational cursor also requires logical uniqueness (zero rows expected).
SELECT pipeline_name, COUNT(*) AS copies
FROM iceberg.meta_ops.pipeline_checkpoint
GROUP BY pipeline_name HAVING COUNT(*) > 1;
