-- Post-publication validation for the selected immutable ingestion batch.
-- Change batch_001 to the batch being processed.
-- The final statement deliberately FAILS if the published fact state is invalid.
CREATE OR REPLACE VIEW iceberg.meta_ops.v_postflight_issues AS
WITH incoming AS (
    SELECT cluster_id, query_id, event_version, payload_hash
    FROM (
        SELECT e.*, ROW_NUMBER() OVER (
            PARTITION BY cluster_id, query_id
            ORDER BY event_version DESC, ingested_at ASC, source_event_ref ASC
        ) AS rn
        FROM iceberg.meta_staging.v_canonical_events e
        WHERE ingestion_batch_id = 'batch_001'
    ) ranked
    WHERE rn = 1
),
not_published AS (
    SELECT COUNT(*) AS n
    FROM incoming i
    LEFT JOIN iceberg.meta_core.fact_query_execution q
      ON q.cluster_id = i.cluster_id AND q.query_id = i.query_id
    WHERE q.cluster_id IS NULL OR q.event_version < i.event_version
       OR (q.event_version = i.event_version
           AND q.payload_hash IS DISTINCT FROM i.payload_hash)
),
duplicate_fact AS (
    SELECT COUNT(*) AS n FROM (
        SELECT cluster_id, query_id
        FROM iceberg.meta_core.fact_query_execution
        GROUP BY 1,2 HAVING COUNT(*) > 1
    ) d
),
noncanonical_accesses AS (
    SELECT COUNT(*) AS n
    FROM iceberg.meta_core.fact_query_execution
    WHERE accesses IS NULL OR accesses_complete IS NULL
       OR accesses IS DISTINCT FROM array_sort(array_distinct(accesses))
)
SELECT 'incoming_revision_not_published' issue, n issue_count FROM not_published
UNION ALL SELECT 'duplicate_fact_execution', n FROM duplicate_fact
UNION ALL SELECT 'noncanonical_access_list', n FROM noncanonical_accesses;

SELECT issue, issue_count FROM iceberg.meta_ops.v_postflight_issues;

SELECT CAST(
  CASE WHEN EXISTS (
      SELECT 1 FROM iceberg.meta_ops.v_postflight_issues
      WHERE issue_count > 0
  ) THEN 'POSTFLIGHT_FAILED' ELSE '0' END
  AS BIGINT
) AS postflight_gate;
