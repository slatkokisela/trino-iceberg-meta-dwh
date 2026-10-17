-- Run before each batch: change batch_001 in all predicates.
-- The orchestrator MUST ABORT the batch if ANY issue_count > 0.
-- The final CAST raises an SQL error if issues are detected; the CLI must stop on error.
-- Serialize publishers; reject malformed records before updating the fact table.
CREATE OR REPLACE VIEW iceberg.meta_ops.v_preflight_issues AS
WITH batch AS (
    SELECT * FROM iceberg.meta_staging.v_canonical_events
    WHERE ingestion_batch_id = 'batch_001'
),
empty_batch AS (
    SELECT IF(COUNT(*) = 0, BIGINT '1', BIGINT '0') AS n FROM batch
),
raw_invalid AS (
    SELECT COUNT(*) AS n FROM batch
    WHERE ingestion_batch_id IS NULL OR ingestion_batch_seq IS NULL
       OR ingestion_batch_seq < 1 OR source_event_ref IS NULL
       OR ingested_at IS NULL OR normalization_version IS NULL
       OR cluster_id IS NULL OR query_id IS NULL OR event_version IS NULL
       OR event_version < 1 OR end_time IS NULL
       OR query_state NOT IN ('FINISHED', 'FAILED') OR query_state IS NULL
       OR accesses_complete IS NULL OR accesses IS NULL
       OR cpu_time_ms < 0 OR physical_input_bytes < 0 OR peak_memory_bytes < 0
),
invalid_access AS (
    SELECT COUNT(*) AS n FROM batch e
    CROSS JOIN UNNEST(e.accesses) AS a(
        catalog_name, schema_name, table_name, access_role
    )
    WHERE a.catalog_name IS NULL OR a.schema_name IS NULL
       OR a.table_name IS NULL OR a.access_role IS NULL
       OR a.access_role NOT IN ('READ','WRITE')
),
batch_sequence_conflict AS (
    SELECT COUNT(*) AS n FROM (
        SELECT ingestion_batch_id
        FROM batch
        GROUP BY 1 HAVING COUNT(DISTINCT ingestion_batch_seq) > 1
    ) x
),
-- The durable cursor would silently skip data if two batch IDs reused one sequence.
sequence_reuse AS (
    SELECT COUNT(*) AS n FROM (
        SELECT ingestion_batch_seq
        FROM iceberg.meta_staging.query_events
        WHERE ingestion_batch_seq IN (
            SELECT DISTINCT ingestion_batch_seq FROM batch
        )
        GROUP BY 1 HAVING COUNT(DISTINCT ingestion_batch_id) > 1
    ) x
),
-- Conflicts across batches for affected execution + revision identities:
source_conflict AS (
    SELECT COUNT(*) AS n FROM (
        SELECT e.cluster_id, e.query_id, e.event_version
        FROM iceberg.meta_staging.v_canonical_events e
        JOIN (SELECT DISTINCT cluster_id, query_id, event_version FROM batch) b
          ON e.cluster_id=b.cluster_id AND e.query_id=b.query_id
         AND e.event_version=b.event_version
        GROUP BY 1,2,3 HAVING COUNT(DISTINCT e.payload_hash) > 1
    ) AS conflicts
),
current_conflict AS (
    SELECT COUNT(*) AS n FROM batch s
    JOIN iceberg.meta_core.fact_query_execution t
      ON s.cluster_id=t.cluster_id AND s.query_id=t.query_id
     AND s.event_version=t.event_version
    WHERE s.payload_hash <> t.payload_hash
),
existing_duplicates AS (
    SELECT COUNT(*) AS n FROM (
        SELECT cluster_id, query_id
        FROM iceberg.meta_core.fact_query_execution
        GROUP BY 1,2 HAVING COUNT(*) > 1
    ) AS duplicates
)
SELECT 'empty_batch' AS issue, n AS issue_count FROM empty_batch
UNION ALL SELECT 'invalid_source_record', n FROM raw_invalid
UNION ALL SELECT 'invalid_access', n FROM invalid_access
UNION ALL SELECT 'inconsistent_batch_sequence', n FROM batch_sequence_conflict
UNION ALL SELECT 'reused_batch_sequence', n FROM sequence_reuse
UNION ALL SELECT 'conflicting_source_revision', n FROM source_conflict
UNION ALL SELECT 'same_revision_disagrees_with_current', n FROM current_conflict
UNION ALL SELECT 'duplicate_target_execution', n FROM existing_duplicates;


-- Explicit blocking gate. Trino CAST of a nonnumeric string fails if any
-- issue_count is positive. Run as part of the same serialized batch job.
-- If this statement fails, do NOT run 02_transform.sql or 06_checkpoint.sql.
SELECT issue, issue_count
FROM iceberg.meta_ops.v_preflight_issues;

SELECT CAST(
    CASE WHEN EXISTS (
        SELECT 1 FROM iceberg.meta_ops.v_preflight_issues
        WHERE issue_count > 0
    ) THEN 'PREFLIGHT_FAILED' ELSE '0' END
    AS BIGINT
) AS preflight_gate;
