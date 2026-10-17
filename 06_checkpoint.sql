-- Operational example: complete *only after* preflight, fact MERGE and
-- post-merge checks have passed. A job runner must enforce those gates.
-- Sequential immutable batch IDs are provided by the ingestion layer.
-- If this checkpoint write fails, rerun the fact MERGE then this statement.
-- The source batch sequence must be globally monotonic for this pipeline.
MERGE INTO iceberg.meta_ops.pipeline_checkpoint AS t
USING (
    SELECT 'trino_query_usage' AS pipeline_name,
           CAST(1 AS BIGINT) AS last_batch_seq
) AS s
ON t.pipeline_name = s.pipeline_name
WHEN MATCHED AND s.last_batch_seq > t.last_batch_seq
    THEN UPDATE SET last_batch_seq = s.last_batch_seq,
                    last_completed_at = current_timestamp
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_batch_seq, last_completed_at
) VALUES (
    s.pipeline_name, s.last_batch_seq, current_timestamp
);

-- Query the current cursor; the job runner should select (last_batch_seq + 1).
SELECT COALESCE(MAX(last_batch_seq), 0) + 1 AS next_batch_seq
FROM iceberg.meta_ops.pipeline_checkpoint
WHERE pipeline_name = 'trino_query_usage';

-- Run for batch_002 only after changing CAST(1 AS BIGINT) to CAST(2 AS BIGINT).
-- Production orchestration selects the NEXT immutable batch sequence above
-- this cursor, processing missing sequences in order. Late event-time records
-- arrive in NEW batches, never appended to a previously completed batch.
SELECT pipeline_name, last_batch_seq, last_completed_at
FROM iceberg.meta_ops.pipeline_checkpoint
WHERE pipeline_name = 'trino_query_usage';
