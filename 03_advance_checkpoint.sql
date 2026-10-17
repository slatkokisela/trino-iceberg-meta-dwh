-- Replace the placeholders for a validated batch. Run after post-write checks;
-- the serialized runner must confirm exactly one row updated or stop for reconciliation.
UPDATE iceberg.meta_ops.pipeline_checkpoint
SET last_batch_seq = __BATCH_SEQ__,
    last_batch_id = __BATCH_ID__,
    updated_at = CAST(current_timestamp(6) AT TIME ZONE 'UTC' AS timestamp(6))
WHERE pipeline_id = 'trino_usage'
  AND last_batch_seq = __BATCH_SEQ__ - 1;
