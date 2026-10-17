-- NEGATIVE TEST ONLY: run against disposable test environment AFTER batch 001.
-- Reuse q2 revision 1 with changed CPU value in a NEW batch.
-- Replaying same revision with a different payload must NEVER silently pass.
-- The preflight for batch_003 should fail (SQL error PREFLIGHT_FAILED).
-- Do not run transformation or checkpoint on this batch.
INSERT INTO iceberg.meta_staging.query_events VALUES
('batch_003',3,'s3://example-raw-events/batch_003/q2_changed.json','483',
 from_iso8601_timestamp('2026-09-15T00:00:00Z'),'v1','main','q2',1,
 'bob','platform','workload',from_iso8601_timestamp('2026-09-10T13:00:00Z'),
 'FINISHED',21000,1073741824,209715200,TRUE,
 ARRAY[CAST(ROW('iceberg','analytics','orders','READ') AS
       ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,
           access_role VARCHAR))]);
