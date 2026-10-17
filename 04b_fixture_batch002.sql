-- Correction: source Q1 was mis-enriched (not a native Trino re-execution).
-- Q1's authoritative revision 2 changes CPU, table-access list AND end_time,
-- exercising an Iceberg MERGE update that moves a row to another day partition.
-- Preserve Q1 version 1 in staging: DO NOT DELETE source records.
INSERT INTO iceberg.meta_staging.query_events VALUES
('batch_002',2,'s3://example-raw-events/batch_002/q1.json','483',from_iso8601_timestamp('2026-09-14T00:00:00Z'),'v1','main','q1',2,'alice','analytics','workload',
 from_iso8601_timestamp('2026-09-12T12:00:00Z'),'FINISHED',35000,2147483648,104857600,TRUE,
 ARRAY[CAST(ROW('iceberg','analytics','orders','READ') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR))]);

-- Run the preflight and MERGE once with batch_001, then change 'batch_001'
-- to 'batch_002' in 01_preflight.sql and 02_transform.sql and run both again.
-- Re-running either batch (including the OLDER batch) must not change output.
