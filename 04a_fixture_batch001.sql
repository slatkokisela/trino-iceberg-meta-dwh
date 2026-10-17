-- Run after 00_schema.sql, only in a fresh disposable Iceberg schema.
-- Load batch 001, run preflight (zero issues), then run 02_transform.sql.
INSERT INTO iceberg.meta_staging.query_events VALUES
('batch_001',1,'s3://example-raw-events/batch_001/q1.json','483',from_iso8601_timestamp('2026-09-13T00:00:00Z'),'v1','main','q1',1,'alice','analytics','workload',
 from_iso8601_timestamp('2026-09-10T12:00:00Z'),'FINISHED',30000,2147483648,104857600,TRUE,
 ARRAY[CAST(ROW('iceberg','analytics','orders','READ') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR)),
       CAST(ROW('iceberg','analytics','customers','READ') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR))]),
('batch_001',1,'s3://example-raw-events/batch_001/q2.json','483',from_iso8601_timestamp('2026-09-13T00:00:00Z'),'v1','main','q2',1,'bob','platform','workload',
 from_iso8601_timestamp('2026-09-10T13:00:00Z'),'FINISHED',20000,1073741824,209715200,TRUE,
 ARRAY[CAST(ROW('iceberg','analytics','orders','READ') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR))]),
('batch_001',1,'s3://example-raw-events/batch_001/q3.json','483',from_iso8601_timestamp('2026-09-13T00:00:00Z'),'v1','main','q3',1,'alice','analytics','workload',
 from_iso8601_timestamp('2026-09-11T13:00:00Z'),'FAILED',5000,0,52428800,TRUE,
 ARRAY[CAST(ROW('iceberg','analytics','orders','READ') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR))]),
('batch_001',1,'s3://example-raw-events/batch_001/q4.json','483',from_iso8601_timestamp('2026-09-13T00:00:00Z'),'v1','main','q4',1,'bob','platform','workload',
 from_iso8601_timestamp('2026-09-11T14:00:00Z'),'FINISHED',8000,0,73400320,TRUE,
 ARRAY[CAST(ROW('iceberg','reports','daily_summary','WRITE') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR))]),
('batch_001',1,'s3://example-raw-events/batch_001/q5.json','483',from_iso8601_timestamp('2026-09-13T00:00:00Z'),'v1','main','q5',1,'charlie',NULL,NULL,
 from_iso8601_timestamp('2026-09-11T15:00:00Z'),'FINISHED',NULL,NULL,NULL,FALSE,
 CAST(ARRAY[] AS ARRAY(ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR)))),
('batch_001',1,'s3://example-raw-events/batch_001/q6.json','483',from_iso8601_timestamp('2026-09-13T00:00:00Z'),'v1','main','q6',1,'alice','analytics','workload',
 from_iso8601_timestamp('2026-09-12T16:00:00Z'),'FINISHED',6000,0,94371840,TRUE,
 ARRAY[CAST(ROW('iceberg','analytics','orders','READ') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR)),
       CAST(ROW('iceberg','analytics','orders','WRITE') AS ROW(catalog_name VARCHAR,schema_name VARCHAR,table_name VARCHAR,access_role VARCHAR))]);

