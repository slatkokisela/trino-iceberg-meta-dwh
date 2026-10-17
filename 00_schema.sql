-- Reference Trino 483 / Iceberg format v2 SQL.
-- 'iceberg' is an illustrative configured Trino catalog; use a disposable schema for fixtures.
CREATE SCHEMA IF NOT EXISTS iceberg.meta_staging;
CREATE SCHEMA IF NOT EXISTS iceberg.meta_core;

-- ASSUMED normalized completion-event contract, not a native JetBrains raw schema.
-- Raw source events remain immutable in their original Iceberg tables.
CREATE TABLE IF NOT EXISTS iceberg.meta_staging.query_events (
    ingestion_batch_id VARCHAR,
    ingestion_batch_seq BIGINT,   -- monotone immutable batch number assigned by ingestion
    source_event_ref VARCHAR,     -- stable pointer to native raw event; not SQL text
    source_trino_version VARCHAR, -- nullable if producer version unavailable
    ingested_at TIMESTAMP(6) WITH TIME ZONE,
    normalization_version VARCHAR,
    cluster_id VARCHAR,
    query_id VARCHAR,
    event_version BIGINT,      -- ingest-controlled monotone revision, not a Trino field
    user_id VARCHAR,
    team_id VARCHAR,            -- responsible team at execution time; nullable when unknown
    attribution_source VARCHAR,
    end_time TIMESTAMP(6) WITH TIME ZONE,
    query_state VARCHAR,        -- normalized terminal states: FINISHED or FAILED
    cpu_time_ms BIGINT,
    physical_input_bytes BIGINT,
    peak_memory_bytes BIGINT,
    accesses_complete BOOLEAN,
    accesses ARRAY(ROW(
        catalog_name VARCHAR,
        schema_name VARCHAR,
        table_name VARCHAR,
        access_role VARCHAR    -- READ or WRITE, both allowed for a table in one query
    ))
) WITH (format = 'PARQUET', format_version = 2);

-- Canonical ordering and duplicate elimination make replay independent of
-- the order and repetition of per-table access records.
-- Hash covers the business payload, NOT batch ID or event version.
-- HASH CONTRACT v1: pin row-to-JSON cast mode (object representation;
-- deprecated.legacy-row-to-json-cast=false) and Trino version for both
-- ingestion and replay. A changed canonicalization or Trino upgrade requires
-- a separately validated historical rebuild, not comparing old/new hashes.
-- Both normalization version and the expected hash serialization are managed
-- as a single versioned contract, not changed independently.
CREATE OR REPLACE VIEW iceberg.meta_staging.v_canonical_events AS
WITH normalized AS (
    SELECT ingestion_batch_id, ingestion_batch_seq, source_event_ref,
           source_trino_version, ingested_at, normalization_version,
           cluster_id, query_id, event_version,
           user_id, team_id, attribution_source, end_time, query_state,
           cpu_time_ms, physical_input_bytes, peak_memory_bytes,
           accesses_complete, array_sort(array_distinct(accesses)) AS accesses
    FROM iceberg.meta_staging.query_events
)
SELECT n.*,
       to_hex(sha256(to_utf8(json_format(CAST(ROW(
           user_id, team_id, attribution_source, CAST(end_time AT TIME ZONE 'UTC' AS VARCHAR),
           query_state, cpu_time_ms, physical_input_bytes, peak_memory_bytes,
           accesses_complete, accesses
       ) AS JSON))))) AS payload_hash
FROM normalized AS n;

-- One atomically published CURRENT record per completed query execution.
-- Logical uniqueness (cluster_id,query_id) is checked outside the Iceberg format:
-- Iceberg does not enforce a primary key on this table.
CREATE TABLE IF NOT EXISTS iceberg.meta_core.fact_query_execution (
    cluster_id VARCHAR,
    query_id VARCHAR,
    event_version BIGINT,
    payload_hash VARCHAR,
    source_event_ref VARCHAR,
    source_trino_version VARCHAR,
    ingested_at TIMESTAMP(6) WITH TIME ZONE,
    normalization_version VARCHAR,
    published_batch_seq BIGINT,
    user_id VARCHAR,
    team_id VARCHAR,
    attribution_source VARCHAR,
    end_time TIMESTAMP(6) WITH TIME ZONE,
    query_state VARCHAR,
    cpu_time_ms BIGINT,
    physical_input_bytes BIGINT,
    peak_memory_bytes BIGINT,
    accesses_complete BOOLEAN,
    accesses ARRAY(ROW(
        catalog_name VARCHAR,
        schema_name VARCHAR,
        table_name VARCHAR,
        access_role VARCHAR
    ))
) WITH (format = 'PARQUET', format_version = 2,
        partitioning = ARRAY['day(end_time)']);

-- Logical query-to-table bridge derived from the SAME atomic fact snapshot.
-- Grain: (cluster_id, query_id, catalog, schema, table, access_role).
CREATE OR REPLACE VIEW iceberg.meta_core.v_query_table_access AS
SELECT q.cluster_id, q.query_id, q.end_time, q.user_id,
       q.query_state, q.accesses_complete,
       a.catalog_name, a.schema_name, a.table_name, a.access_role
FROM iceberg.meta_core.fact_query_execution AS q
CROSS JOIN UNNEST(q.accesses) AS a(
    catalog_name, schema_name, table_name, access_role
);

-- Optional metadata registry, not joined into the mandatory reporting queries.
CREATE TABLE IF NOT EXISTS iceberg.meta_core.dim_table (
    catalog_name VARCHAR,
    schema_name VARCHAR,
    table_name VARCHAR,
    object_type VARCHAR,
    owner_team_id VARCHAR,
    description VARCHAR
) WITH (format = 'PARQUET', format_version = 2);

-- Operational cursor: written only AFTER fact MERGE and SQL checks pass.
-- A failed checkpoint write can be replayed safely because the fact MERGE
-- is idempotent. This table is not itself an analytics data source.
CREATE SCHEMA IF NOT EXISTS iceberg.meta_ops;
CREATE TABLE IF NOT EXISTS iceberg.meta_ops.pipeline_checkpoint (
    pipeline_name VARCHAR,
    last_batch_seq BIGINT,
    last_completed_at TIMESTAMP(6) WITH TIME ZONE
) WITH (format = 'PARQUET', format_version = 2);
