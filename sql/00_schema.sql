-- Trino 483 / Iceberg v2 reference schema. `iceberg` is an example catalog.
-- Run with deprecated.legacy-row-to-json-cast=false for the pinned hash contract.
CREATE SCHEMA IF NOT EXISTS iceberg.meta_staging;
CREATE SCHEMA IF NOT EXISTS iceberg.meta_core;
CREATE SCHEMA IF NOT EXISTS iceberg.meta_ops;

-- Append-only normalized event revisions delivered by ingestion.
-- Timestamps are UTC wall-clock values stored as TIMESTAMP without time zone.
-- A row is one delivered revision; replay deliveries can repeat the same revision.
CREATE TABLE IF NOT EXISTS iceberg.meta_staging.query_events (
    batch_id                  varchar,
    ingestion_batch_seq       bigint,
    source_event_ref          varchar,
    source_trino_version      varchar, -- nullable producer provenance
    ingested_at               timestamp(6),
    normalization_version     varchar,
    cluster_id                varchar,
    query_id                  varchar,
    event_version             bigint,
    workload_id               varchar,
    user_id                   varchar,
    query_state               varchar, -- normalized terminal state: FINISHED/FAILED/CANCELED
    start_time                timestamp(6),
    end_time                  timestamp(6),
    cpu_time_ms               bigint,
    physical_input_bytes      bigint,
    peak_memory_bytes         bigint,
    accesses_complete         boolean,
    accesses                  array(row(
        catalog_name varchar,
        schema_name  varchar,
        table_name   varchar,
        access_role  varchar
    ))
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['ingestion_batch_seq']
);

-- Canonicalization/hash only; preflight detects conflicts and MERGE selects a revision.
-- Ingestion must normalize access roles to uppercase READ/WRITE.
-- Hash includes identity, normalization contract, workload/user, times, terminal state,
-- resources, completeness, and accesses. Delivery/batch provenance is deliberately excluded.
-- Contract v1 is pinned to Trino 483 JSON row serialization; do not change the session
-- property or Trino version independently. Upgrade/recanonicalization requires a rebuild.
CREATE OR REPLACE VIEW iceberg.meta_staging.v_canonical_events AS
WITH normalized AS (
    SELECT
        e.*,
        array_sort(array_distinct(e.accesses)) AS canonical_accesses
    FROM iceberg.meta_staging.query_events e
)
SELECT
    n.batch_id,
    n.ingestion_batch_seq,
    n.source_event_ref,
    n.source_trino_version,
    n.ingested_at,
    n.normalization_version,
    n.cluster_id,
    n.query_id,
    n.event_version,
    n.workload_id,
    n.user_id,
    upper(n.query_state) AS query_state,
    n.start_time,
    n.end_time,
    n.cpu_time_ms,
    n.physical_input_bytes,
    n.peak_memory_bytes,
    n.accesses_complete,
    n.canonical_accesses AS accesses,
    'trino-483-row-json-v1' AS hash_contract_version,
    to_hex(sha256(to_utf8(json_format(CAST(ROW(
        n.cluster_id,
        n.query_id,
        n.normalization_version,
        n.workload_id,
        n.user_id,
        CAST(n.start_time AS varchar),
        CAST(n.end_time AS varchar),
        upper(n.query_state),
        n.cpu_time_ms,
        n.physical_input_bytes,
        n.peak_memory_bytes,
        n.accesses_complete,
        n.canonical_accesses
    ) AS json)))) AS payload_hash
FROM normalized n;

-- Effective-dated workload ownership. valid_to is exclusive; NULL is open-ended.
-- This is workload team attribution, not an inference from user identity.
CREATE TABLE IF NOT EXISTS iceberg.meta_ops.workload_team_map (
    mapping_id       varchar,
    workload_id      varchar,
    team_id          varchar,
    valid_from       timestamp(6),
    valid_to         timestamp(6),
    mapping_version  varchar,
    description      varchar
)
WITH (format = 'PARQUET', format_version = 2);

-- Current corrected state: one logical row per (cluster_id, query_id).
-- Iceberg does not enforce that uniqueness; the serialized publisher must validate it.
CREATE TABLE IF NOT EXISTS iceberg.meta_core.fact_query_execution (
    cluster_id                varchar,
    query_id                  varchar,
    event_version             bigint,
    payload_hash              varchar,
    hash_contract_version     varchar,
    workload_id               varchar,
    team_id                   varchar,
    team_mapping_id           varchar,
    team_mapping_version      varchar,
    user_id                   varchar,
    query_state               varchar,
    start_time                timestamp(6),
    end_time                  timestamp(6),
    event_day                 date,
    cpu_time_ms               bigint,
    physical_input_bytes      bigint,
    peak_memory_bytes         bigint,
    accesses_complete         boolean,
    accesses                  array(row(
        catalog_name varchar,
        schema_name  varchar,
        table_name   varchar,
        access_role  varchar
    )),
    source_batch_id           varchar,
    source_batch_seq          bigint,
    source_event_ref          varchar,
    source_trino_version      varchar,
    source_ingested_at        timestamp(6),
    normalization_version     varchar,
    published_at              timestamp(6)
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['event_day']
);

-- Operational cursor. Require exactly one row per pipeline_id; Iceberg does not
-- enforce it. The external runner must serialize publishers with a lock/lease.
CREATE TABLE IF NOT EXISTS iceberg.meta_ops.pipeline_checkpoint (
    pipeline_id       varchar,
    last_batch_seq    bigint,
    last_batch_id     varchar,
    updated_at        timestamp(6)
)
WITH (format = 'PARQUET', format_version = 2);

-- Bootstrap once for a new installation only; do not reset an existing checkpoint.
INSERT INTO iceberg.meta_ops.pipeline_checkpoint
SELECT 'trino_usage', 0, NULL,
       CAST(current_timestamp(6) AT TIME ZONE 'UTC' AS timestamp(6))
WHERE NOT EXISTS (
    SELECT 1
    FROM iceberg.meta_ops.pipeline_checkpoint
    WHERE pipeline_id = 'trino_usage'
);

-- One logical query/table/role row. Never sum execution-level measures from this view.
CREATE OR REPLACE VIEW iceberg.meta_core.v_query_table_access AS
SELECT
    f.cluster_id,
    f.query_id,
    f.event_version,
    f.user_id,
    f.team_id,
    f.query_state,
    f.start_time,
    f.end_time,
    f.event_day,
    f.accesses_complete,
    a.catalog_name,
    a.schema_name,
    a.table_name,
    a.access_role
FROM iceberg.meta_core.fact_query_execution f
CROSS JOIN UNNEST(f.accesses) AS u(a);
