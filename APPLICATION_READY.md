# Task #2 — Meta DWH for Trino/Iceberg usage

**Assumptions and grain.** Trino completion events in S3/Iceberg are normalized into `meta_staging.query_events`; this is a proposed contract, not JetBrains' actual raw schema. An execution is identified by `(cluster_id, query_id)`, and a query has one accountable team resolved at execution time from workload metadata or an effective-dated directory. Ingestion provides immutable sequential batches, a canonical revision number per query (not native Trino), and raw-event provenance. The fact has **one current row per query** with its complete READ/WRITE access array. The derived access view has **one row per query/table/role**; it never duplicates query-level CPU. `dim_table` is an optional metadata registry, not required to calculate resource totals.

**Metric policy.** CPU and physical input bytes are additive by query; peak memory is not. Failed queries may consume resources and count in team totals. Table user counts are of *observed* successful READs and distinct authenticated principals. `accesses_complete=FALSE` and null resource metrics are retained and reported as coverage gaps. The input sources/views and writes must be normalized using the real Trino event contract; unsupported lineage is marked incomplete, not silently assumed to be empty.

## Model and SQL DDL

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.meta_staging;
CREATE SCHEMA IF NOT EXISTS iceberg.meta_core;

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

CREATE OR REPLACE VIEW iceberg.meta_core.v_query_table_access AS
SELECT q.cluster_id, q.query_id, q.end_time, q.user_id,
       q.query_state, q.accesses_complete,
       a.catalog_name, a.schema_name, a.table_name, a.access_role
FROM iceberg.meta_core.fact_query_execution AS q
CROSS JOIN UNNEST(q.accesses) AS a(
    catalog_name, schema_name, table_name, access_role
);

CREATE TABLE IF NOT EXISTS iceberg.meta_core.dim_table (
    catalog_name VARCHAR,
    schema_name VARCHAR,
    table_name VARCHAR,
    object_type VARCHAR,
    owner_team_id VARCHAR,
    description VARCHAR
) WITH (format = 'PARQUET', format_version = 2);

CREATE SCHEMA IF NOT EXISTS iceberg.meta_ops;
CREATE TABLE IF NOT EXISTS iceberg.meta_ops.pipeline_checkpoint (
    pipeline_name VARCHAR,
    last_batch_seq BIGINT,
    last_completed_at TIMESTAMP(6) WITH TIME ZONE
) WITH (format = 'PARQUET', format_version = 2);
```

## Population and correctness

The pipeline must reject invalid source records, reused batch sequence numbers, contradictory same-revision payloads, and duplicate target keys (`01_preflight.sql`). The publisher runs serially. The following `MERGE` atomically publishes the query's metrics and its access array in one Iceberg fact; a corrected revision may also change the event-time partition. Exact reruns do nothing. After preflight, publication, and `07_postflight.sql` checks pass, `06_checkpoint.sql` advances the durable batch cursor; a failed checkpoint can be retried safely. A changed transformation rule requires a validated historical rebuild, not just replaying the same revision through `MERGE`. The canonical payload hash requires a pinned row-to-JSON serialization setting and normalization version; it must not be silently recomputed under a different configuration.

```sql
MERGE INTO iceberg.meta_core.fact_query_execution AS t
USING (
    SELECT cluster_id, query_id, event_version, payload_hash,
           source_event_ref, source_trino_version, ingested_at,
           normalization_version, ingestion_batch_seq,
           user_id, team_id, attribution_source, end_time, query_state,
           cpu_time_ms, physical_input_bytes, peak_memory_bytes,
           accesses_complete, accesses
    FROM (
        SELECT e.*, ROW_NUMBER() OVER (
            PARTITION BY cluster_id, query_id
            ORDER BY event_version DESC, ingested_at ASC, source_event_ref ASC
        ) AS rn
        FROM iceberg.meta_staging.v_canonical_events AS e
        WHERE ingestion_batch_id = 'batch_001'
    ) AS ranked
    WHERE rn = 1
) AS s
ON t.cluster_id = s.cluster_id AND t.query_id = s.query_id
WHEN MATCHED AND s.event_version > t.event_version THEN UPDATE SET
    event_version = s.event_version,
    payload_hash = s.payload_hash,
    source_event_ref = s.source_event_ref,
    source_trino_version = s.source_trino_version,
    ingested_at = s.ingested_at,
    normalization_version = s.normalization_version,
    published_batch_seq = s.ingestion_batch_seq,
    user_id = s.user_id,
    team_id = s.team_id,
    attribution_source = s.attribution_source,
    end_time = s.end_time,
    query_state = s.query_state,
    cpu_time_ms = s.cpu_time_ms,
    physical_input_bytes = s.physical_input_bytes,
    peak_memory_bytes = s.peak_memory_bytes,
    accesses_complete = s.accesses_complete,
    accesses = s.accesses
WHEN NOT MATCHED THEN INSERT (
    cluster_id, query_id, event_version, payload_hash,
    source_event_ref, source_trino_version, ingested_at,
    normalization_version, published_batch_seq,
    user_id, team_id, attribution_source, end_time, query_state,
    cpu_time_ms, physical_input_bytes, peak_memory_bytes,
    accesses_complete, accesses
) VALUES (
    s.cluster_id, s.query_id, s.event_version, s.payload_hash,
    s.source_event_ref, s.source_trino_version, s.ingested_at,
    s.normalization_version, s.ingestion_batch_seq,
    s.user_id, s.team_id, s.attribution_source, s.end_time, s.query_state,
    s.cpu_time_ms, s.physical_input_bytes, s.peak_memory_bytes,
    s.accesses_complete, s.accesses
);

MERGE INTO iceberg.meta_core.dim_table AS t
USING (
    SELECT DISTINCT a.catalog_name, a.schema_name, a.table_name
    FROM iceberg.meta_staging.v_canonical_events AS e
    CROSS JOIN UNNEST(e.accesses) AS a(
        catalog_name, schema_name, table_name, access_role
    )
    WHERE e.ingestion_batch_id = 'batch_001'
) AS s
ON t.catalog_name = s.catalog_name
AND t.schema_name = s.schema_name
AND t.table_name = s.table_name
WHEN NOT MATCHED THEN INSERT (
    catalog_name, schema_name, table_name, object_type, owner_team_id, description
) VALUES (
    s.catalog_name, s.schema_name, s.table_name, NULL, NULL, NULL
);
```


### Pre-publication conflict guard (must pass before `MERGE`)

Incoming revisions must be compared **before** `ROW_NUMBER` deduplication, both against all staged historical events and against the currently published fact. Any conflicting payload for the same `(cluster_id,query_id,event_version)` blocks publication, including conflicts arriving in a later batch. The production `01_preflight.sql` also validates keys, terminal states, access roles, batch sequence reuse and existing target duplicates. It uses an error-producing final `CAST` to block processing if any check fails. The postflight gate similarly rejects missing or inconsistent published revisions before checkpoint advancement.

```sql
WITH incoming AS (
    SELECT DISTINCT cluster_id, query_id, event_version, payload_hash
    FROM iceberg.meta_staging.v_canonical_events
    WHERE ingestion_batch_id = 'batch_001'
), staging_conflict AS (
    SELECT h.cluster_id, h.query_id, h.event_version
    FROM iceberg.meta_staging.v_canonical_events h
    JOIN (SELECT DISTINCT cluster_id, query_id, event_version
          FROM incoming) i USING (cluster_id, query_id, event_version)
    GROUP BY 1, 2, 3
    HAVING COUNT(DISTINCT h.payload_hash) > 1
), published_conflict AS (
    SELECT i.cluster_id, i.query_id, i.event_version
    FROM incoming i
    JOIN iceberg.meta_core.fact_query_execution q
      ON q.cluster_id = i.cluster_id AND q.query_id = i.query_id
     AND q.event_version = i.event_version
    WHERE i.payload_hash <> q.payload_hash
), violations AS (
    SELECT * FROM staging_conflict
    UNION SELECT * FROM published_conflict
)
SELECT CAST(CASE WHEN COUNT(*) > 0 THEN 'REVISION_CONFLICT'
                 ELSE '0' END AS BIGINT) AS prepublication_gate
FROM violations;
```

The zero-violation result is required before executing any fact `MERGE`. The staging history must retain immutable revisions while their conflict detection is required; if pruned, an authoritative revision ledger must replace it. No concurrent publishers may bypass this guard.

## Example queries

A: distinct users and executions reading table X; B: resources consumed by team Y without joining/access duplication; C: daily historical table usage. D and E quantify incomplete access metadata, missing resource measures, team attribution and ingestion freshness.

```sql
WITH executions AS (
    SELECT DISTINCT cluster_id, query_id, user_id
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name = 'iceberg' AND schema_name = 'analytics'
      AND table_name = 'orders' AND access_role = 'READ'
      AND query_state = 'FINISHED'
      AND end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
      AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z')
)
SELECT COUNT(*) AS observed_read_queries,
       COUNT(DISTINCT user_id) AS observed_distinct_users
FROM executions;

SELECT team_id, COUNT(*) AS total_queries,
       COUNT(cpu_time_ms) AS queries_with_cpu,
       SUM(cpu_time_ms) / 1000.0 AS known_cpu_seconds,
       COUNT(physical_input_bytes) AS queries_with_input_bytes,
       SUM(physical_input_bytes) / POW(1024, 3) AS known_input_gib,
       COUNT(peak_memory_bytes) AS queries_with_memory,
       MAX(peak_memory_bytes) AS max_single_query_peak_memory_bytes
FROM iceberg.meta_core.fact_query_execution
WHERE team_id = 'analytics'
  AND end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
  AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z')
GROUP BY team_id;

WITH executions AS (
    SELECT DISTINCT
           CAST(end_time AT TIME ZONE 'UTC' AS DATE) AS usage_day,
           cluster_id, query_id, user_id
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name = 'iceberg' AND schema_name = 'analytics'
      AND table_name = 'orders' AND access_role = 'READ'
      AND query_state = 'FINISHED'
      AND end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
      AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z')
)
SELECT usage_day, COUNT(*) AS observed_read_queries,
       COUNT(DISTINCT user_id) AS observed_distinct_users
FROM executions GROUP BY usage_day ORDER BY usage_day;

SELECT COUNT(*) AS total_queries,
       COUNT_IF(accesses_complete = TRUE) AS complete_access_metadata,
       ROUND(100.0 * COUNT_IF(accesses_complete = TRUE)
             / NULLIF(COUNT(*), 0), 2) AS access_coverage_pct,
       COUNT_IF(user_id IS NULL) AS unknown_users,
       COUNT_IF(team_id IS NULL) AS unattributed_queries,
       COUNT(cpu_time_ms) AS queries_with_cpu,
       COUNT(physical_input_bytes) AS queries_with_input_bytes
FROM iceberg.meta_core.fact_query_execution
WHERE end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
  AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z');

SELECT COUNT(*) AS any_access_queries FROM (
    SELECT DISTINCT cluster_id, query_id
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name='iceberg' AND schema_name='analytics'
      AND table_name='orders' AND access_role IN ('READ','WRITE')
      AND query_state='FINISHED'
      AND end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
      AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z')
) AS distinct_queries;

SELECT COUNT(*) AS total_queries,
       COUNT_IF(source_event_ref IS NULL) AS untraceable_events,
       COUNT_IF(normalization_version IS NULL) AS unknown_normalization,
       COUNT_IF(ingested_at < end_time) AS clock_skew_events,
       MAX(ingested_at) AS latest_ingested_at,
       MAX(end_time) AS latest_completion_time,
       approx_percentile(
           IF(ingested_at >= end_time,
              date_diff('second', end_time, ingested_at), NULL),
           0.95
       ) AS p95_ingestion_lag_seconds
FROM iceberg.meta_core.fact_query_execution
WHERE end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
  AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z');
```

## Maintenance and limits

Raw event references, ingestion timestamp, source version and normalization version remain on the fact for audit/replay. Use a completion-time half-open UTC window for history and an ingestion-batch cursor for incremental discovery; late query events arrive in new immutable batches. The sample fixture, a negative same-version-conflict fixture, and `05_sql_checks.sql` cover duplicates, late corrections, READ+WRITE, unknown teams and missing metrics. Preflight must block a contradictory same-version event before the `MERGE` can ignore it. Expected after two batches: 6 current executions; Analytics 46 CPU seconds, Platform 28; 3 successful `orders` READ executions from 2 distinct principals; no stale `customers` READ. These results have **not** been tested in a running Trino/Iceberg deployment.

Partition `fact_query_execution` by completion day for time-window reporting. This may increase `MERGE` search cost by query ID, so compare query plans, Iceberg `$files` and representative workloads before considering bucketing or materializing table-access records. Raw SQL text and raw S3 event paths should be restricted; general analytics consumers receive curated views and aggregate reports only. `iceberg` is an example configured catalog, not a mandated JetBrains name.
