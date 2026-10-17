-- A. Confirmed READ usage of table X in the half-open UTC window.
-- Count a query once per table and READ role, including self-joins.
-- Because incomplete source access metadata may omit reads, these are
-- OBSERVED counts, not guaranteed exhaustive counts; see coverage report D.
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

-- B. Team resource consumption: use execution-grain facts, no access join.
-- Source contains only query-completion events, including failures.
-- Missing measures are NOT substituted with zero; coverage is reported.
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

-- C. Time-bounded daily historical READ usage (UTC).
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

-- D. Completeness and enrichment coverage for the SAME reporting window.
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

-- Optional example: one query that BOTH reads and writes the same table
-- must count only once as a querying execution across both roles.
SELECT COUNT(*) AS any_access_queries FROM (
    SELECT DISTINCT cluster_id, query_id
    FROM iceberg.meta_core.v_query_table_access
    WHERE catalog_name='iceberg' AND schema_name='analytics'
      AND table_name='orders' AND access_role IN ('READ','WRITE')
      AND query_state='FINISHED'
      AND end_time >= from_iso8601_timestamp('2026-09-01T00:00:00Z')
      AND end_time <  from_iso8601_timestamp('2026-10-01T00:00:00Z')
) AS distinct_queries;

-- E. Data freshness and provenance quality in the SAME requested period.
-- A negative ingestion lag indicates clock skew / invalid event-time mapping.
-- This checks metadata quality but cannot by itself prove source completeness.
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
