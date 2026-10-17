-- Example 1: distinct authenticated principals observed reading one table in a UTC period.
-- This is an observed count; inspect the companion coverage query below.
SELECT count(DISTINCT user_id) AS observed_distinct_users
FROM iceberg.meta_core.v_query_table_access
WHERE catalog_name = 'iceberg'
  AND schema_name = 'analytics'
  AND table_name = 'orders'
  AND access_role = 'READ'
  AND query_state = 'FINISHED'
  AND user_id IS NOT NULL
  AND end_time >= TIMESTAMP '2026-09-01 00:00:00'
  AND end_time <  TIMESTAMP '2026-10-01 00:00:00';

-- Companion coverage: query-level completeness is the denominator; do not join this to table rows.
SELECT
    count(*) AS terminal_executions,
    count_if(accesses_complete = true) AS complete_access_metadata,
    count_if(accesses_complete = false) AS known_incomplete_access_metadata,
    count_if(accesses_complete IS NULL) AS unknown_access_metadata,
    1.0 * count_if(accesses_complete = true) / nullif(count(*), 0) AS complete_fraction
FROM iceberg.meta_core.fact_query_execution
WHERE end_time >= TIMESTAMP '2026-09-01 00:00:00'
  AND end_time <  TIMESTAMP '2026-10-01 00:00:00';

-- Example 2: team-level resources in a UTC period. Includes all terminal states;
-- resource totals stay at query grain and are not multiplied by table accesses.
SELECT
    team_id,
    count(*) AS terminal_executions,
    count_if(query_state = 'FINISHED') AS successful_executions,
    count_if(query_state = 'FAILED') AS failed_executions,
    count_if(query_state = 'CANCELED') AS canceled_executions,
    count(cpu_time_ms) AS executions_with_cpu,
    sum(cpu_time_ms) / 1000.0 AS known_cpu_seconds,
    count(physical_input_bytes) AS executions_with_input_bytes,
    sum(physical_input_bytes) / 1073741824.0 AS known_physical_input_gib,
    max(peak_memory_bytes) AS largest_single_execution_peak_memory_bytes
FROM iceberg.meta_core.fact_query_execution
WHERE team_id = 'analytics'
  AND end_time >= TIMESTAMP '2026-09-01 00:00:00'
  AND end_time <  TIMESTAMP '2026-10-01 00:00:00'
GROUP BY team_id;

-- Example 3: daily history by responsible team. These are recalculated from current corrected facts.
SELECT
    event_day,
    team_id,
    count(*) AS terminal_executions,
    sum(cpu_time_ms) / 1000.0 AS known_cpu_seconds,
    sum(physical_input_bytes) / 1073741824.0 AS known_physical_input_gib,
    count(cpu_time_ms) AS executions_with_cpu,
    count(physical_input_bytes) AS executions_with_input_bytes
FROM iceberg.meta_core.fact_query_execution
WHERE event_day >= DATE '2026-09-01'
  AND event_day <  DATE '2026-10-01'
GROUP BY event_day, team_id
ORDER BY event_day, team_id;

-- Example 4: table usage trend. The count is distinct users per day; do not sum it
-- to obtain a period-wide distinct-user count (the same user can appear on many days).
SELECT
    event_day,
    catalog_name,
    schema_name,
    table_name,
    count(DISTINCT user_id) AS observed_distinct_users,
    count(DISTINCT query_id) AS successful_read_executions
FROM iceberg.meta_core.v_query_table_access
WHERE access_role = 'READ'
  AND query_state = 'FINISHED'
  AND user_id IS NOT NULL
  AND event_day >= DATE '2026-09-01'
  AND event_day <  DATE '2026-10-01'
GROUP BY event_day, catalog_name, schema_name, table_name
ORDER BY event_day, catalog_name, schema_name, table_name;

-- Unattributed resource usage: investigate null-team executions rather than guessing ownership.
SELECT
    count(*) AS executions_without_team_mapping,
    sum(cpu_time_ms) / 1000.0 AS known_unattributed_cpu_seconds
FROM iceberg.meta_core.fact_query_execution
WHERE team_id IS NULL
  AND end_time >= TIMESTAMP '2026-09-01 00:00:00'
  AND end_time <  TIMESTAMP '2026-10-01 00:00:00';
