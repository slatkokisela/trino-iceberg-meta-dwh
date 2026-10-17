-- Example only; replace with approved ownership metadata.
-- `valid_to` is exclusive. Add a new interval when ownership changes; retain history.
INSERT INTO iceberg.meta_ops.workload_team_map (
    mapping_id, workload_id, team_id, valid_from, valid_to, mapping_version, description
)
VALUES (
    'map-warehouse-analytics-v1',
    'warehouse-analytics',
    'analytics',
    TIMESTAMP '2026-01-01 00:00:00',
    NULL,
    'v1',
    'Example mapping; replace with approved workload ownership'
);
