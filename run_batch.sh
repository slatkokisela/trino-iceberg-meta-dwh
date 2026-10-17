#!/usr/bin/env bash
set -euo pipefail
# Usage: ./run_batch.sh batch_001 ; ./run_batch.sh batch_002
case "${1:-}" in
  batch_001) batch=batch_001; seq=1 ;;
  batch_002) batch=batch_002; seq=2 ;;
  batch_003) batch=batch_003; seq=3 ;; # NEGATIVE TEST: expected to fail preflight
  *) echo 'Usage: ./run_batch.sh batch_001|batch_002|batch_003' >&2; exit 2 ;;
esac
trino_sql() { docker compose exec -T trino trino --file /dev/stdin; }
# Do not commit any publication if preflight SQL signals an error.
sed "s/batch_001/${batch}/g" sql/01_preflight.sql | trino_sql
sed "s/batch_001/${batch}/g" sql/02_transform.sql | trino_sql
# Do not advance the cursor unless post-write integrity checks succeed.
sed "s/batch_001/${batch}/g" sql/07_postflight.sql | trino_sql
sed "s/CAST(1 AS BIGINT)/CAST(${seq} AS BIGINT)/g" sql/06_checkpoint.sql | trino_sql
printf 'Published and checkpointed %s successfully\n' "$batch"
