# Browser-only Trino + Iceberg setup for the JetBrains assignment

**No local Docker installation needed.** Run this repository in a GitHub Codespace. All containers run in the remote Codespace. The demo is for disposable testing only; passwords are intentionally non-production.

## Start the environment

1. Create a private GitHub repository and upload the extracted contents of this folder (including `compose.yaml`, `catalog/`, and `sql/`).
2. In GitHub, select **Code → Codespaces → Create codespace on main**.
3. In the Codespaces terminal, run:

   ```bash
   docker compose up -d
   docker compose ps
   ```

4. Check that Trino is ready:

   ```bash
   docker compose exec -T trino trino --execute 'SELECT version()'
   docker compose exec -T trino trino --execute 'SHOW CATALOGS'
   ```

   The second command must list `iceberg`. If the catalog is absent, inspect `docker compose logs iceberg-rest trino`. The first startup may take several minutes.

## Run the assignment SQL

Execute scripts **one at a time** with a fresh disposable environment. For example:

```bash
docker compose exec -T trino trino --file /dev/stdin < sql/00_schema.sql
docker compose exec -T trino trino --file /dev/stdin < sql/04a_fixture_batch001.sql
docker compose exec -T trino trino --file /dev/stdin < sql/01_preflight.sql
```

**Important:** Preflight now ends with a deliberately failing `CAST` when any issue_count is positive; the command must exit nonzero. The batch publisher must stop on SQL errors. `./run_batch.sh` demonstrates this with `set -euo pipefail`, followed by `07_postflight.sql` before the checkpoint.

```bash
docker compose exec -T trino trino --file /dev/stdin < sql/02_transform.sql
docker compose exec -T trino trino --execute 'SELECT count(*) FROM iceberg.meta_core.fact_query_execution'
docker compose exec -T trino trino --file /dev/stdin < sql/04b_fixture_batch002.sql
sed 's/batch_001/batch_002/g' sql/01_preflight.sql | docker compose exec -T trino trino --file /dev/stdin
```

Only if that second preflight also has zero issues:

```bash
sed 's/batch_001/batch_002/g' sql/02_transform.sql | docker compose exec -T trino trino --file /dev/stdin
docker compose exec -T trino trino --file /dev/stdin < sql/05_sql_checks.sql
```

`05_sql_checks.sql` contains different types of queries: **failure queries should return zero rows**; coverage and observation queries intentionally return data. Review each result according to its comment rather than expecting an entirely empty output.

The expected fixture totals after the correction are: 6 current queries, Analytics 46,000 CPU ms, Platform 28,000 CPU ms, and zero current accesses to `customers`.

The checkpoint script is an operational example and should only be run after required validations. Adjust its value from sequence 1 to 2 for the second batch.

## Common problems

- `iceberg` not in `SHOW CATALOGS`: inspect Trino configuration and `docker compose logs trino`.
- `NoSuchBucket` / S3 errors: inspect `docker compose logs create-bucket minio iceberg-rest`, confirm warehouse bucket creation, retry startup.
- `MERGE` fails: capture the exact message. It could be SQL incompatibility, source duplication, or Iceberg connector configuration.
- Insufficient Codespace RAM: choose a larger machine if your account supports one; stop unused containers.
- SQL file errors: these are reference scripts that have **not** been executed end-to-end here. Treat the exercise as a validation opportunity, not as proof of working SQL.

## Cleanup

```bash
docker compose down -v
```

Stop or delete the Codespace when finished to avoid unnecessary quota usage. Publicly exposing the demonstration MinIO credentials or ports would be unsafe; keep the Codespace and repository private.

## Fail-closed batch runner and negative conflict test

After starting services and applying `00_schema.sql`, load the batch fixture and run:

```bash
./run_batch.sh batch_001
# Load batch_002 SQL, then:
./run_batch.sh batch_002
docker compose exec -T trino trino --file /dev/stdin < sql/05_sql_checks.sql
```

To verify **cross-batch same-version conflict rejection** without modifying published data, use a disposable environment after batch 001 or 002:

```bash
docker compose exec -T trino trino --file /dev/stdin < sql/04c_conflicting_revision.sql
./run_batch.sh batch_003
```

The last command **must fail** with an invalid cast of `PREFLIGHT_FAILED`. If it succeeds, the guard is broken. `fact_query_execution` and the checkpoint must stay unchanged for `batch_003`. Do not run the negative fixture on production data.

`payload_hash` is calculated from canonical source values using a pinned row-to-JSON convention. Trino release 357 changed the JSON output of ROW casts and supports a legacy configuration toggle. The demo targets Trino 483 with object-style ROW JSON (`deprecated.legacy-row-to-json-cast=false`), with changes to serialization treated as migration/rebuilds, not ordinary event revisions. 

This demo hasn't been run against a live Trino cluster in this environment. Real orchestration must additionally enforce serialized writers, stable source snapshots during preflight/publish, and source-level reconciliation when available. The included postflight gates check the published query revision, duplicate facts, and canonical accesses before checkpoint advancement.
