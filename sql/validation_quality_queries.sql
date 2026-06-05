-- Validation quality monitoring queries
-- Purpose:
--   Query validation_log to monitor data quality results by Airflow DAG run.
--   These queries are used to inspect valid/quarantine row counts,
--   validation pass rates, GX failures, and status differences.

-- 1. Recent validation runs
SELECT
    dag_id,
    run_id,
    total_rows,
    valid_rows,
    quarantine_rows,
    pandas_status,
    gx_status,
    overall_status,
    created_at
FROM validation_log
ORDER BY created_at DESC
LIMIT 10;


-- 2. Validation pass rate by run
SELECT
    created_at::date AS run_date,
    run_id,
    total_rows,
    valid_rows,
    quarantine_rows,
    ROUND(valid_rows::numeric / NULLIF(total_rows, 0) * 100, 2) AS valid_rate
FROM validation_log
ORDER BY created_at DESC
LIMIT 10;


-- 3. Quarantine rate by run
SELECT
    created_at::date AS run_date,
    run_id,
    total_rows,
    quarantine_rows,
    ROUND(quarantine_rows::numeric / NULLIF(total_rows, 0) * 100, 2) AS quarantine_rate
FROM validation_log
ORDER BY created_at DESC
LIMIT 10;


-- 4. GX failed runs
SELECT
    run_id,
    gx_status,
    gx_unsuccessful_expectations,
    gx_error,
    created_at
FROM validation_log
WHERE gx_status = 'FAILED'
ORDER BY created_at DESC
LIMIT 10;


-- 5. Pandas / GX / Overall status comparison
SELECT
    run_id,
    pandas_status,
    gx_status,
    overall_status,
    total_rows,
    valid_rows,
    quarantine_rows,
    created_at
FROM validation_log
ORDER BY created_at DESC
LIMIT 10;


-- 6. Quarantine reason counts from JSONB
SELECT
    run_id,
    reason.key AS fail_reason,
    reason.value::integer AS failed_count,
    created_at
FROM validation_log,
LATERAL jsonb_each_text(fail_reason_counts) AS reason
ORDER BY created_at DESC, failed_count DESC;
