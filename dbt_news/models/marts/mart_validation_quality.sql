select
    run_id,
    logical_date,
    total_rows,
    valid_rows,
    quarantine_rows,
    valid_rows::numeric / nullif(total_rows, 0) as valid_rate,
    quarantine_rows::numeric / nullif(total_rows, 0) as quarantine_rate,
    pandas_status,
    gx_status,
    overall_status,
    gx_unsuccessful_expectations,
    created_at
from {{ ref('stg_validation_log') }}
