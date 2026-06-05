select
    dag_id,
    run_id,
    logical_date,
    total_rows,
    valid_rows,
    quarantine_rows,
    pandas_status,
    gx_status,
    overall_status,
    gx_unsuccessful_expectations,
    created_at
from {{ source('raw', 'validation_log') }}
