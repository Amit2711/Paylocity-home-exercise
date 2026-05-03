{{ config(materialized='view') }}

-- source sends NULL for fields that didn't change (partial updates)
-- LAST_VALUE IGNORE NULLS carries the last known value forward per employee
-- keeping this as a view so downstream always sees the full history without rematerialising

SELECT
    employee_id,
    _de_source_datetime,
    _de_source_operation,
    ingestion_ts,
    LAST_VALUE(full_name  IGNORE NULLS) OVER (
        PARTITION BY employee_id
        ORDER BY _de_source_datetime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS full_name,
    LAST_VALUE(department IGNORE NULLS) OVER (
        PARTITION BY employee_id
        ORDER BY _de_source_datetime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS department,
    LAST_VALUE(salary     IGNORE NULLS) OVER (
        PARTITION BY employee_id
        ORDER BY _de_source_datetime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS salary
FROM {{ ref('stg_employee_events') }}
