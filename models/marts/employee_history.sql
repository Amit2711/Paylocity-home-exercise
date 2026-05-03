-- depends_on: {{ ref('stg_employee_events') }}

{{
    config(
        materialized         = 'incremental',
        unique_key           = ['employee_id', 'valid_from'],
        cluster_by           = ['employee_id', 'valid_from'],
        incremental_strategy = 'merge',
        merge_update_columns = ['full_name', 'department', 'salary', 'is_active', 'valid_to', '_stg_ingestion_ts'],
        on_schema_change     = 'sync_all_columns'
    )
}}

-- SCD2 history - each row is one period of an employee's state
-- valid_to is NULL for the current open record
-- LEAD() figures out valid_to from the next event's timestamp per employee
--
-- out-of-order events are handled naturally here: when a late event arrives
-- (newer ingestion_ts but earlier business timestamp), we re-select ALL rows
-- for that employee and merge. existing rows get their valid_to corrected,
-- new row gets inserted. no full refresh needed.
--
-- _stg_ingestion_ts is stored so we know which employees to reprocess next run

WITH enriched AS (
    SELECT
        *,
        MAX(ingestion_ts) OVER (PARTITION BY employee_id) AS _stg_ingestion_ts
    FROM {{ ref('int_employee_forward_filled') }}
),

all_history AS (
    SELECT
        employee_id,
        full_name,
        department,
        salary,
        (_de_source_operation != 'delete')  AS is_active,
        _de_source_datetime                 AS valid_from,
        LEAD(_de_source_datetime) OVER (
            PARTITION BY employee_id
            ORDER BY _de_source_datetime
        )                                   AS valid_to,
        _stg_ingestion_ts
    FROM enriched
)

SELECT *
FROM all_history

{% if is_incremental() %}
-- only reprocess employees with new events - but pull ALL their history rows
-- so LEAD() recomputes correctly across the full timeline
WHERE employee_id IN (
    SELECT s.employee_id
    FROM (
        SELECT employee_id, MAX(ingestion_ts) AS max_ingestion_ts
        FROM {{ ref('stg_employee_events') }}
        GROUP BY employee_id
    ) s
    LEFT JOIN (
        SELECT employee_id, MAX(_stg_ingestion_ts) AS _stg_ingestion_ts
        FROM {{ this }}
        GROUP BY employee_id
    ) t ON s.employee_id = t.employee_id
    WHERE s.max_ingestion_ts > COALESCE(t._stg_ingestion_ts, '1900-01-01'::TIMESTAMP_LTZ)
)
{% endif %}
