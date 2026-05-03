-- depends_on: {{ ref('stg_employee_events') }}

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'employee_id',
        incremental_strategy = 'merge',
        merge_update_columns = ['full_name', 'department', 'salary', 'is_active', 'effective_as_of', '_stg_ingestion_ts'],
        on_schema_change     = 'sync_all_columns'
    )
}}

-- one row per employee, always the latest state
-- _stg_ingestion_ts tracks when we last processed each employee so on incremental
-- runs we only touch employees that actually have new events (saves scanning everything)
-- the view underneath always has full history so QUALIFY gives the true latest
-- even when a late event arrives with an earlier business timestamp

WITH enriched AS (
    SELECT
        *,
        MAX(ingestion_ts) OVER (PARTITION BY employee_id) AS _stg_ingestion_ts
    FROM {{ ref('int_employee_forward_filled') }}
),

latest AS (
    SELECT
        employee_id,
        full_name,
        department,
        salary,
        _de_source_datetime                     AS effective_as_of,
        (_de_source_operation != 'delete')      AS is_active,
        _stg_ingestion_ts
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY employee_id
        ORDER BY _de_source_datetime DESC
    ) = 1
)

SELECT *
FROM latest

{% if is_incremental() %}
-- only process employees that have new staging events since last run
WHERE employee_id IN (
    SELECT s.employee_id
    FROM (
        SELECT employee_id, MAX(ingestion_ts) AS max_ingestion_ts
        FROM {{ ref('stg_employee_events') }}
        GROUP BY employee_id
    ) s
    LEFT JOIN {{ this }} t ON s.employee_id = t.employee_id
    WHERE s.max_ingestion_ts > COALESCE(t._stg_ingestion_ts, '1900-01-01'::TIMESTAMP_LTZ)
)
{% endif %}
