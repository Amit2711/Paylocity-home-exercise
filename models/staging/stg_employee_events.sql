{{
    config(
        materialized        = 'incremental',
        unique_key          = ['employee_id', '_de_source_datetime'],
        cluster_by          = ['_de_source_datetime'],
        incremental_strategy= 'merge',
        on_schema_change    = 'sync_all_columns'
    )
}}

-- first layer after raw - cast types, lowercase the operation, deduplicate
-- using ingestion_ts as the watermark (not _de_source_datetime) so out-of-order
-- events are always picked up regardless of their business timestamp
-- NULLs from partial updates are kept as-is here, forward fill happens downstream

WITH source AS (
    SELECT
        employee_id::INTEGER                    AS employee_id,
        full_name,
        department,
        salary::NUMBER(10, 2)                   AS salary,
        LOWER(_de_source_operation)             AS _de_source_operation,
        _de_source_datetime::TIMESTAMP_NTZ      AS _de_source_datetime,
        ingestion_ts::TIMESTAMP_NTZ             AS ingestion_ts
    FROM {{ ref('raw_employee_events') }}

    {% if is_incremental() %}
    WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
)

-- collapse true duplicates (same employee + same timestamp)
SELECT *
FROM source
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY employee_id, _de_source_datetime
    ORDER BY ingestion_ts DESC
) = 1
