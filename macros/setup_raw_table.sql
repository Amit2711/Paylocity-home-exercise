{% macro setup_raw_table() %}

-- run this once before dbt seed to create the raw table with the right schema
-- important: ingestion_ts has a default so we don't need to pass it on every insert
-- don't run dbt seed --full-refresh after this, it will drop the table and lose the default

{% call statement('create_raw_employee_events', fetch_result=False) %}

    CREATE OR REPLACE TABLE {{ target.database }}.{{ target.schema }}.RAW_EMPLOYEE_EVENTS (
        employee_id           INTEGER,
        full_name             VARCHAR,
        department            VARCHAR,
        salary                NUMBER(10, 2),
        _de_source_operation  VARCHAR,
        _de_source_datetime   TIMESTAMP_NTZ,
        ingestion_ts          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )
    CLUSTER BY (_de_source_datetime);

{% endcall %}

{% endmacro %}
