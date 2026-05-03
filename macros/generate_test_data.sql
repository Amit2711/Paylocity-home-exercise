{% macro generate_test_data(num_employees=1000) %}

-- loads synthetic test data to cover all the NFR edge cases
-- default is 1000 employees but can pass more: --args '{num_employees: 5000}'
-- run dbt run after this to process everything through the pipeline
--
-- each employee gets: 1 create, 7 updates, and then some get edge cases mixed in:
-- every 10th -> duplicate event (same timestamp, should be deduped)
-- every 15th -> out-of-order event (timestamp between create and first update)
-- every 20th -> partial update with NULLs (forward fill should handle)
-- every 25th -> soft delete + reactivation

{% call statement('generate_test_data', fetch_result=False) %}

-- ingestion_ts omitted — Snowflake DEFAULT CURRENT_TIMESTAMP() fills it at insert time
INSERT INTO {{ target.database }}.{{ target.schema }}.RAW_EMPLOYEE_EVENTS
    (employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime)

WITH
employees AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) + 1000 AS employee_id
    FROM TABLE(GENERATOR(ROWCOUNT => {{ num_employees }}))
),

dept_numbered AS (
    SELECT column1 AS dept,
           ROW_NUMBER() OVER (ORDER BY column1) AS dept_num
    FROM VALUES ('Engineering'), ('Finance'), ('HR'), ('Legal'),
                ('Marketing'), ('Operations'), ('Platform'), ('Sales')
),

creates AS (
    SELECT
        e.employee_id,
        'Employee_' || e.employee_id || ' Name'        AS full_name,
        d.dept                                          AS department,
        50000 + (e.employee_id * 7 % 50000)            AS salary,
        'create'                                        AS _de_source_operation,
        DATEADD('second',
            e.employee_id * 37,
            '2024-01-01 00:00:00'::TIMESTAMP_NTZ)      AS _de_source_datetime
    FROM employees e
    JOIN dept_numbered d ON MOD(e.employee_id, 8) + 1 = d.dept_num
),

updates AS (
    SELECT
        c.employee_id,
        c.full_name,
        c.department,
        c.salary + (iter.seq * 2000)                   AS salary,
        'update'                                        AS _de_source_operation,
        DATEADD('day', iter.seq * 30, c._de_source_datetime) AS _de_source_datetime
    FROM creates c
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS seq
        FROM TABLE(GENERATOR(ROWCOUNT => 7))
    ) iter
),

duplicates AS (
    SELECT
        employee_id, full_name, department, salary,
        _de_source_operation, _de_source_datetime
    FROM updates
    WHERE MOD(employee_id, 10) = 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY _de_source_datetime) = 1
),

out_of_order AS (
    SELECT
        employee_id, full_name, department, salary,
        'update'                                        AS _de_source_operation,
        DATEADD('day', 15, _de_source_datetime)        AS _de_source_datetime
    FROM creates
    WHERE MOD(employee_id, 15) = 0
),

partial_updates AS (
    SELECT
        employee_id,
        NULL                                            AS full_name,
        NULL                                            AS department,
        salary + 5000                                   AS salary,
        'update'                                        AS _de_source_operation,
        DATEADD('month', 8, _de_source_datetime)       AS _de_source_datetime
    FROM creates
    WHERE MOD(employee_id, 20) = 0
),

deletes AS (
    SELECT
        employee_id, full_name, department, salary,
        'delete'                                        AS _de_source_operation,
        DATEADD('month', 10, _de_source_datetime)      AS _de_source_datetime
    FROM creates
    WHERE MOD(employee_id, 25) = 0
),

reactivations AS (
    SELECT
        d.employee_id, d.full_name, d.department, d.salary,
        'update'                                        AS _de_source_operation,
        DATEADD('month', 1, d._de_source_datetime)     AS _de_source_datetime
    FROM deletes d
)

SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM creates
UNION ALL
SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM updates
UNION ALL
SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM duplicates
UNION ALL
SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM out_of_order
UNION ALL
SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM partial_updates
UNION ALL
SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM deletes
UNION ALL
SELECT employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime FROM reactivations

{% endcall %}

{% endmacro %}
