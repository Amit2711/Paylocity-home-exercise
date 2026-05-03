{% macro generate_out_of_order_test() %}

-- test for out-of-order handling - inserts 2 events with timestamps that fall
-- between events already processed in a previous dbt run
--
-- employee 1128: created 2024-01-01 11:35:36, first update 2024-01-31
-- employee 1178: created 2024-01-01 12:06:26, first update 2024-01-31
-- both get a late event on 2024-01-16 - sits between create and first update
--
-- steps:
--   1. dbt run-operation generate_test_data  then  dbt run   (base data)
--   2. dbt run-operation generate_out_of_order_test          (insert late events)
--   3. dbt run                                               (pipeline corrects history)
--
-- verify with:
--   SELECT * FROM PAYLOCITY_HW.PLCTY_DBT.EMPLOYEE_HISTORY
--   WHERE EMPLOYEE_ID IN (1128, 1178)
--   ORDER BY EMPLOYEE_ID, VALID_FROM;
--
-- expecting: create row VALID_TO shifts from 2024-01-31 to 2024-01-16
--            new row appears for 2024-01-16 with salary 99128 / 99178
--            rest of the timeline stays the same

{% call statement('generate_out_of_order_test', fetch_result=False) %}

INSERT INTO {{ target.database }}.{{ target.schema }}.RAW_EMPLOYEE_EVENTS
    (employee_id, full_name, department, salary, _de_source_operation, _de_source_datetime)

VALUES
    (1128, 'Employee_1128 Name', 'Engineering', 99128, 'update', '2024-01-16 11:35:36'::TIMESTAMP_NTZ),
    (1178, 'Employee_1178 Name', 'HR',          99178, 'update', '2024-01-16 12:06:26'::TIMESTAMP_NTZ)

{% endcall %}

{% endmacro %}
