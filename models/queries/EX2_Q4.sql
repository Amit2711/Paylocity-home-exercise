{{ config(materialized='view') }}

-- 4. What was employee 102's salary on 2024-01-12 at 3 PM?
SELECT EMPLOYEE_ID, FULL_NAME, DEPARTMENT, SALARY
FROM {{ ref('employee_history') }}
WHERE EMPLOYEE_ID = 102
  AND VALID_FROM  <= '2024-01-12 15:00:00'::TIMESTAMP_NTZ
  AND (VALID_TO    > '2024-01-12 15:00:00'::TIMESTAMP_NTZ OR VALID_TO IS NULL)

/*
Results:
EMPLOYEE_ID  FULL_NAME   DEPARTMENT  SALARY
102          Bob Smith   Sales       65000
*/
