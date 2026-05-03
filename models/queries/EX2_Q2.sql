{{ config(materialized='view') }}

-- 2. Which employees were active as of 2024-12-31 at 23:59:59?
SELECT EMPLOYEE_ID, FULL_NAME, DEPARTMENT, SALARY
FROM {{ ref('employee_history') }}
WHERE VALID_FROM  <= '2024-12-31 23:59:59'::TIMESTAMP_NTZ
  AND (VALID_TO   > '2024-12-31 23:59:59'::TIMESTAMP_NTZ OR VALID_TO IS NULL)
  AND IS_ACTIVE   = TRUE

/*
Results:
EMPLOYEE_ID  FULL_NAME      DEPARTMENT  SALARY
101          Alice Johnson  Platform    95000
102          Bob Smith      Marketing   72000
*/
