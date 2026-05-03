{{ config(materialized='view') }}

-- 2. List all active employees.
SELECT EMPLOYEE_ID, FULL_NAME, DEPARTMENT, SALARY
FROM {{ ref('employee_current_state') }}
WHERE IS_ACTIVE = TRUE

/*
Results:
EMPLOYEE_ID  FULL_NAME      DEPARTMENT  SALARY
101          Alice Johnson  Platform    95000
102          Bob Smith      Marketing   72000
*/
